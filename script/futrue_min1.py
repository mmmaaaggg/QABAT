# -*- coding: utf-8 -*-
"""
Created on 2017/12/10
@author: MG
"""
import os, math, time
from queue import Empty
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy.dialects.mysql import DOUBLE
import logging, re
from config import Config
from sqlalchemy.types import String, Date, Time, DateTime
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count, JoinableQueue, Process, Queue
from backend.fh_utils import get_last, get_first, str_2_date
from backend.windy_utils_rest import WindRest, APIError

MAX_THREADING_POOL_WORKER = 8
MAX_PROCESS_POOL_WORKER = cpu_count()
logger = logging.getLogger()
# 已排序的交易日列表
trade_date_list = []
# 用于定位下一个交易日日期，key为交易日，val 为下一个交易日
next_trade_date_dic = {}


def load_trade_date():
    """
    加载交易日列表：用于定位下一个交易日日期
    :return: 
    """
    global next_trade_date_dic, trade_date_list
    with Config.with_db_session(schema_name=Config.DB_SCHEMA_DEFAULT) as session:
        table = session.execute("select * from wind_trade_date")
        trade_date_list = [content[0] for content in table.fetchall()]
    trade_date_list.sort()
    next_trade_date_dic = {trade_date_list[n]: trade_date_list[n + 1] for n in range(len(trade_date_list) - 1)}

    trade_date_cur = trade_date_list[0]
    trade_date_end = trade_date_list[-1]
    one_day = timedelta(days=1)
    while trade_date_cur < trade_date_end:
        if trade_date_cur not in next_trade_date_dic:
            trade_date_next = get_first(trade_date_list, lambda x: x >= trade_date_cur)
            next_trade_date_dic[trade_date_cur] = trade_date_next
        trade_date_cur += one_day
    logger.info("加载交易日数据完成，共加载 %d 条交易日数据", len(next_trade_date_dic))


class Consumer(Process):
    def __init__(self, task_queue, result_queue, next_trade_date_dic, name=None):
        Process.__init__(self, name=name)
        self.db_table_tail_name = None  # 进程启动过程中这个值会丢失因此在run中去赋值
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.next_trade_date_dic = next_trade_date_dic
        # self.logger = logging.getLogger()
        # self.keep_running = True

    def run(self):
        proc_name = self.name
        self.db_table_tail_name = proc_name.split('-')[1]
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(funcName)s:%(lineno)d|%(message)s')
        # print("开启进程", proc_name, self.db_table_tail_name)
        logger.info("开启进程%s db_table_tail_name %s", proc_name, self.db_table_tail_name)
        file_count, file_count_tot, file_path = None, None, None
        while True:
            try:
                task_params = self.task_queue.get(timeout=5)
                if task_params is None:
                    # Poison pill means shutdown
                    print('%s: Exiting' % proc_name)
                    self.task_queue.task_done()
                    break
                # print('%s: %s' % (proc_name, next_task))
                file_count, file_count_tot, file_path = task_params

                try:
                    logger.info("[进程 %s] %5d/%4d)处理文件：%s", proc_name, file_count, file_count_tot, file_path)
                    data_count = self.load_csv_2_db(file_count, file_count_tot, file_path)
                    exp = None
                    result = (file_count, file_count_tot, file_path, data_count, exp)
                    self.result_queue.put(result)
                except Exception as exp:
                    logger.exception("[进程 %s] %5d/%4d)处理文件：%s" % (proc_name, file_count, file_count_tot, file_path))
                    data_count = 0
                    result = (file_count, file_count_tot, file_path, data_count, exp)
                    self.result_queue.put(result)
                # 标示任务结束，不通过 finally 来进行操作是因为 Empty 异常下不需要调用该过程
                self.task_queue.task_done()
            except Empty:
                time.sleep(1)
            except:
                # self.logger.exception("%s error", proc_name)
                logger.exception("[进程 %s] %5d/%4d)处理文件：%s", proc_name, file_count, file_count_tot, file_path)
                # 标示任务结束，不通过 finally 来进行操作是因为 Empty 异常下不需要调用该过程
                self.task_queue.task_done()
        return

    def load_csv_2_db(self, file_count, file_count_tot, file_path):
        size = os.path.getsize(file_path)
        if size == 0:
            return 0
        data_df = pd.read_csv(file_path, encoding="GBK")
        data_count = data_df.shape[0]
        if data_count == 0:
            return data_count
        data_df.rename(columns={
            "市场代码": "ExchangeID",
            "证券代码": "InstrumentID",
            "日期": "ActionDateTime",
            "开盘价": "open",
            "最高价": "high",
            "最低价": "low",
            "收盘价": "close",
            "成交量": "vol",
            "成交金额": "amount",
            "持仓量": "OpenInterest",
            "c_market": "ExchangeID",
            "c_stock_no": "InstrumentID",
            "c_date": "ActionDateTime",
            "c_open": "open",
            "c_high": "high",
            "c_low": "low",
            "c_close": "close",
            "c_vol": "vol",
            "c_money": "amount",
            "c_count": "OpenInterest",
            "大富翁数据中心_市场代码": "ExchangeID"
        }, inplace=True)
        # if "OpenInterest" in data_df.columns:
        #     return 0
        # 部分数据 缺少 OpenInterest 字段，需要补充
        if "OpenInterest" not in data_df.columns:
            data_df["OpenInterest"] = 0
        # print("%5d/%4d)处理文件：%s [%d 条数据]" % (file_count, file_count_tot, file_path, data_count))
        self.import_data_df_list_2_db(data_df, file_count)
        logger.info("[进程 %s]%5d/%4d)处理文件：%s [%d 条数据]", self.name, file_count, file_count_tot, file_path, data_count)
        return data_count

    def import_data_df_list_2_db(self, data_df, file_count):
        """
        将 DataFrame 导入到数据库 md_min_1_csv
        :param data_df:
        :param file_count:
        :param table_tail_name:
        :return:
        """
        # data_df = pd.concat(data_df_list)
        action_datetime_s = data_df["ActionDateTime"]
        data_df["TradingDay"] = action_datetime_s.apply(self.calc_trade_date)
        data_df["ActionDay"] = action_datetime_s.apply(lambda x: x.split(" ")[0])
        data_df["UpdateTime"] = action_datetime_s.apply(lambda x: x.split(" ")[1])
        data_df["ActionTime"] = data_df["UpdateTime"]
        # data_df.drop(columns='ActionDateTime', inplace=True)
        col_name_list = ["ActionDateTime",
                         "TradingDay",
                         "ActionDay",
                         "ActionTime",
                         "UpdateTime",
                         "InstrumentID",
                         "ExchangeID",
                         "open",
                         "high",
                         "low",
                         "close",
                         "vol",
                         "amount",
                         "OpenInterest",
                         ]
        data_df = data_df[col_name_list]
        data_df.set_index(['TradingDay', 'UpdateTime', "InstrumentID"], inplace=True)
        engine = Config.get_db_engine(schema_name=Config.DB_SCHEMA_MD)
        data_df.to_sql("md_min_1_csv_%s" % self.db_table_tail_name, engine, if_exists='append',
                       dtype={
                           'ExchangeID': String(20),
                           'TradingDay': Date,
                           'ActionDateTime': DateTime,
                           'ActionDay': Date,
                           'UpdateTime': Time,
                           'ActionTime': Time,
                           'InstrumentID': String(20),
                           'open': String(20),
                           'high': DOUBLE,
                           'low': DOUBLE,
                           'close': DOUBLE,
                           'vol': DOUBLE,
                           'amount': DOUBLE,
                           'OpenInterest': DOUBLE,
                       })
        # 性能不如 to_sql
        # params = data_df.to_dict("records")
        # with Config.with_db_session(schema_name=Config.DB_SCHEMA_MD) as session:
        #     sql_str = "insert into md_min_1_csv(%s) value(%s)" % (
        #         ",".join(col_name_list),
        #         ":" + ",:".join(col_name_list)
        #     )
        #     session.execute(sql_str, params=params)

    def calc_trade_date(self, datetime_str):
        """
        计算交易日：如果时间大于晚上20点，小于早上6点
        :param datetime_str:
        :return:
        """
        datetime_trade_date = datetime.strptime(datetime_str, Config.DATETIME_FORMAT_STR)
        trade_date = datetime_trade_date.date()
        if datetime_trade_date.hour < 6 or 20 < datetime_trade_date.hour:
            trade_date_new = self.next_trade_date_dic[trade_date]
        else:
            trade_date_new = trade_date
        trade_date_str = trade_date_new.strftime(Config.DATE_FORMAT_STR)
        return trade_date_str


def calc_trade_date(datetime_str):
    """
    计算交易日：如果时间大于晚上20点，小于早上6点
    :param datetime_str: 
    :return: 
    """
    global next_trade_date_dic
    datetime_trade_date = datetime.strptime(datetime_str, Config.DATETIME_FORMAT_STR)
    trade_date = datetime_trade_date.date()
    if datetime_trade_date.hour < 6 or 20 < datetime_trade_date.hour:
        trade_date_new = next_trade_date_dic[trade_date]
    else:
        trade_date_new = trade_date
    trade_date_str = trade_date_new.strftime(Config.DATE_FORMAT_STR)
    return trade_date_str


def import_data_df_list_2_db(data_df, file_count, table_tail_name=""):
    """
    将 DataFrame 导入到数据库 md_min_1_csv
    :param data_df: 
    :param file_count: 
    :param table_tail_name: 
    :return: 
    """
    # data_df = pd.concat(data_df_list)
    action_datetime_s = data_df["ActionDateTime"]
    data_df["TradingDay"] = action_datetime_s.apply(calc_trade_date)
    data_df["ActionDay"] = action_datetime_s.apply(lambda x: x.split(" ")[0])
    data_df["UpdateTime"] = action_datetime_s.apply(lambda x: x.split(" ")[1])
    data_df["ActionTime"] = data_df["UpdateTime"]
    # data_df.drop(columns='ActionDateTime', inplace=True)
    col_name_list = ["ActionDateTime",
                     "TradingDay",
                     "ActionDay",
                     "ActionTime",
                     "UpdateTime",
                     "InstrumentID",
                     "ExchangeID",
                     "open",
                     "high",
                     "low",
                     "close",
                     "vol",
                     "amount",
                     "OpenInterest",
                     ]
    data_df = data_df[col_name_list]
    data_df.set_index(['TradingDay', 'UpdateTime', "InstrumentID"], inplace=True)
    engine = Config.get_db_engine(schema_name=Config.DB_SCHEMA_MD)
    data_df.to_sql("md_min_1_csv_%d" % table_tail_name, engine, if_exists='append',
                   dtype={
                       'ExchangeID': String(20),
                       'TradingDay': Date,
                       'ActionDateTime': DateTime,
                       'ActionDay': Date,
                       'UpdateTime': Time,
                       'ActionTime': Time,
                       'InstrumentID': String(20),
                       'open': String(20),
                       'high': DOUBLE,
                       'low': DOUBLE,
                       'close': DOUBLE,
                       'vol': DOUBLE,
                       'amount': DOUBLE,
                       'OpenInterest': DOUBLE,
                   })
    # 性能不如 to_sql
    # params = data_df.to_dict("records")
    # with Config.with_db_session(schema_name=Config.DB_SCHEMA_MD) as session:
    #     sql_str = "insert into md_min_1_csv(%s) value(%s)" % (
    #         ",".join(col_name_list),
    #         ":" + ",:".join(col_name_list)
    #     )
    #     session.execute(sql_str, params=params)


def load_csv_2_db(file_count, file_count_tot, file_path, table_tail_name=""):
    data_df = pd.read_csv(file_path, encoding="GBK")
    data_count = data_df.shape[0]
    if data_count == 0:
        return data_count
    data_df.rename(columns={
        "市场代码": "ExchangeID",
        "证券代码": "InstrumentID",
        "日期": "ActionDateTime",
        "开盘价": "open",
        "最高价": "high",
        "最低价": "low",
        "收盘价": "close",
        "成交量": "vol",
        "成交金额": "amount",
        "持仓量": "OpenInterest",
        "c_market": "ExchangeID",
        "c_stock_no": "InstrumentID",
        "c_date": "ActionDateTime",
        "c_open": "open",
        "c_high": "high",
        "c_low": "low",
        "c_close": "close",
        "c_vol": "vol",
        "c_money": "amount",
        "c_count": "OpenInterest",
        "大富翁数据中心_市场代码": "ExchangeID"
    }, inplace=True)
    logger.info("%5d/%4d)处理文件：%s [%d 条数据]", file_count, file_count_tot, file_path, data_count)
    import_data_df_list_2_db(data_df, file_count, table_tail_name=table_tail_name)
    return data_count


def import_data_from_folder(folder_path):
    """
    导入期货1分钟历史数据
    :param folder_path: 
    :return: 
    """
    logger.info("导入历史数据 开始 %s", folder_path)
    datetime_start = datetime.now()

    # 获取文件列表
    file_path_list = []
    for dir_path, sub_dir_path_list, file_name_list in os.walk(folder_path):
        for file_name in file_name_list:
            file_base_name, file_extension = os.path.splitext(file_name)
            if file_extension.lower() != '.csv':
                continue
            file_path = os.path.join(dir_path, file_name)
            file_path_list.append(file_path)
    # 获取文件数据
    file_count_tot = len(file_path_list)
    data_count_tot = 0
    error_file_list = []
    with ThreadPoolExecutor(max_workers=MAX_THREADING_POOL_WORKER) as pool:
        result_dic = {}
        for file_count, file_path in enumerate(file_path_list):
            # data_count = load_csv_2_db(file_count, file_count_tot, file_path)
            result = pool.submit(load_csv_2_db, file_count, file_count_tot, file_path)
            result_dic[result] = (file_count, file_count_tot, file_path)
        for result in as_completed(result_dic):
            try:
                data_count = result.result()
                data_count_tot += data_count
            except Exception:
                file_count, file_count_tot, file_path = result_dic[result]
                logger.exception("%5d/%4d)处理文件：%s", file_count, file_count_tot, file_path)
                error_file_list.append((file_count, file_count_tot, file_path))

    # 检查是否有错误文件
    for n, (file_count, file_count_tot, file_path) in enumerate(error_file_list):
        logger.warning("%d) %5d/%4d 处理文件错误：%s [%d 条数据]", n, file_count, file_count_tot, file_path)
    datetime_end = datetime.now()
    logger.info("导入历史数据 结束 %d 个数据文件 %d 数据被导入，耗时：%s",
                file_count, data_count_tot, datetime_end - datetime_start)


def import_data_from_folder2(folder_path):
    """
    导入期货1分钟历史数据
    :param folder_path: 
    :return: 
    """
    logger.info("导入历史数据 开始 %s", folder_path)
    datetime_start = datetime.now()

    # 获取文件列表
    file_path_list = []
    for dir_path, sub_dir_path_list, file_name_list in os.walk(folder_path):
        for file_name in file_name_list:
            file_base_name, file_extension = os.path.splitext(file_name)
            if file_extension.lower() != '.csv':
                continue
            file_path = os.path.join(dir_path, file_name)
            file_path_list.append(file_path)
    # 获取文件数据
    file_count_tot = len(file_path_list)

    # 建立 consumer 池
    task_queue = JoinableQueue()
    result_queue = Queue()
    consumer_list = []
    worker_num = 4  # cpu_count() 8核 太烧机器了
    logger.info("%d workers will be created", worker_num)
    for n in range(worker_num):
        consumer = Consumer(task_queue, result_queue, next_trade_date_dic)  # , str(n)
        consumer.start()
        consumer_list.append(consumer)

    # 创建任务列表
    for file_count, file_path in enumerate(file_path_list):
        # data_count = load_csv_2_db(file_count, file_count_tot, file_path)
        task_params = (file_count, file_count_tot, file_path)
        task_queue.put(task_params)

    # 等待任务结束
    logging.info("等待全部任务执行结束")
    task_queue.join()
    logging.info("全部任务执行结束，开始结束进程")
    for n in range(worker_num):
        task_queue.put(None)

    task_queue.join()

    # 检查是否有错误文件
    logger.info("统计执行结果")
    err_count = 0
    data_count_tot = 0
    while True:
        try:
            result = result_queue.get(timeout=1)
            file_count, file_count_tot, file_path, data_count, exp = result
            if exp is None:
                data_count_tot += data_count
            else:
                logger.exception("%d) %5d/%4d 处理文件错误：%s \n%s",
                                 err_count, file_count, file_count_tot, file_path, exp)
                err_count += 1
        except Empty:
            break

    datetime_end = datetime.now()
    logger.info("导入历史数据 结束。 %d 数据文件 %d 数据被导入 %d 导入失败，耗时：%s",
                file_count_tot, data_count_tot, err_count, datetime_end - datetime_start)


def merge_md_min_1_csv_2_md_min_1():
    """
    将 md_min_1_csv 表中的数据 迁移到 md_min_1 md_min_n 中
    :return: 
    """
    logger.info("将 md_min_1_csv 表中的数据 迁移到 md_min_1 开始")
    with Config.with_db_session(schema_name=Config.DB_SCHEMA_MD) as session:
        table = session.execute("select count(*) from md_min_1_csv")
        logger.info("md_min_1_csv 表现存 %d 条数据", table.fetchone()[0])
        # 将 md_min_1_csv 表中的数据 迁移到 md_min_1
        sql_str = """replace into md_min_1(TradingDay,UpdateTime,InstrumentID,ActionDateTime,ActionDay,ActionTime,ExchangeID,open,high,low,close,vol,amount,OpenInterest)
        select TradingDay,UpdateTime,InstrumentID,ActionDateTime,ActionDay,ActionTime,ExchangeID,open,high,low,close,vol,amount,OpenInterest from md_min_1_csv"""
        session.execute(sql_str)
        logger.info("将 md_min_1_csv 表中的数据 迁移到 md_min_1 完成")


def merge_md_min_1_2_md_min_n(table_name_from="md_min_1", trade_date: str = None, min_n_list=[5, 10, 15, 30]):
    """
    将 md_min_1_csv 表中的数据 迁移到 md_min_1 md_min_n 中
    :param table_name_from: 
    :param trade_date: 
    :return: 
    """
    with Config.with_db_session(schema_name=Config.DB_SCHEMA_MD) as session:
        table = session.execute("select count(*) from " + table_name_from)
        logger.info("%s 表现存 %d 条数据，开始合并 %s 分钟级数据", table_name_from, table.fetchone()[0], min_n_list)
        for n_min in min_n_list:
            logger.info("将 %(table_name_from)s 合并数据到 md_min_n 中 %(n_min)d分钟数据 开始" % {
                "table_name_from": table_name_from, "n_min": n_min})
            # 将 md_min_1 合并数据到 md_min_n 中
            if trade_date is None:
                sql_str = """replace into md_min_n(InstrumentID,TradingDay,UpdateTime,period_min,shift_min,ActionDateTime,ActionDay,ActionTime,ExchangeID,ExchangeInstID,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,UpperLimitPrice,LowerLimitPrice,PreClosePrice,PreSettlementPrice,PreOpenInterest,high,low,vol,amount)
                select InstrumentID,TradingDay,
                  date_sub(UpdateTime, interval MINUTE(UpdateTime) %% %(n_min)d minute) UpdateTime,
                  %(n_min)d as period_min, 0 as shift_min, max(ActionDateTime),ActionDay,max(ActionTime),ExchangeID,ExchangeInstID,OpenPrice,max(HighestPrice),min(LowestPrice),max(Volume),max(Turnover),UpperLimitPrice,LowerLimitPrice,PreClosePrice,PreSettlementPrice,PreOpenInterest,max(high),min(low),sum(vol),sum(amount)
                from %(table_name_from)s
                group by InstrumentID,TradingDay, hour(UpdateTime), MINUTE(UpdateTime) Div %(n_min)d""" % {"table_name_from": table_name_from, "n_min": n_min}
                session.execute(sql_str)
            else:
                sql_str = """replace into md_min_n(InstrumentID,TradingDay,UpdateTime,period_min,shift_min,ActionDateTime,ActionDay,ActionTime,ExchangeID,ExchangeInstID,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,UpperLimitPrice,LowerLimitPrice,PreClosePrice,PreSettlementPrice,PreOpenInterest,high,low,vol,amount)
                select InstrumentID,TradingDay,
                  date_sub(UpdateTime, interval MINUTE(UpdateTime) %% %(n_min)d minute) UpdateTime,
                  %(n_min)d as period_min, 0 as shift_min, max(ActionDateTime),ActionDay,max(ActionTime),ExchangeID,ExchangeInstID,OpenPrice,max(HighestPrice),min(LowestPrice),max(Volume),max(Turnover),UpperLimitPrice,LowerLimitPrice,PreClosePrice,PreSettlementPrice,PreOpenInterest,max(high),min(low),sum(vol),sum(amount)
                from %(table_name_from)s where TradingDay > :trade_date
                group by InstrumentID,TradingDay, hour(UpdateTime), MINUTE(UpdateTime) Div %(n_min)d""" % {"table_name_from": table_name_from, "n_min": n_min}
                session.execute(sql_str, params={'trade_date': trade_date})

            # 更新 open 价格
            logger.debug("更新 Open 字段")
            if trade_date is None:
                sql_str = """update md_min_n,
                (
                    select md_min_1.InstrumentID, md_min_1.TradingDay, min1_min.UpdateTime_start, open
                    from %(table_name_from)s md_min_1
                    inner join
                    (
                        select InstrumentID,TradingDay, 
                          date_sub(UpdateTime, interval MINUTE(UpdateTime) %% %(n_min)d minute) UpdateTime_start, 
                          min(UpdateTime)  UpdateTime_min
                        from %(table_name_from)s md_min_1
                        group by InstrumentID,TradingDay, hour(UpdateTime), MINUTE(UpdateTime) Div %(n_min)d
                    ) min1_min
                    on md_min_1.InstrumentID = min1_min.InstrumentID
                    and md_min_1.TradingDay = min1_min.TradingDay
                    and md_min_1.UpdateTime = min1_min.UpdateTime_min
                ) min1_group
                set md_min_n.open = min1_group.open
                where md_min_n.TradingDay = min1_group.TradingDay
                and md_min_n.InstrumentID = min1_group.InstrumentID
                and md_min_n.UpdateTime = min1_group.UpdateTime_start
                and md_min_n.period_min = %(n_min)d 
                and md_min_n.shift_min = 0""" % {"table_name_from": table_name_from, "n_min": n_min}
                session.execute(sql_str)
            else:
                sql_str = """update md_min_n,
                (
                    select md_min_1.InstrumentID, md_min_1.TradingDay, min1_min.UpdateTime_start, open
                    from %(table_name_from)s md_min_1
                    inner join
                    (
                        select InstrumentID,TradingDay, 
                          date_sub(UpdateTime, interval MINUTE(UpdateTime) %% %(n_min)d minute) UpdateTime_start, 
                          min(UpdateTime)  UpdateTime_min
                        from %(table_name_from)s md_min_1
                        where TradingDay >= :trade_date
                        group by InstrumentID,TradingDay, hour(UpdateTime), MINUTE(UpdateTime) Div %(n_min)d
                    ) min1_min
                    on md_min_1.InstrumentID = min1_min.InstrumentID
                    and md_min_1.TradingDay = min1_min.TradingDay
                    and md_min_1.UpdateTime = min1_min.UpdateTime_min
                ) min1_group
                set md_min_n.open = min1_group.open
                where md_min_n.TradingDay = min1_group.TradingDay
                and md_min_n.InstrumentID = min1_group.InstrumentID
                and md_min_n.UpdateTime = min1_group.UpdateTime_start
                and md_min_n.period_min = %(n_min)d 
                and md_min_n.shift_min = 0""" % {"table_name_from": table_name_from, "n_min": n_min}
                session.execute(sql_str, params={'trade_date': trade_date})

            # 更新 close 价格
            logger.debug("更新 Close 字段")
            if trade_date is None:
                sql_str = """update md_min_n,
                (
                    select md_min_1.InstrumentID, md_min_1.TradingDay, min1_min.UpdateTime_start, close, OpenInterest, AveragePrice
                    from %(table_name_from)s md_min_1
                    inner join
                    (
                        select InstrumentID,TradingDay, 
                          date_sub(UpdateTime, interval MINUTE(UpdateTime) %% %(n_min)d minute) UpdateTime_start, 
                          max(UpdateTime) UpdateTime_max 
                        from %(table_name_from)s md_min_1
                        group by InstrumentID,TradingDay, hour(UpdateTime), MINUTE(UpdateTime) Div %(n_min)d
                    ) min1_min
                    on md_min_1.InstrumentID = min1_min.InstrumentID
                    and md_min_1.TradingDay = min1_min.TradingDay
                    and md_min_1.UpdateTime = min1_min.UpdateTime_max
                ) min1_group
                set md_min_n.close = min1_group.close, 
                md_min_n.OpenInterest = min1_group.OpenInterest,
                md_min_n.AveragePrice = min1_group.AveragePrice 
                where md_min_n.TradingDay = min1_group.TradingDay
                and md_min_n.InstrumentID = min1_group.InstrumentID
                and md_min_n.UpdateTime = min1_group.UpdateTime_start
                and md_min_n.period_min = %(n_min)d 
                and md_min_n.shift_min = 0""" % {"table_name_from": table_name_from, "n_min": n_min}
                session.execute(sql_str)
            else:
                sql_str = """update md_min_n,
                (
                    select md_min_1.InstrumentID, md_min_1.TradingDay, min1_min.UpdateTime_start, close, OpenInterest, AveragePrice
                    from %(table_name_from)s md_min_1
                    inner join
                    (
                        select InstrumentID,TradingDay,  
                          date_sub(UpdateTime, interval MINUTE(UpdateTime) %% %(n_min)d minute) UpdateTime_start, 
                          max(UpdateTime) UpdateTime_max 
                        from %(table_name_from)s md_min_1
                        where TradingDay >= :trade_date
                        group by InstrumentID,TradingDay, hour(UpdateTime), MINUTE(UpdateTime) Div %(n_min)d
                    ) min1_min
                    on md_min_1.InstrumentID = min1_min.InstrumentID
                    and md_min_1.TradingDay = min1_min.TradingDay
                    and md_min_1.UpdateTime = min1_min.UpdateTime_max
                ) min1_group
                set md_min_n.close = min1_group.close, 
                md_min_n.OpenInterest = min1_group.OpenInterest,
                md_min_n.AveragePrice = min1_group.AveragePrice 
                where md_min_n.TradingDay = min1_group.TradingDay
                and md_min_n.InstrumentID = min1_group.InstrumentID
                and md_min_n.UpdateTime = min1_group.UpdateTime_start
                and md_min_n.period_min = %(n_min)d 
                and md_min_n.shift_min = 0""" % {"table_name_from": table_name_from, "n_min": n_min}
                session.execute(sql_str, params={'trade_date': trade_date})
            logger.info("将 %(table_name_from)s 合并数据到 md_min_n 中 %(n_min)d分钟数据 完成"% {
                "table_name_from": table_name_from, "n_min": n_min})

        logger.info("将 md_min_1 合并数据到 md_min_n 分钟线 完成")


def merge_md_min_1_2_md_half_day(table_name_from="md_min_1", trade_date: str = None):
    """
    将 md_min_1_csv 表中的数据 迁移到 md_half_day 中
    :param table_name_from: 
    :param trade_date: 
    :return: 
    """
    with Config.with_db_session(schema_name=Config.DB_SCHEMA_MD) as session:
        table = session.execute("select count(*) from " + table_name_from)
        logger.info("%s 表现存 %d 条数据", table_name_from, table.fetchone()[0])
        logger.info("将 %(table_name_from)s 合并数据到 md_half_day 中 开始" % {
            "table_name_from": table_name_from})
        # 将 md_min_1 合并数据到 md_min_n 中
        if trade_date is None:
            sql_str = """replace into md_half_day(InstrumentID,TradingDay,UpdateTime,ActionDateTime,ActionDay,ActionTime,ExchangeID,ExchangeInstID,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,UpperLimitPrice,LowerLimitPrice,PreClosePrice,PreSettlementPrice,PreOpenInterest,high,low,vol,amount)
                select InstrumentID,TradingDay,min(UpdateTime) UpdateTime, max(ActionDateTime),ActionDay,max(ActionTime),ExchangeID,ExchangeInstID,OpenPrice,max(HighestPrice),min(LowestPrice),max(Volume),max(Turnover),UpperLimitPrice,LowerLimitPrice,PreClosePrice,PreSettlementPrice,PreOpenInterest,max(high),min(low),sum(vol),sum(amount) 
          from %(table_name_from)s
            group by InstrumentID, TradingDay, if('8:30:00' < UpdateTime and UpdateTime < '12:00:00',1, if('1:00:00' < UpdateTime and UpdateTime < '15:30:00', 2, 3))""" % {"table_name_from": table_name_from}
            session.execute(sql_str)
        else:
            sql_str = """replace into md_half_day(InstrumentID,TradingDay,UpdateTime,ActionDateTime,ActionDay,ActionTime,ExchangeID,ExchangeInstID,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,UpperLimitPrice,LowerLimitPrice,PreClosePrice,PreSettlementPrice,PreOpenInterest,high,low,vol,amount)
                select InstrumentID,TradingDay,min(UpdateTime) UpdateTime, max(ActionDateTime),ActionDay,max(ActionTime),ExchangeID,ExchangeInstID,OpenPrice,max(HighestPrice),min(LowestPrice),max(Volume),max(Turnover),UpperLimitPrice,LowerLimitPrice,PreClosePrice,PreSettlementPrice,PreOpenInterest,max(high),min(low),sum(vol),sum(amount) 
              from %(table_name_from)s where TradingDay > :trade_date
              group by InstrumentID, TradingDay, if('8:30:00' < UpdateTime and UpdateTime < '12:00:00',1, if('1:00:00' < UpdateTime and UpdateTime < '15:30:00', 2, 3))""" % {"table_name_from": table_name_from}
            session.execute(sql_str, params={'trade_date': trade_date})

        # 更新 open 价格
        if trade_date is None:
            sql_str = """update md_half_day,
            (
                select md_min_1.InstrumentID, md_min_1.TradingDay, min1_min.UpdateTime_min, open
                from %(table_name_from)s md_min_1
                inner join
                (
                    select InstrumentID,TradingDay, min(UpdateTime) UpdateTime_min
                    from %(table_name_from)s md_min_1
                    group by InstrumentID, TradingDay, if('8:30:00' < UpdateTime and UpdateTime < '12:00:00',1, if('1:00:00' < UpdateTime and UpdateTime < '15:30:00', 2, 3))
                ) min1_min
                on md_min_1.InstrumentID = min1_min.InstrumentID
                and md_min_1.TradingDay = min1_min.TradingDay
                and md_min_1.UpdateTime = min1_min.UpdateTime_min
            ) min1_group
            set md_half_day.open = min1_group.open
            where md_half_day.TradingDay = min1_group.TradingDay
            and md_half_day.InstrumentID = min1_group.InstrumentID
            and md_half_day.UpdateTime = min1_group.UpdateTime_min""" % {"table_name_from": table_name_from}
            session.execute(sql_str)
        else:
            sql_str = """update md_half_day,
                            (
                                select md_min_1.InstrumentID, md_min_1.TradingDay, min1_min.UpdateTime_min, open
                                from %(table_name_from)s md_min_1
                                inner join
                                (
                                    select InstrumentID,TradingDay, min(UpdateTime) UpdateTime_min
                                    from %(table_name_from)s md_min_1
                                    where TradingDay >= :trade_date
                                    group by InstrumentID, TradingDay, if('8:30:00' < UpdateTime and UpdateTime < '12:00:00',1, if('1:00:00' < UpdateTime and UpdateTime < '15:30:00', 2, 3))
                                ) min1_min
                                on md_min_1.InstrumentID = min1_min.InstrumentID
                                and md_min_1.TradingDay = min1_min.TradingDay
                                and md_min_1.UpdateTime = min1_min.UpdateTime_min
                            ) min1_group
                            set md_half_day.open = min1_group.open
                            where md_half_day.TradingDay = min1_group.TradingDay
                            and md_half_day.InstrumentID = min1_group.InstrumentID
                            and md_half_day.UpdateTime = min1_group.UpdateTime_min""" % {"table_name_from": table_name_from}
            session.execute(sql_str, params={'trade_date': trade_date})
        # 更新 close 价格
        if trade_date is None:
            sql_str = """update md_half_day,
            (
                select md_min_1.InstrumentID, md_min_1.TradingDay, min1_min.UpdateTime_min, close, OpenInterest, AveragePrice
                from %(table_name_from)s md_min_1
                inner join
                (
                    select InstrumentID,TradingDay, min(UpdateTime) UpdateTime_min, max(UpdateTime) UpdateTime_max 
                    from %(table_name_from)s md_min_1
                    group by InstrumentID, TradingDay, if('8:30:00' < UpdateTime and UpdateTime < '12:00:00',1, if('1:00:00' < UpdateTime and UpdateTime < '15:30:00', 2, 3))
                ) min1_min
                on md_min_1.InstrumentID = min1_min.InstrumentID
                and md_min_1.TradingDay = min1_min.TradingDay
                and md_min_1.UpdateTime = min1_min.UpdateTime_max
            ) min1_group
            set md_half_day.close = min1_group.close, 
            md_half_day.OpenInterest = min1_group.OpenInterest,
            md_half_day.AveragePrice = min1_group.AveragePrice 
            where md_half_day.TradingDay = min1_group.TradingDay
            and md_half_day.InstrumentID = min1_group.InstrumentID
            and md_half_day.UpdateTime = min1_group.UpdateTime_min""" % {"table_name_from": table_name_from}
            session.execute(sql_str)
        else:
            sql_str = """update md_half_day,
            (
                select md_min_1.InstrumentID, md_min_1.TradingDay, min1_min.UpdateTime_min, close, OpenInterest, AveragePrice
                from %(table_name_from)s md_min_1
                inner join
                (
                    select InstrumentID,TradingDay, min(UpdateTime) UpdateTime_min, max(UpdateTime) UpdateTime_max 
                    from %(table_name_from)s md_min_1
                    where TradingDay >= :trade_date
                    group by InstrumentID, TradingDay, if('8:30:00' < UpdateTime and UpdateTime < '12:00:00',1, if('1:00:00' < UpdateTime and UpdateTime < '15:30:00', 2, 3))
                ) min1_min
                on md_min_1.InstrumentID = min1_min.InstrumentID
                and md_min_1.TradingDay = min1_min.TradingDay
                and md_min_1.UpdateTime = min1_min.UpdateTime_max
            ) min1_group
            set md_half_day.close = min1_group.close, 
            md_half_day.OpenInterest = min1_group.OpenInterest,
            md_half_day.AveragePrice = min1_group.AveragePrice 
            where md_half_day.TradingDay = min1_group.TradingDay
            and md_half_day.InstrumentID = min1_group.InstrumentID
            and md_half_day.UpdateTime = min1_group.UpdateTime_min""" % {"table_name_from": table_name_from}
            session.execute(sql_str, params={'trade_date': trade_date})
        logger.info("将 %(table_name_from)s 合并数据到 md_half_day 中 完成" % {"table_name_from": table_name_from})


def import_futrue_min1_from_wind(base_on_csv_table=False, merge_2_min_n=True):
    """
    每日同步期货行情数据到 md_min_1
    合并 md_min_n 数据
    首次运行时，以 md_min_1_csv 为基础数据源，此后，由 md_min_1_wind 代替
    1）wind端口接收的数据，存入 md_min_1_wind 表
    2）整理后，进入 md_min_1
    3）合并后，进入 md_min_n 表
    :param base_on_csv_table: 是否基于  md_min_1_csv 表进行合并，csv 数据从 md_min_1_csv 合并到 md_min_1 后 首次执行同步wind数据时使用
    :return: 
    """
    if base_on_csv_table:
        logger.info("执行初次导入wind数据，以 md_min_1_csv 表为数据基础")
    else:
        logger.info("执行导入wind数据，以 md_min_1_wind 表为数据基础")
    # 加载本地文件（记录 合约以及最新更新时间对应关系.
    wind_code_trade_datetime_latest_file_path = '合约wind数据最新更新日期.csv'
    wind_code_trade_datetime_latest_dic = {}
    if os.path.exists(wind_code_trade_datetime_latest_file_path):
        data_df = pd.read_csv(wind_code_trade_datetime_latest_file_path)
        inst_col_name = data_df.columns[0]
        datetime_col_name = data_df.columns[1]
        for row_num in range(data_df.shape[0]):
            inst_id = data_df.iloc[row_num][inst_col_name]
            datetime_str = data_df.iloc[row_num][datetime_col_name]
            wind_code_trade_datetime_latest_dic[inst_id] = datetime_str

    # 如果大于下午16点，则接受当天白天数据
    # 否则只接受当日凌晨数据
    if datetime.now().hour > 16:
        date_end = date.today()
        datetime_end_str = date_end.strftime(Config.DATE_FORMAT_STR) + " 03:20:00"
    else:
        # get_last(trade_date_list, lambda x: x < date.today())
        date_end = date.today() - timedelta(days=1)
        datetime_end_str = date_end.strftime(Config.DATE_FORMAT_STR) + " 03:20:00"
    # 用于保存本次导入的数据
    table_name_new = "md_min_1_" + date_end.strftime("%Y%m%d")
    # if datetime.strptime(datetime_from_str, Config.DATETIME_FORMAT_STR) >= datetime.strptime(datetime_end_str,
    #                                                                                          Config.DATETIME_FORMAT_STR):
    #     logger.warning("获取分钟线数据区间有误 %s %s", datetime_from_str, datetime_end_str)
    #     return
    if base_on_csv_table:
        sql_str = """select * from
        (
            SELECT concat(upper(inst.InstrumentID), '.', case ExchangeID 
            when 'SHFE' then 'SHF' 
            when 'CZCE' then 'CZC' 
            when 'CFFEX' then 'CFE' 
            else ExchangeID
            end) wind_code, 
            inst.InstrumentID, ExchangeID, InstrumentName, ProductID,
            if(ActionDateTime is null, date_format(OpenDate, '%%Y-%%m-%%d 9:00:00'), date_format(ActionDateTime, '%%Y-%%m-%%d 21:00:00')) datetime_from, 
            date_format(if(ExpireDate < curdate(), ExpireDate, if(hour(now())<15, subdate(curdate(),1), curdate())), '%%Y-%%m-%%d 15:00:00') datetime_to, 
            ActionDateTime, OpenDate, ExpireDate
            FROM 
            (
                select * from instrument where not(instrumentID like '%%efp')
            ) inst left join 
            (
                select InstrumentID, max(ActionDateTime) ActionDateTime from md_min_1_csv group by InstrumentID
            ) min1
            on inst.InstrumentID = min1.InstrumentID
            where length(inst.InstrumentID) <= 10
        ) result
        where datetime_from <= datetime_to"""
    else:
        sql_str = """select * from
        (
            SELECT concat(upper(inst.InstrumentID), '.', case ExchangeID 
            when 'SHFE' then 'SHF' 
            when 'CZCE' then 'CZC' 
            when 'CFFEX' then 'CFE' 
            else ExchangeID
            end) wind_code, 
            inst.InstrumentID, ExchangeID, InstrumentName, ProductID,
            if(ActionDateTime is null, date_format(OpenDate, '%%Y-%%m-%%d 9:00:00'), date_format(ActionDateTime, '%%Y-%%m-%%d 21:00:00')) datetime_from, 
            date_format(if(ExpireDate < curdate(), ExpireDate, if(hour(now())<15, subdate(curdate(),1), curdate())), '%%Y-%%m-%%d 15:00:00') datetime_to, 
            ActionDateTime, OpenDate, ExpireDate
            FROM 
            (
                select * from instrument where not(instrumentID like '%%efp')
            ) inst left join 
            (
                select InstrumentID, max(ActionDateTime) ActionDateTime from md_min_1_wind group by InstrumentID
            ) min1
            on inst.InstrumentID = min1.InstrumentID
            where length(inst.InstrumentID) <= 10
        ) result
        where datetime_from <= datetime_to"""
    engine = Config.get_db_engine(schema_name=Config.DB_SCHEMA_MD)
    instrument_info_df = pd.read_sql(sql_str, engine)
    instrument_info_list = instrument_info_df.to_dict('record')
    rest = WindRest(Config.WIND_REST_URL)
    with Config.with_db_session(schema_name=Config.DB_SCHEMA_MD) as session:

        # table = session.execute(sql_str)
        # instrument_datetime_dic = {instrument_id: (open_date, expire_date) for instrument_id, open_date, expire_date in
        #                            table.fetchall()}
        logger.warning("删除历史临时表%s", table_name_new)
        session.execute("drop table if exists %s" % table_name_new)
        table = session.execute(r"show create table md_min_1")
        table_name, table_create_script = table.fetchone()
        create_table_script = table_create_script.replace(table_name, table_name_new)
        session.execute(create_table_script)
        # 构造保存数据用的 sql
        col_name_list = ["InstrumentID",
                         "TradingDay",
                         "ActionDay",
                         "UpdateTime",
                         "ActionTime",
                         "ActionDateTime",
                         "open",
                         "high",
                         "low",
                         "close",
                         "vol",
                         "amount",
                         "OpenInterest",
                         ]
        sql_str = "replace into %s(%s) value(%s)" % (
            table_name_new,
            ",".join(col_name_list),
            ":" + ",:".join(col_name_list)
        )
        # 用于保留 action_datetime 最小值
        min_trade_date = None
        instrument_count = len(instrument_info_list)
        row_num = 0
        wind_code = None
        try:
            for row_num, instrument_info_dic in enumerate(instrument_info_list):
                wind_code = instrument_info_dic['wind_code']
                instrument_id = instrument_info_dic['InstrumentID']
                datetime_from_str = instrument_info_dic['datetime_from']
                datetime_to_str = instrument_info_dic['datetime_to']
                if instrument_id in wind_code_trade_datetime_latest_dic:
                    datetime_latest_str = wind_code_trade_datetime_latest_dic[instrument_id]
                    datetime_latest = datetime.strptime(datetime_latest_str, Config.DATETIME_FORMAT_STR)
                    datetime_from = datetime.strptime(datetime_from_str, Config.DATETIME_FORMAT_STR)
                    datetime_from = max([datetime_latest + timedelta(minutes=1), datetime_from])
                    datetime_to = datetime.strptime(datetime_to_str, Config.DATETIME_FORMAT_STR)
                    if datetime_from >= datetime_to:
                        continue
                    datetime_from_str = datetime_from.strftime(Config.DATETIME_FORMAT_STR)

                # datetime_to_str = datetime_to.strftime(Config.DATETIME_FORMAT_STR)
                # 检查当前合约起止日期是否与制定日期存在交集，调整查询起止日期
                # if expire_date < trade_date:
                #     logger.warning("%d) %s 合约 截止日期 %s < 指定日期 %s ，跳过", n, instrument_id, expire_date, trade_date)
                #     continue
                # if trade_date <= open_date:
                #     logger.warning("%d) %s 合约 指定日期 %s <= 开始日期 %s，调整指定日期", n, instrument_id, trade_date, open_date)
                #     trade_date = open_date
                #     datetime_from_str = trade_date.strftime(Config.DATE_FORMAT_STR) + " 09:00:00"
                # else:
                #     datetime_from_str = (trade_date - timedelta(days=1)).strftime(Config.DATE_FORMAT_STR) + " 21:00:00"
                # w.wsi("RU1801.SHF", "open,high,low,close,volume,amt,oi", "2017-12-11 09:00:00", "2017-12-11 10:07:01", "")
                logger.info("%d/%d) 获取 %s %s - %s 区间的分钟线数据",
                            row_num, instrument_count, wind_code, datetime_from_str, datetime_to_str)
                try:
                    data_df_tmp = rest.wsi(wind_code, "open,high,low,close,volume,amt,oi",
                                           datetime_from_str, datetime_to_str, "")
                except APIError as exp:
                    logger.exception("%d/%d) 获取 %s %s - %s 区间的分钟线数据 异常",
                                     row_num, instrument_count, wind_code, datetime_from_str, datetime_to_str)
                    if exp.ret_dic.setdefault('error_code', 0) in (
                            -40520007,  # "没有可用数据"
                            -40522003,  # "非法请求"
                    ):
                        continue
                    else:
                        break
                wind_code_trade_datetime_latest_dic[instrument_id] = datetime_to_str
                if data_df_tmp is None:
                    continue
                # 剔除所有全空列
                data_df = data_df_tmp[data_df_tmp.apply(lambda x: not all(x.apply(lambda y: math.isnan(y))), axis=1)]
                data_df.fillna(0, inplace=True)
                data_len = data_df.shape[0]
                if data_len == 0:
                    continue
                data_df.index.rename("ActionDateTime", inplace=True)
                data_df.reset_index(inplace=True)
                data_df.rename(columns={
                    "volume": "vol",
                    "AMT": "amount",
                    "position": "OpenInterest",
                }, inplace=True)
                data_df["InstrumentID"] = instrument_id
                action_datetime_s = data_df["ActionDateTime"]
                data_df["TradingDay"] = action_datetime_s.apply(calc_trade_date)
                data_df["ActionDay"] = action_datetime_s.apply(lambda x: x.split(" ")[0])
                data_df["UpdateTime"] = action_datetime_s.apply(lambda x: x.split(" ")[1])
                data_df["ActionTime"] = data_df["UpdateTime"]
                # 插入数据库
                params = data_df.to_dict("records")
                logger.info("%d/%d) 保存 %s %s - %s 区间的分钟线数据 %d 条",
                            row_num, instrument_count, instrument_id, datetime_from_str, datetime_end_str, data_len)
                session.execute(sql_str, params=params)

                # 保留 action_datetime 最小值
                min_trade_date_cur = data_df["TradingDay"].apply(
                    lambda x: datetime.strptime(x, Config.DATE_FORMAT_STR)).min()
                if min_trade_date is None:
                    min_trade_date = min_trade_date_cur
                elif min_trade_date > min_trade_date_cur:
                    min_trade_date = min_trade_date_cur
        except:
            logger.exception("%d/%d) %s 向 md_min_1_wind 导入数据过程中发生异常",
                             row_num, instrument_count, wind_code)

        # 保存本地文件（记录 合约以及最新更新时间对应关系
        if len(wind_code_trade_datetime_latest_dic) > 0:
            wind_code_trade_datetime_latest_df = pd.DataFrame(
                [{'instrument_id': inst_id, 'datetime_latest': datetime_latest} for inst_id, datetime_latest in
                 wind_code_trade_datetime_latest_dic.items()])[["instrument_id", "datetime_latest"]]
            wind_code_trade_datetime_latest_df.to_csv(wind_code_trade_datetime_latest_file_path, index=False)

        logger.info("%s 表中的数据 迁移到 md_min_1_wind 开始 action_datetime 最小值 %s", table_name_new, min_trade_date)
        # 将最新数据保存到 md_min_1_wind 及 md_min_1 表中
        sql_str = """replace into md_min_1_wind(TradingDay,UpdateTime,InstrumentID,ActionDateTime,ActionDay,ActionTime,ExchangeID,open,high,low,close,vol,amount,OpenInterest)
            select TradingDay,UpdateTime,InstrumentID,ActionDateTime,ActionDay,ActionTime,ExchangeID,open,high,low,close,vol,amount,OpenInterest 
            from %s""" % table_name_new
        session.execute(sql_str)
        logger.info("%s 表中的数据 迁移到 md_min_1 开始 action_datetime 最小值 %s", table_name_new, min_trade_date)
        sql_str = """replace into md_min_1(TradingDay,UpdateTime,InstrumentID,ActionDateTime,ActionDay,ActionTime,ExchangeID,open,high,low,close,vol,amount,OpenInterest)
            select TradingDay,UpdateTime,InstrumentID,ActionDateTime,ActionDay,ActionTime,ExchangeID,open,high,low,close,vol,amount,OpenInterest 
            from %s""" % table_name_new
        session.execute(sql_str)
        logger.info("将 md_min_1_wind 表中的数据 迁移到 md_min_1 完成")
        # 将 md_min_1_csv 表中的数据 迁移到 md_min_1
        if merge_2_min_n and min_trade_date is not None:
            trade_date_str = min_trade_date.strftime(Config.DATE_FORMAT_STR)
            logger.info("将 %s 表中的数据 迁移到 md_min_n 完成", table_name_new)
            # 将 md_min_1_csv 表中的数据 迁移到 md_min_1 md_min_n 中
            merge_md_min_1_2_md_min_n(table_name_new)
            logger.info("将 %s 表中的数据 迁移到 md_half_day 完成", table_name_new)
            merge_md_min_1_2_md_half_day(table_name_new)


def check_min1_data():
    """
    比对各个合约分钟线数据是否存在交易日级别的数据缺失
    由于数据缺失比较严重，比较意义不大了
    :return: 
    """
    # 获取历史全部交易日数据
    engine_trade_date = Config.get_db_engine(Config.DB_SCHEMA_DEFAULT)
    sql_str = r"select trade_date from wind_trade_date_all where exch_code='SZSE'"
    trade_date_df = pd.read_sql(sql_str, engine_trade_date)
    trade_date_df['trade_date'] = trade_date_df['trade_date'].apply(str_2_date)

    # 获取各个合约及相应起止日期
    engine_md = Config.get_db_engine(Config.DB_SCHEMA_MD)
    sql_str = "select InstrumentID, TradingDay from md_min_1 where TradingDay<'2017-12-1' group by InstrumentID, TradingDay"
    instrument_date_df = pd.read_sql(sql_str, engine_md)
    instrument_date_df['TradingDay'] = instrument_date_df['TradingDay'].apply(str_2_date)
    instrument_date_dfg = instrument_date_df.groupby('InstrumentID')
    miss_date_df = None
    re_pattern_instrument_id = re.compile(r"^[A-Za-z]+\d{3,4}$", re.IGNORECASE)
    re_pattern_instrument_type = re.compile(r"^[A-Za-z]+(?=\d{3,4}$)", re.IGNORECASE)
    for instrument_id, date_df in instrument_date_dfg:
        match = re_pattern_instrument_type.match(instrument_id)
        if match is None:
            continue
        if match.group().upper() in ('MA', 'WT', 'A', 'B', 'BB', 'CS', 'FB', 'FU'):
            continue
        if instrument_id.find('0001') != -1:
            continue
        trade_date_min = date_df['TradingDay'].min()
        trade_date_max = date_df['TradingDay'].max()
        if (trade_date_max - trade_date_min).days > 600:
            continue
        available_trade_date_df = trade_date_df[(trade_date_min <= trade_date_df['trade_date']) & (trade_date_df['trade_date'] <= trade_date_max)]
        # available_trade_date_df = date_df['TradingDay'][(trade_date_min <= trade_date_df['trade_date']) & (trade_date_df['trade_date'] <= trade_date_max)]
        merged_df = pd.DataFrame.merge(available_trade_date_df, date_df, how='left', left_on='trade_date', right_on='TradingDay')
        trade_missing_s = merged_df[merged_df['TradingDay'].isna()]['trade_date']
        trade_missing_s.name = instrument_id
        if trade_missing_s.shape[0] == 0:
            continue
        if miss_date_df is None:
            miss_date_df = pd.DataFrame({instrument_id: trade_missing_s, 'trade_date': trade_missing_s}).set_index('trade_date')
        else:
            miss_date_df = miss_date_df.merge(pd.DataFrame({instrument_id: trade_missing_s, 'trade_date': trade_missing_s}).set_index('trade_date'),
                               how='outer', left_index=True, right_index=True)
    if miss_date_df is not None:
        try:
            miss_date_df.to_csv("miss_date.csv")
        except:
            logger.exception("write file error")

        ustack_df = miss_date_df.unstack()
        ustack_df.to_csv("ustack_df.csv")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=Config.LOG_FORMAT)

    load_trade_date()

    # folder_path = r"D:\WSPych\QABAT\data\Commodity1Min\2014\f_min1_201402"
    # import_data_from_folder2(folder_path)
    # logger.info("开始迁移数据")
    # with Config.with_db_session(schema_name=Config.DB_SCHEMA_MD) as session:
    #     session.execute("insert ignore into md_min_1_csv select * from md_min_1_csv_1")
    #     logger.info("md_min_1_csv_1 完成")
    #     session.execute("insert ignore into md_min_1_csv select * from md_min_1_csv_2")
    #     logger.info("md_min_1_csv_2 完成")
    #     session.execute("insert ignore into md_min_1_csv select * from md_min_1_csv_3")
    #     logger.info("md_min_1_csv_3 完成")
    #     session.execute("insert ignore into md_min_1_csv select * from md_min_1_csv_4")
    #     logger.info("md_min_1_csv_4 完成")
    #     table = session.execute("select count(*) from md_min_1_csv")
    #     logger.info("%d 条数据被移入 md_min_1_csv 表", table.fetchone()[0])

    logger.info("开始导入wind数据")
    # csv 数据从 md_min_1_vsv 合并到 md_min_1 后 首次执行同步wind数据时使用
    merge_2_min_n = True  # 默认为 True
    base_on_csv_table = False
    import_futrue_min1_from_wind(base_on_csv_table, merge_2_min_n=merge_2_min_n)

    # 临时脚本
    # with Config.with_db_session(schema_name=Config.DB_SCHEMA_MD) as session:
    #     # session.execute(r"""update md_min_1 set ActionDateTime = concat(date_format(ActionDay, '%Y-%m-%d '), date_format(ActionTime, '%h:%i:%s'))""")
    #     table = session.execute(r"show create table md_min_1")
    #     table_name, table_create_script = table.fetchone()
    #     table_create_script.replace(table_name, table_name + "_tmp")
    # table_name_new = 'md_min_1_20171231'
    # merge_md_min_1_2_md_min_n(table_name_new)
    # merge_md_min_1_2_md_half_day(table_name_new)
    # min_n_list = [5, 10, 15, 30]
    # merge_md_min_1_2_md_min_n(min_n_list=min_n_list)
    # merge_md_min_1_2_md_half_day()

    # 检查日线数据是否有数据缺失
    # 由于数据缺失比较严重，比较意义不大了
    # check_min1_data()
