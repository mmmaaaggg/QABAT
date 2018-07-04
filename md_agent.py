# -*- coding: utf-8 -*-
"""
Created on 2017/9/1
@author: MG
"""
import json
from config import Config, PeriodType, RunMode
from threading import Thread
from queue import Queue, Empty
import time
import pandas as pd
import logging
from abc import ABC, abstractmethod
from backend.fh_utils import bytes_2_str


class MdAgentBase(Thread, ABC):

    @staticmethod
    def factory(run_mode: RunMode, instrument_id_list, md_period, name=None, **kwargs):
        if run_mode == RunMode.Backtest:
            md_agent = MdAgentBacktest(instrument_id_list, md_period, name, **kwargs)
        elif run_mode == RunMode.Realtime:
            md_agent = MdAgentRealtime(instrument_id_list, md_period, name, **kwargs)
        else:
            raise ValueError("run_mode:%s exception", run_mode)
        return md_agent

    def __init__(self, instrument_id_set, md_period, name=None, init_load_md_count=None, init_md_date_from=None, init_md_date_to=None, **kwargs):
        if name == None:
            name = md_period
        super().__init__(name=name, daemon=True)
        self.md_period = md_period
        self.keep_running = None
        self.instrument_id_set = instrument_id_set
        self.init_load_md_count = init_load_md_count
        self.init_md_date_from = init_md_date_from
        self.init_md_date_to = init_md_date_to
        self.logger = logging.getLogger()

    def load_history(self, date_from=None, date_to=None, load_md_count=None):
        """
        从mysql中加载历史数据
        实时行情推送时进行合并后供数据分析使用
        :param date_from: None代表沿用类的 init_md_date_from 属性
        :param date_to: None代表沿用类的 init_md_date_from 属性
        :param load_md_count: 0 代表不限制，None代表沿用类的 init_load_md_count 属性，其他数字代表相应的最大加载条数
        :return: 
        """
        # 如果 init_md_date_from 以及 init_md_date_to 为空，则不加载历史数据
        if self.init_md_date_from is None and self.init_md_date_to is None:
            return None

        if self.md_period == PeriodType.Tick:
            sql_str = """select * from md_tick
                where InstrumentID in (%s) %s
                order by ActionDay desc, ActionTime desc, ActionMillisec desc %s"""
        elif self.md_period == PeriodType.Min1:
            # sql_str = """select * from md_min_1
            #     where InstrumentID in ('j1801') and tradingday>='2017-08-14'
            #     order by ActionDay, ActionTime, ActionMillisec limit 200"""
            sql_str = """select * from md_min_1
    where InstrumentID in (%s) %s
    order by ActionDay desc, ActionTime desc %s"""
        else:
            raise ValueError('%s error' % md_period)

        # 合约列表
        qry_str_inst_list = "'" + "', '".join(self.instrument_id_set) + "'"
        # date_from 起始日期
        if date_from is None:
            date_from = self.init_md_date_from
        if date_from is None:
            qry_str_date_from = ""
        else:
            qry_str_date_from = " and tradingday>='%s'" % date_from
        # date_to 截止日期
        if date_to is None:
            date_to = self.init_md_date_to
        if date_to is None:
            qry_str_date_to = ""
        else:
            qry_str_date_to = " and tradingday<='%s'" % date_to
        # load_limit 最大记录数
        if load_md_count is None:
            load_md_count = self.init_load_md_count
        if load_md_count is None or load_md_count == 0:
            qry_str_limit = ""
        else:
            qry_str_limit = " limit %d" % load_md_count
        # 拼接sql
        qry_sql_str = sql_str % (qry_str_inst_list, qry_str_date_from + qry_str_date_to, qry_str_limit)

        # 加载历史数据
        engine = Config.get_db_engine(Config.DB_SCHEMA_MD)
        md_df = pd.read_sql(qry_sql_str, engine)
        # self.md_df = md_df
        return md_df

    @abstractmethod
    def connect(self):
        """链接redis、初始化历史数据"""

    @abstractmethod
    def release(self):
        """释放channel资源"""

    def subscribe(self, instrument_id_set=None):
        """订阅合约"""
        if instrument_id_set is None:
            return
        self.instrument_id_set |= instrument_id_set

    def unsubscribe(self, instrument_id_set):
        """退订合约"""
        if instrument_id_set is None:
            self.instrument_id_set = set()
        else:
            self.instrument_id_set -= instrument_id_set


class MdAgentRealtime(MdAgentBase):

    def __init__(self, instrument_id_set, md_period, name=None, init_load_md_count=None,
                 init_md_date_from=None, init_md_date_to=None, **kwargs):
        super().__init__(instrument_id_set, md_period, name=name, init_load_md_count=init_load_md_count,
                         init_md_date_from=init_md_date_from, init_md_date_to=init_md_date_to, **kwargs)
        self.pub_sub = None
        self.md_queue = Queue()

    def connect(self):
        """链接redis、初始化历史数据"""
        redis_client = Config.get_redis()
        self.pub_sub = redis_client.pubsub()

    def release(self):
        """释放channel资源"""
        self.pub_sub.close()

    def subscribe(self, instrument_id_set=None):
        """订阅合约"""
        super().subscribe(instrument_id_set)
        if instrument_id_set is None:
            instrument_id_set = self.instrument_id_set
        channel_head = Config.REDIS_CHANNEL[self.md_period]
        channel_list = [channel_head + instrument_id for instrument_id in instrument_id_set]
        self.pub_sub.psubscribe(*channel_list)

    def run(self):
        """启动多线程获取MD"""
        if not self.keep_running:
            self.keep_running = True
            for item in self.pub_sub.listen():
                if self.keep_running:
                    if item['type'] == 'pmessage':
                        # self.logger.debug("pmessage:", item)
                        md_dic_str = bytes_2_str(item['data'])
                        md_dic = json.loads(md_dic_str)
                        self.md_queue.put(md_dic)
                    else:
                        self.logger.debug("%s response: %s", self.name, item)
                else:
                    break

    def unsubscribe(self, instrument_id_set):
        """退订合约"""
        if instrument_id_set is None:
            tmp_set = self.instrument_id_set
            super().unsubscribe(instrument_id_set)
            instrument_id_set = tmp_set
        else:
            super().unsubscribe(instrument_id_set)

        channel_head = Config.REDIS_CHANNEL[self.md_period]
        channel_list = [channel_head + instrument_id for instrument_id in instrument_id_set]
        if pub_sub is not None:  # 在回测模式下有可能不进行 connect 调用以及 subscribe 订阅，因此，没有 pub_sub 实例
            self.pub_sub.punsubscribe(*channel_list)

    def pull(self, timeout=None):
        """阻塞方式提取合约数据"""
        md = self.md_queue.get(block=True, timeout=timeout)
        self.md_queue.task_done()
        return md


class MdAgentBacktest(MdAgentBase):

    def __init__(self, instrument_id_set, md_period, name=None, init_load_md_count=None,
                 init_md_date_from=None, init_md_date_to=None, **kwargs):
        super().__init__(instrument_id_set, md_period, name=name, init_load_md_count=init_load_md_count,
                         init_md_date_from=init_md_date_from, init_md_date_to=init_md_date_to, **kwargs)
        self.timeout = 1

    def connect(self):
        """链接redis、初始化历史数据"""
        pass

    def release(self):
        """释放channel资源"""
        pass

    def run(self):
        """启动多线程获取MD"""
        if not self.keep_running:
            self.keep_running = True
            while self.keep_running:
                time.sleep(self.timeout)
            else:
                self.logger.info('%s job finished', self.name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    instrument_id_list = set(['jm1711', 'rb1712', 'pb1801', 'IF1710'])
    md_agent = MdAgentBase.factory(RunMode.Realtime ,instrument_id_list, md_period=PeriodType.Min1, init_load_md_count=100)
    md_df = md_agent.load_history()
    print(md_df.shape)
    md_agent.connect()
    md_agent.subscribe(instrument_id_list)
    md_agent.start()
    for n in range(120):
        time.sleep(1)
    md_agent.keep_running = False
    md_agent.join()
    md_agent.release()
    print("all finished")