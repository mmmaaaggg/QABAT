# -*- coding: utf-8 -*-
"""
Created on 2017/11/13
@author: MG
"""
import json
import logging
import os
import threading
import time, numpy as np
from datetime import timedelta, datetime
from threading import Thread
from queue import Queue, Empty
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from config import Config, PeriodType
from backend.fh_utils import bytes_2_str
from event_agent import event_agent, EventType
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert


@compiles(Insert)
def append_string(insert, compiler, **kw):
    """
    支持 ON DUPLICATE KEY UPDATE
    通过使用 on_duplicate_key_update=True 开启
    :param insert: 
    :param compiler: 
    :param kw: 
    :return: 
    """
    s = compiler.visit_insert(insert, **kw)
    if insert.kwargs.get('on_duplicate_key_update'):
        fields = s[s.find("(") + 1:s.find(")")].replace(" ", "").split(",")
        generated_directive = ["{0}=VALUES({0})".format(field) for field in fields]
        return s + " ON DUPLICATE KEY UPDATE " + ",".join(generated_directive)
    return s


class MdCombiner(Thread):
    """
    用于异步将md存储到数据库中，该设计采用 productor consumer模式
    目前仅考虑 productor多线程接收md，MDSaver单线程存储到数据库的情况
    暂不考虑多线程MDSaver存储的情况
    """

    def __init__(self, event_type, name, daemon=True):
        super().__init__(name=name, daemon=True)
        self.time_interval = 10
        self.queue_md = Queue()
        self.event_type = event_type
        self.logger = logging.getLogger(self.__class__.__name__)
        # 注册事件响应句柄：将一分钟数据广播
        # 注册重复时间将被覆盖，因此不会发生重复广播的情况
        # if not MdMin1Combiner.publisher_has_registed:
        #     self.md_min1_publisher = MdPublisher(EventType.Min1_MD_Event, PeriodType.Min1)
        #     MdMin1Combiner.publisher_has_registed = True
        # 注册事件响应句柄：捕获tick数据
        event_agent.register_handler(
            event_type,
            self.queue_md.put_nowait,
            handler_name='%s queue %s -> %s' % (self.__class__.__name__, event_type, self.name),
            key=self.name)


class MdMin1Combiner(MdCombiner):
    """
    用于异步将md存储到数据库中，该设计采用 productor consumer模式
    目前仅考虑 productor多线程接收md，MDSaver单线程存储到数据库的情况
    暂不考虑多线程MDSaver存储的情况
    """

    def __init__(self, instrument_id):
        super().__init__(event_type=EventType.Tick_MD_Event, name=instrument_id, daemon=True)
        self.trade_datetime_range_list = Config.get_trade_datetime_range_list(instrument_id)
        self._mutex = threading.Lock()
        self.keep_running = False
        self.md_k_dic_last = None

        # self.logger = logging.getLogger(instrument_id)
        # self.exch_eng = Config.instrument_market_dic.setdefault(instrument_id, 'DCE')
        try:
            instrument_info_dic = Config.instrument_info_dic[instrument_id]
            self.exch_eng = instrument_info_dic["ExchangeID"]
        except:
            self.exch_eng = "DCE"
        self._second_4_minute_end = None
        self._datetime_target = None
        self._datetime_current = None
        self.time_unit = timedelta(minutes=1)
        # 设置 服务器与本地时间差
        # exch_eng = Config.instrument_market_dic.setdefault(instrument_id, 'DCE')
        try:
            instrument_info_dic = Config.instrument_info_dic[instrument_id]
            exch_eng = instrument_info_dic["ExchangeID"]
        except:
            exch_eng = "DCE"

        if exch_eng == 'DCE':
            self.exch_timedelta = Config.DCETimeDelta
        elif exch_eng == 'CZCE':
            self.exch_timedelta = Config.CZCETimeDelta
        elif exch_eng == 'FFEX':
            self.exch_timedelta = Config.FFEXTimeDelta
        elif exch_eng == 'INE':
            self.exch_timedelta = Config.INETimeDelta
        else:
            self.exch_timedelta = Config.SHFETimeDelta

    def is_end_of_k(self, depth_md_dic, datetime_target):
        """
        判断短期tick 是否是k线结束，或者是新的K线开始
        :param depth_md_dic: 
        :param datetime_target: 
        :return: 0 未结束，1 当前tick为k线最后一个数据；2 当前数据为新的一个k线开始
        """
        ret_mark = 0
        sec_ms = depth_md_dic['UpdateTime_Sec_ms']
        if sec_ms >= 59.5:
            # 在 datetime_target 时间内，出现分钟线最后一根tick
            ret_mark = 1
        else:
            exch_datetime = Config.exch_datetime
            # update_time_new_str = depth_md_dic['ActionDay'] + ' ' + depth_md_dic['UpdateTime']
            # update_time_new_str = exch_datetime.strftime('%Y-%m-%d') + ' ' + depth_md_dic['UpdateTime']
            # update_time_new = datetime.strptime(update_time_new_str, '%Y-%m-%d %H:%M:%S')
            update_time_new_str = depth_md_dic['UpdateTime']
            update_time_new = datetime(year=exch_datetime.year, month=exch_datetime.month, day=exch_datetime.day,
                                       hour=int(update_time_new_str[:2]),
                                       minute=int(update_time_new_str[3:5]),
                                       second=int(update_time_new_str[6:]))
            if update_time_new >= datetime_target:
                # 当前tick时间已经大于  datetime_target
                instrument_id = depth_md_dic['InstrumentID']
                self.logger.debug("%s 当前tick时间 已经大于 目标K线结束时间:%s > %s",
                                  instrument_id, update_time_new, datetime_target)
                ret_mark = 2
        return ret_mark

    def calc_target_minute(self, add_minute=None):
        """
        计算下一个整分钟时间到当前时间的距离
        计算后，修改属性 
        second_4_minute_end 距离下一个整分钟时间的秒数
        datetime_target  下一个整分钟目标时间，整分钟时间（秒数为0）
        datetime_current 当前分钟线时间（datetime_target - 1分钟）
        :param add_minute: None(默认): 完整计算  false：使用当前目标时间，只计算剩余秒数  true：至少增加1分钟 
        :return: 
        """
        exch_datetime = Config.exch_datetime
        if add_minute == False:
            # 目标时间不变，只修改到期秒数
            second_4_minute_end = self._datetime_target - exch_datetime
            self._second_4_minute_end = second_4_minute_end.seconds + second_4_minute_end.microseconds / 1000000
        else:
            datetime_target = None
            for datetime_from, datetime_to in self.trade_datetime_range_list:
                if exch_datetime < datetime_from:
                    next_minute = datetime_from + self.time_unit
                    datetime_target = datetime(year=next_minute.year, month=next_minute.month, day=next_minute.day,
                                               hour=next_minute.hour, minute=next_minute.minute)
                    break
                elif datetime_from <= exch_datetime < datetime_to:
                    next_minute = exch_datetime + self.time_unit
                    datetime_target = datetime(year=next_minute.year, month=next_minute.month, day=next_minute.day,
                                               hour=next_minute.hour, minute=next_minute.minute)
                    break
            if datetime_target is None:
                self._second_4_minute_end = None
                self._datetime_target = None
                self._datetime_current = None
            else:
                # 根据 add_minute 决定是否使用当前计算的时间数据
                if add_minute:
                    if self._datetime_target + self.time_unit > datetime_target:
                        datetime_target = self._datetime_target + self.time_unit

                # 得到距离下一个整分钟时间的秒数
                second_4_minute_end = datetime_target - exch_datetime
                self._second_4_minute_end = second_4_minute_end.seconds + second_4_minute_end.microseconds / 1000000
                self._datetime_target = datetime_target
                self._datetime_current = datetime_target - self.time_unit

        return self._second_4_minute_end, self._datetime_target

    @property
    def second_4_minute_end(self):
        """距离下一个整分钟时间的秒数"""
        return self._second_4_minute_end

    @property
    def datetime_target(self) -> datetime:
        """下一个整分钟目标时间，整分钟时间（秒数为0）"""
        return self._datetime_target

    @property
    def datetime_current(self) -> datetime:
        """当前分钟线时间，秒数为00"""
        return self._datetime_current

    def combine_publisher(self, depth_md_dic_list):
        """
        将 tick 列表数据合并成1分钟k线数据，秒钟设置为：00（非线程安全）
        :param depth_md_dic_list: 
        :return: 
        """
        depth_md_dic_list_len = len(depth_md_dic_list)
        if depth_md_dic_list_len == 0:
            # 当前分钟没有tick数据
            if self.md_k_dic_last is None:
                # 没有上一分钟最后一个tick数据可以参考
                md_k_dic = None
                self.logger.warning('%s 缺少上一根K线数据，且当前时间段无Tick，无法合成分钟线数据', self.name)
                # TODO: 以后通过添加上一交易日数据来补充当前 分钟线数据
            else:
                # 利用上一分钟最后一个tick数据进行数据修改
                md_k_dic = self.md_k_dic_last.copy()
                close_last = md_k_dic['close']
                md_k_dic['open'] = close_last
                md_k_dic['high'] = close_last
                md_k_dic['low'] = close_last
                # md_k['close'] = close_last
                md_k_dic['Volume'] = 0
                md_k_dic['Turnover'] = 0
                md_k_dic['UpdateTime_LastTick'] = md_k_dic['UpdateTime']
                # 重置 分钟线数据时间
                updatetime_str = self.datetime_current.strftime(Config.TIME_FORMAT_STR)
                md_k_dic['UpdateTime'] = updatetime_str
                md_k_dic['ActionDateTime'] = self.datetime_current.strftime(Config.DATETIME_FORMAT_STR)
        else:
            # 将当前分钟的tick
            md_k_dic = depth_md_dic_list[-1].copy()
            updatetime_last_tick = md_k_dic['UpdateTime']
            prices = [depth_md_dic['LastPrice'] for depth_md_dic in depth_md_dic_list]
            md_k_dic['open'] = depth_md_dic_list[0]['LastPrice']
            md_k_dic['high'] = max(prices)
            md_k_dic['low'] = min(prices)
            md_k_dic['close'] = md_k_dic['LastPrice']
            vol_last = 0 if self.md_k_dic_last is None else self.md_k_dic_last['Volume']
            md_k_dic['vol'] = depth_md_dic_list[-1]['Volume'] - vol_last
            amount_last = 0 if self.md_k_dic_last is None else self.md_k_dic_last['Turnover']
            md_k_dic['amount'] = depth_md_dic_list[-1]['Turnover'] - amount_last

            md_k_dic['UpdateTime_LastTick'] = updatetime_last_tick
            # update_time = md_k_dic['UpdateTime'][:6] + '00'
            # md_k_dic['UpdateTime'] = update_time
            updatetime_str = self.datetime_current.strftime(Config.TIME_FORMAT_STR)
            md_k_dic['UpdateTime'] = updatetime_str
            md_k_dic['ActionDateTime'] = self.datetime_current.strftime(Config.DATETIME_FORMAT_STR)
            self.logger.debug('%s %s分钟线数据[%d] last tick update time %s %s',
                              self.name, updatetime_str, depth_md_dic_list_len, updatetime_last_tick, md_k_dic)

        # 发送事件
        if md_k_dic is not None:
            instrument_id = md_k_dic['InstrumentID']
            # 发送 Min1 事件
            event_agent.send_event(EventType.Min1_MD_Event, md_k_dic, key=instrument_id)
            self.md_k_dic_last = md_k_dic

        return md_k_dic

    def run(self):
        with self._mutex:
            if self.keep_running:
                return
            else:
                self.keep_running = True
        try:
            # keep_getting_count 实际作用不大，只是debug的时候显示一下连续抓取了多少数据而已
            self.logger.debug("启动 %s -> %s", self.__class__.__name__, self.name)
            keep_getting_count = 0
            update_datetime = datetime(1990, 1, 1, 1, 1, 1)
            depth_md_dic_list = []
            # 如果在交易时段内则获取下一分钟的整点时间，否则以最接近的开盘时间+1分钟为时点
            # second_4_minute_end, datetime_target 与 self.second_4_minute_end self.datetime_target 相同
            second_4_minute_end, datetime_target = self.calc_target_minute()
            if second_4_minute_end is not None:
                self.logger.debug('%s距离下一个整分钟%s，还有 %.3f 秒', self.name,
                                  datetime_target.strftime('%Y-%m-%d %H:%M:%S.%f'), second_4_minute_end)
            while self.keep_running:
                try:
                    if second_4_minute_end is None:
                        self.logger.info('%s 合约交易时间已经结束 %s[%s]', self.name, datetime.now(), Config.exch_datetime)
                        break
                    depth_md_dic = self.queue_md.get(block=True, timeout=second_4_minute_end)

                    # 判断是否符合k线结束条件，0，未结束 1：tick最后一根；2：下一个Tick开始
                    ret_mark = self.is_end_of_k(depth_md_dic, datetime_target)
                    is_end_of_k = ret_mark > 0
                    if is_end_of_k:
                        if ret_mark == 1:
                            depth_md_dic_list.append(depth_md_dic)
                            self.logger.debug('合并1分钟线 1：%s %s %s',
                                              depth_md_dic['InstrumentID'], depth_md_dic['TradingDay'],
                                              depth_md_dic['UpdateTime'])
                            self.combine_publisher(depth_md_dic_list)
                            depth_md_dic_list = []
                            keep_getting_count = 0
                        else:
                            self.logger.debug('合并1分钟线 2：%s %s %s 超时时间：%s',
                                              depth_md_dic['InstrumentID'], depth_md_dic['TradingDay'],
                                              depth_md_dic['UpdateTime'], second_4_minute_end)
                            self.combine_publisher(depth_md_dic_list)
                            depth_md_dic_list = [depth_md_dic]
                            keep_getting_count = 1

                        # 重新计算超时时间
                        second_4_minute_end, datetime_target = self.calc_target_minute(True)
                    else:
                        depth_md_dic_list.append(depth_md_dic)
                        keep_getting_count += 1
                        # 重新计算超时时间
                        second_4_minute_end, datetime_target = self.calc_target_minute(False)

                    # 标记完成
                    self.queue_md.task_done()
                except Empty:
                    if keep_getting_count > 0:
                        self.logger.debug("%s 超时时间：%s，未能获取tick数据，存量数据 %d 条",
                                          self.name, second_4_minute_end, keep_getting_count)
                        # 仅当 keep_getting_count > 0 时才保
                        with self._mutex:
                            self.combine_publisher(depth_md_dic_list)
                            depth_md_dic_list = []
                            keep_getting_count = 0
                            # 取得下一分钟的整分钟时间
                            second_4_minute_end, datetime_target = self.calc_target_minute(True)

            # 循环结束，检查是否存在未保存的tick数据
            with self._mutex:
                if keep_getting_count > 0:
                    # 仅当 keep_getting_count > 0 时才保
                    self.combine_publisher(depth_md_dic_list)
        finally:
            # 交易时段结束
            self.logger.info('%s job finished. 服务器时间：%s',
                             self.name, Config.exch_datetime.strftime(Config.DATETIME_FORMAT_STR))
            self.keep_running = False


class MdMinNCombiner(MdCombiner):
    """
    用于异步将 min1 数据合成为 N 分钟数据，该设计采用 productor consumer模式
    目前仅考虑 productor多线程接收md，MDSaver单线程存储到数据库的情况
    暂不考虑多线程MDSaver存储的情况
    """

    def __init__(self, instrument_id, period_shift_min_set_dic: dict):
        """
        :param instrument_id: 
        :param period_shift_min_set_dic: {PeriodType.Min5: {0,1,2,3,4}, PeriodType.Min15: {...}}
        """
        super().__init__(event_type=EventType.Min1_MD_Event, name=instrument_id, daemon=True)
        self._mutex = threading.Lock()
        self.keep_running = False
        self.md_k_dic_last = None
        # period_type, shift_n_min_set=set(0)
        self.period_shift_min_set_dic = {}
        for period_type, shift_n_min_set in period_shift_min_set_dic.items():
            self.period_type = period_type
            self.period_type = period_type
            count_of_min1 = PeriodType.get_min_count(period_type)
            self.period_shift_min_set_dic[period_type] = (count_of_min1, shift_n_min_set)

        # TODO: 加载当日历史一分钟数据，考虑当日中途断开连接重启的情况，可以继续生成相应分钟级别数据

    def combine_publisher(self, md_dic_list, period_type, period_min, shift_n_min):
        """
        将1分钟k线数据合并成N分钟数据，偏移量shift_n_min
        分钟线合成逻辑依赖两个前提
        时间顺序、时间连续
        合并1分钟K线时处于性能及复杂度考虑，暂时不做分钟数检查，5分钟默认就是最后的5根1分钟K线，15分钟就是最后15根K线
        对于存在位移的N分钟K线，不补充合并每日开始之前、及连接结束时的shift_n分钟K线
        :param md_dic_list_sub: 
        :param period_min: 
        :param shift_min: 
        :return: 
        """
        md_dic_list_sub_len = len(md_dic_list)
        if md_dic_list_sub_len <= 1:
            md_k_dic = None
        else:
            # 将各个分钟线数据合并
            # 基于假设该list为时间顺序排列
            # 提取最后一分钟K线数据为基准数据
            md_k_dic = md_dic_list[-1].copy()
            md_k_dic['period_min'] = period_min
            md_k_dic['shift_min'] = shift_n_min
            high_last = md_k_dic['high']
            low_last = md_k_dic['low']
            vol = md_k_dic['vol']
            amount = md_k_dic['amount']
            #
            # min_num = int(md_dic_last['UpdateTime'].split(':')[1])
            # min_num_start = min_num - period_min + 1
            is_none = True
            # 循环合并，最后一根K线的 vol 等数据以及在循环外导出，不可以重复计算，因此，最后一列要抛弃
            for row_num, md_dic in enumerate(md_dic_list[-period_min:-1]):
                if row_num == 0:
                    # UpdateTime 按照第一根K线时间为准
                    md_k_dic['UpdateTime'] = md_dic['UpdateTime']
                    md_k_dic['TradingDay'] = md_dic['TradingDay']
                    md_k_dic['ActionDay'] = md_dic['ActionDay']
                    md_k_dic['ActionTime'] = md_dic['ActionTime']
                high_curr = md_dic['high']
                if high_curr > high_last:
                    md_k_dic['high'] = high_curr
                    high_last = high_curr
                low_curr = md_dic['low']
                if low_curr < low_last:
                    md_k_dic['low'] = low_curr
                    low_last = low_curr
                vol += md_dic['vol']
                amount += md_k_dic['amount']
                # 最后一条K线数据的行号
            else:
                # print('\nrun here\n')
                md_k_dic['close'] = md_dic['close']
                md_k_dic['vol'] = vol
                md_k_dic['amount'] = amount
                md_k_dic['TradingDayEnd'] = md_dic['TradingDay']
                md_k_dic['UpdateTimeEnd'] = md_dic['UpdateTime']
                md_k_dic['ActionDayEnd'] = md_dic['ActionDay']
                md_k_dic['ActionTimeEnd'] = md_dic['ActionTime']
                md_k_dic['ActionDateTime'] = md_dic['ActionDateTime']
                is_none = False
                # 处理时间相关数据 'UpdateTime'
                # updatetime_str = Config.exch_datetime.strftime(Config.TIME_FORMAT_STR)
                # md_k_dic['ActionTime'] = updatetime_str
                # self.logger.debug('合成%s %d分钟数据[%+d] %s %s - %s %s | %s',
                #                   self.name, period_min, shift_min,
                #                   md_k_dic['TradingDay'], md_k_dic['UpdateTime'],
                #                   md_k_dic['TradingDayEnd'], md_k_dic['UpdateTimeEnd'],
                #                   md_k_dic)
            if is_none:
                md_k_dic = None

        # 发送事件
        if md_k_dic is not None:
            instrument_id = md_k_dic['InstrumentID']
            # 发送 MinN 事件
            if period_type == PeriodType.Min5:
                event_type = EventType.Min5_MD_Event
            elif period_type == PeriodType.Min15:
                event_type = EventType.Min15_MD_Event
            else:
                raise ValueError('不支持 %s 周期的数据' % period_type)
            if shift_n_min == 0:
                event_agent.send_event(event_type, md_k_dic, key=instrument_id)
            else:
                event_agent.send_event(event_type, md_k_dic, key=(instrument_id, shift_n_min))

            self.md_k_dic_last = md_k_dic

    def is_end_of_k(self, md_dic_list):
        """
        判断是否符合k线结束条件，根据 self.shift_n_min_list 返回每一个值对应的 True/False
        :param md_dic_list: 
        :return: 返回 period_is_end_of_k_dic
        """
        md_len = len(md_dic_list)
        md_dic_last = md_dic_list[-1]
        min_num = int(md_dic_last['UpdateTime'].split(':')[1])
        # 计算每一个偏移量是否复核K线结束条件
        # 比如：5分钟K线，每4、9分钟线时为最后一根K线
        period_is_end_of_k_dic = {period_type:
                                      (period_min,
                                       {shift_min:
                                            (min_num - shift_min + 1) % period_min == 0 if (
                                                                                                   md_len - shift_min - period_min) > 0 else False
                                        for shift_min in shift_min_set})
                                  for period_type, (period_min, shift_min_set) in self.period_shift_min_set_dic.items()}
        # 计算最长的偏移是多少，加上周期长度，获得最长需要历史数据长度
        max_period_len = 0
        for period_type, (period_min, shift_min_set_dic) in period_is_end_of_k_dic.items():
            shift_min_list = [shift_n_min for shift_n_min, is_end in shift_min_set_dic.items() if is_end]
            if len(shift_min_list) > 0:
                max_period_len_tmp = max(shift_min_list) + period_min  # 加上周期长度
                max_period_len = max_period_len if max_period_len > max_period_len_tmp else max_period_len_tmp
        return period_is_end_of_k_dic, max_period_len

    def run(self):
        with self._mutex:
            if self.keep_running:
                return
            else:
                self.keep_running = True
        try:
            update_datetime = datetime(1990, 1, 1, 1, 1, 1)
            md_dic_list = []
            while self.keep_running:
                try:
                    min1_md_dic = self.queue_md.get(block=True, timeout=self.time_interval)
                    md_dic_list.append(min1_md_dic)
                    # 判断是否符合k线结束条件，根据 self.shift_n_min_list 返回每一个值对应的 True/False
                    period_is_end_of_k_dic, max_period_len = self.is_end_of_k(md_dic_list)
                    if max_period_len == 0:
                        continue
                    # 这个操作只是为了提升切片效率，直接传入 md_dic_list 没有任何影响
                    md_dic_list_sub = md_dic_list[max_period_len:]
                    for period_type, (period_min, shift_n_min_set_dic) in period_is_end_of_k_dic.items():
                        for shift_n_min, is_end_of_k in shift_n_min_set_dic.items():
                            if is_end_of_k:
                                # self.logger.debug('%2d分钟数据[%+d]：%s %s %s', period_min, shift_n_min,
                                #                   min1_md_dic['InstrumentID'], min1_md_dic['TradingDay'],
                                #                   min1_md_dic['UpdateTime'])
                                self.combine_publisher(md_dic_list_sub, period_type, period_min, shift_n_min)
                    # 标记完成
                    self.queue_md.task_done()
                except Empty:
                    # 超時设置只是为了循环检查是否结束线程，因此 Empty 不做处理
                    pass

            # 循环结束，对上面循环中未生成k线的数据，放弃
        finally:
            # 交易时段结束
            self.logger.info('%s job finished. 服务器时间：%s',
                             self.name, Config.exch_datetime.strftime(Config.DATETIME_FORMAT_STR))
            self.keep_running = False


class MdSaver(Thread):
    """
    用于异步将 md 存储到数据库中，该设计采用 productor consumer 模式
    目前仅考虑 productor 多线程接收 md ，MDSaver 单线程存储到数据库的情况
    暂不考虑多线程 MDSaver 存储的情况
    """

    def __init__(self, name, table_name, event_type: EventType, daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.queue_md = Queue()
        self.time_interval = 5
        self.timeout_of_get = 5
        self.data_count_of_bunch = 1000
        self.keep_running = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self._mutex = threading.Lock()
        self.queue_md = Queue()
        self.engine_md = Config.get_db_engine(Config.DB_SCHEMA_MD)
        self.session_maker = sessionmaker(bind=self.engine_md)
        self.session = None
        self.table_name = table_name
        self.md_orm_table = Table(table_name, MetaData(self.engine_md), autoload=True)
        self.md_orm_table_insert = self.md_orm_table.insert(on_duplicate_key_update=True)
        self.event_type = event_type
        self.period_type = EventType.try_2_period_type(event_type)
        self.last_time_of_save = time.time()
        # 将 md 数据推送到 queue 的队列，供批量保存使用
        event_agent.register_handler(event_type,
                                     self.queue_md.put_nowait,
                                     handler_name='%s queue -> %s' % (self.__class__.__name__, table_name))
        self.logger.info("建立 %s MdSaver 并注册成功，对应数据库表 %s", event_type, table_name)

    def run(self):
        """
        keep_getting_count 实际作用不大，只是debug的时候显示一下连续抓取了多少数据而已
        :return: 
        """
        with self._mutex:
            if self.keep_running:
                return
            else:
                self.keep_running = True

        # self.logger.info("启动 %s MdSaver，对应数据库表 %s", self.event_type, self.table_name)
        try:
            keep_getting_count = 0
            data_dic_list = []
            while self.keep_running:
                try:
                    md_dic = self.queue_md.get(timeout=self.timeout_of_get)  # 5 秒钟后激活 Empty 异常
                    data_dic_list.append(md_dic)
                    keep_getting_count += 1
                    # if self.event_type == EventType.Tick_MD_Event:
                    # self.logger.debug('%5d %s <<< %s',
                    #                   keep_getting_count, self.table_name, md_dic)
                    self.queue_md.task_done()
                    if len(data_dic_list) >= self.data_count_of_bunch \
                            and time.time() - self.last_time_of_save > self.time_interval:
                        self.bunch_insert(data_dic_list)
                except Empty:
                    self.bunch_insert(data_dic_list)
                    # 休息 N 秒继续插入
                    time.sleep(self.time_interval)
        finally:
            self.logger.info('%s job finished', self.name)
            self.keep_running = False

    def bunch_insert(self, data_dic_list: list):
        md_count = len(data_dic_list)
        if md_count > 0:
            self.save_md(data_dic_list)
            data_dic_list.clear()

    def save_md(self, data_dic_list):
        """
        保存md数据到数据库及文件
        :param data_dic_list:
        :param session:
        :return:
        """
        md_count = len(data_dic_list)
        if md_count == 0:
            return

        # 保存到数据库
        if self.session is None:
            self.session = self.session_maker()
        try:
            self.session.execute(self.md_orm_table_insert, data_dic_list)
            self.logger.info('%d 条数据保存到 %s 完成', md_count, self.table_name)
        except:
            self.logger.exception('%d 条数据保存到 %s 失败', md_count, self.table_name)

        # 保存到文件——功能仅供王淳使用
        if Config.OUTPUT_MIN_K_FILE_ENABLE and self.period_type in Config.OUTPUT_MIN_K_FILE_PERIOD_TYPE:
            for num, data_dic in enumerate(data_dic_list):
                try:
                    file_path = os.path.join(
                        Config.OUTPUT_FOLDER_PATH_K_FILES,
                        "%(InstrumentID)s_%(TradingDay)s_min%(period_min)d_%(shift_min)d.txt" % data_dic)
                    with open(file_path, 'a') as k_file:
                        k_file.write(
                            '%(InstrumentID)s,%(TradingDay)s,%(UpdateTime)s,%(open).6f,%(high).6f,%(low).6f,%(close).6f,%(vol)d,%(OpenInterest).6f,%(AveragePrice).6f\n' % data_dic)
                except:
                    self.logger.exception('%d) %s md save error:%s', num, self.event_type, data_dic)

        self.last_time_of_save = time.time()

    @staticmethod
    def factory(event_type: EventType):
        # name=Tick_MD_Event_Saver 类似
        if event_type == EventType.Tick_MD_Event:
            saver = MdSaver(name="%s_Saver" % str(event_type).split('.')[1], table_name="md_tick",
                            event_type=event_type, daemon=True)
        elif event_type == EventType.Min1_MD_Event:
            saver = MdSaver(name="%s_Saver" % str(event_type).split('.')[1], table_name="md_min_1",
                            event_type=event_type, daemon=True)
        elif event_type == EventType.Min5_MD_Event:
            saver = MdSaver(name="%s_Saver" % str(event_type).split('.')[1], table_name="md_min_n",
                            event_type=event_type, daemon=True)
        elif event_type == EventType.Min15_MD_Event:
            saver = MdSaver(name="%s_Saver" % str(event_type).split('.')[1], table_name="md_min_n",
                            event_type=event_type, daemon=True)
        else:
            raise ValueError("不支持事件类型：%s" % event_type)

        return saver


class MdMin1Saver(MdSaver):
    """
    （该功能已经废弃，由父类统一实现）
    用于异步将md存储到数据库中，该设计采用 productor consumer模式
    目前仅考虑 productor多线程接收md，MDSaver单线程存储到数据库的情况
    暂不考虑多线程MDSaver存储的情况
    """

    def __init__(self, name='MdMin1Saver'):
        super().__init__(name=name, table_name="md_min_1", event_type=EventType.Min1_MD_Event, daemon=True)


class MdPublisher:

    def __init__(self, event_type: EventType, key=None):
        self.event_type = event_type
        self.logger = logging.getLogger('MdPublisher')
        if event_type == EventType.Tick_MD_Event:
            period_type = PeriodType.Tick
        elif event_type == EventType.Min1_MD_Event:
            period_type = PeriodType.Min1
        elif event_type == EventType.Min5_MD_Event:
            period_type = PeriodType.Min5
        elif event_type == EventType.Min15_MD_Event:
            period_type = PeriodType.Min15
        else:
            raise ValueError("不支持事件类型：%s" % event_type)
        self.period_type = period_type
        self.channel_header = Config.REDIS_CHANNEL[period_type]
        event_agent.register_handler(event_type, self.publish_md,
                                     handler_name='%s %s' % (self.__class__.__name__, period_type),
                                     key=key)
        self.logger.info("建立 %s MdPublisher 并注册成功，可通过 PSUBSCRIBE %s* 进行订阅，'*'代表合约代码",
                         str(event_type), self.channel_header)

    def publish_md(self, depth_md_dic):
        """
        publish md data
        redis-cli 可以通过 PUBSUB CHANNELS 查阅活跃的频道
        PSUBSCRIBE pattern [pattern ...]
        SUBSCRIBE channel [channel ...]
        :param depth_md_dic: 
        :return: 
        """
        instrument_id = depth_md_dic['InstrumentID']
        channel = self.channel_header + instrument_id
        r = Config.get_redis()
        md_str = json.dumps(depth_md_dic)
        r.publish(channel, md_str)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    import time
    import pandas as pd
    from backend.fh_utils import timedelta_2_str, date_2_str

    instrument_id = 'rb1712'
    engine = Config.get_db_engine(Config.DB_SCHEMA_MD)
    data_df = pd.read_sql("SELECT DISTINCT * FROM md_min_1 WHERE InstrumentID=%s ORDER BY TradingDay, UpdateTime",
                          engine,
                          params=[instrument_id]
                          )
    data_df['TradingDay'] = data_df['TradingDay'].apply(date_2_str)
    data_df['ActionDay'] = data_df['ActionDay'].apply(date_2_str)
    data_df['UpdateTime'] = data_df['UpdateTime'].apply(timedelta_2_str)
    data_df['ActionTime'] = data_df['ActionTime'].apply(timedelta_2_str)
    md_dic_list = data_df.to_dict('record')
    logging.info('清理数据')
    with Config.with_db_session(engine) as session:
        session.execute('DELETE FROM md_min_n WHERE InstrumentID=:instrument_id',
                        params={'instrument_id': instrument_id})
    # period_shift_min_set_dic = {PeriodType.Min5: {0, 1},
    #                             PeriodType.Min15: {0, 3},
    #                             }
    period_shift_min_set_dic = {PeriodType.Min5: {0}
                                }
    # md_saver = MdMinNSaver(instrument_id, period_shift_min_set_dic)
    # md_saver.start()

    for md_dic in md_dic_list:
        event_agent.send_event(EventType.Min1_MD_Event, md_dic, instrument_id)
    time.sleep(5)
    logging.info('md_saver.join()')
