# -*- coding: utf-8 -*-
"""
Created on 2017/11/18
@author: MG
"""
import threading
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from stg_handler import StgBase, StgHandlerBase
from config import Config, PeriodType, RunMode, BacktestTradeMode, Direction, Action, PositionDateType
import os
from ctp.ApiStruct import PSD_History, PSD_Today, OF_CloseYesterday, OF_Close, OF_CloseToday


class ReadFileStg(StgBase):

    _folder_path = os.path.abspath(os.path.join(os.pardir, 'file_order'))

    def __init__(self):
        super().__init__()
        self._mutex = threading.Lock()
        self._last_check_datetime = datetime.now() - timedelta(minutes=1)
        self.interval_timedelta = timedelta(seconds=15)
        self.target_position = {}
        # 设定相应周期的事件驱动句柄 接收的参数类型
        self._on_period_event_dic[PeriodType.Tick].param_type = dict
        # 记录合约最近一次执行操作的时间
        self.instrument_last_deal_datetime = {}
        # 记录合约最近一个发送买卖请求的时间
        self.instrument_lastest_order_datetime_dic = {}
        # 目前由于交易是异步执行，在尚未记录每一笔订单的情况下，时间太短可能会导致仓位与请求但出现不同步现象，导致下单过多的问题
        self.timedelta_between_deal = timedelta(seconds=3)

    def on_timer(self):
        """
        每15秒进行一次文件检查
        :param md_df: 
        :param context: 
        :return: 
        """
        with self._mutex:
            # 检查最近一次文件检查的时间，避免重复查询
            if self._last_check_datetime + self.interval_timedelta > datetime.now():
                return
            # 获取文件列表
            file_name_list = os.listdir(self._folder_path)
            if file_name_list is None:
                # self.logger.info('No file')
                return
            # 读取所有 csv 文件
            position_df = None
            for file_name in file_name_list:
                file_base_name, file_extension = os.path.splitext(file_name)
                if file_extension.lower() != '.csv':
                    continue
                file_path = os.path.join(self._folder_path,file_name)
                position_df_tmp = pd.read_csv(file_path)
                if position_df is None:
                    position_df = position_df_tmp
                else:
                    position_df = position_df.append(position_df_tmp)

                # 文件备份
                backup_file_name = file_base_name + datetime.now().strftime('%Y-%m-%d %H_%M_%S') + file_extension + '.bak'
                # 调试阶段暂时不重命名备份，不影响程序使用
                os.rename(file_path, os.path.join(self._folder_path, backup_file_name))

            if position_df is None or position_df.shape[0] == 0:
                return
            # 检查多空目标持仓与当前持仓是否匹配
            # 不匹配则添加持仓任务
            long_pos_s = position_df[['Long_InstrumentID', 'Long_Position']].set_index('Long_InstrumentID')['Long_Position'].dropna()
            target_position = {}
            for instrument_id, position in long_pos_s.items():
                if instrument_id is None or pd.isna(instrument_id) or instrument_id == '':
                    continue
                # 检查当前持仓是否与目标持仓一致，如果一致则清空 self.target_position
                position_date_pos_info_dic = self.get_position(instrument_id)
                if position_date_pos_info_dic is not None and len(position_date_pos_info_dic) > 0:
                    position_cur = 0
                    for position_date, pos_info in position_date_pos_info_dic.items():
                        if pos_info.direction == Direction.Long:
                            position_cur += pos_info.position
                    if position_cur == position:
                        self.logger.info("%s 多头 %d 已经目标持仓要求", instrument_id, position)
                        if instrument_id in target_position:
                            del target_position[instrument_id]
                        continue

                # 当前合约累计持仓与目标持仓不一致，则添加目标持仓任务
                # 多头目标持仓
                target_position[instrument_id] = (Direction.Long, position)

            short_pos_s = position_df[['Short_InstrumentID', 'Short_Position']].set_index('Short_InstrumentID')['Short_Position'].dropna()
            for instrument_id, position in short_pos_s.items():
                if instrument_id is None or pd.isna(instrument_id) or instrument_id == '':
                    continue
                # 检查当前持仓是否与目标持仓一致，如果一致则清空 self.target_position
                position_date_pos_info_dic = self.get_position(instrument_id)
                if position_date_pos_info_dic is not None and len(position_date_pos_info_dic) > 0:
                    position_cur = 0
                    for position_date, pos_info in position_date_pos_info_dic.items():
                        if pos_info.direction == Direction.Short:
                            position_cur += pos_info.position
                    if position_cur == position:
                        self.logger.info("%s 空头 %d 已经目标持仓要求", instrument_id, position)
                        if instrument_id in target_position:
                            del target_position[instrument_id]
                        continue

                target_position[instrument_id] = (Direction.Short, position)

            self.target_position = target_position
            if len(target_position) > 0:
                self.logger.info('发现新的目标持仓指令\n%s', target_position)

    def do_order(self, md_dic, instrument_id, position, direction, offset_flag=OF_Close, msg=""):
        # position == 0 则代表无需操作
        # 执行交易
        if direction == Direction.Long:
            if position > 0:
                price = md_dic['AskPrice1']
                self.open_long(instrument_id, price, position)
                self.logger.info("%s %s -> 开多 %d %.0f", instrument_id, msg, position, price)
            elif position < 0:
                price = md_dic['BidPrice1']
                position_net = -position
                self.close_long(instrument_id, price, position_net, offset_flag)
                self.logger.info("%s %s -> 平多 %d %.0f", instrument_id, msg, position_net, price)
        else:
            if position > 0:
                price = md_dic['BidPrice1']
                self.open_short(instrument_id, price, position)
                self.logger.info("%s %s -> 开空 %d %.0f", instrument_id, msg, position, price)
            elif position < 0:
                price = md_dic['AskPrice1']
                position_net = -position
                self.close_short(instrument_id, price, position_net, offset_flag)
                self.logger.info("%s %s -> 平空 %d %.0f", instrument_id, msg, position_net, price)
        self.instrument_lastest_order_datetime_dic[instrument_id] = datetime.now()

    def on_tick(self, md_dic, context):
        """
        tick级数据进行交易操作
        :param md_dic: 
        :param context: 
        :return: 
        """
        if self.target_position is None or len(self.target_position) == 0:
            return
        if self.datetime_last_update_position is None:
            logging.debug("尚未获取持仓数据，跳过")
            return
        instrument_id = md_dic['InstrumentID']
        if instrument_id not in self.target_position:
            return
        # 如果的当前合约近期存在交易回报，则交易回报时间一定要小于查询持仓时间：
        # 防止出现以及成交单持仓信息未及时更新导致的数据不同步问题
        if instrument_id in self.datetime_last_rtn_trade_dic:
            if instrument_id not in self.datetime_last_update_position_dic:
                logging.debug("持仓数据中没有包含当前合约，最近一次成交回报时间：%s，跳过",
                              self.datetime_last_rtn_trade_dic[instrument_id])
                return
            if self.datetime_last_rtn_trade_dic[instrument_id] > self.datetime_last_update_position_dic[instrument_id]:
                logging.debug("持仓数据尚未更新完成，最近一次成交回报时间：%s，最近一次持仓更新时间：%s",
                              self.datetime_last_rtn_trade_dic[instrument_id],
                              self.datetime_last_update_position_dic[instrument_id])
                return
            # logging.debug("最近一次交易返回时间：%s，最近一次持仓更新时间：%s",
            #               self.datetime_last_rtn_trade_dic[instrument_id],
            #               self.datetime_last_update_position_dic[instrument_id])


        # 过于密集执行可能会导致重复下单的问题
        if instrument_id in self.instrument_last_deal_datetime:
            last_deal_datetime = self.instrument_last_deal_datetime[instrument_id]
            if last_deal_datetime + self.timedelta_between_deal > datetime.now():
                # logging.debug("最近一次交易时间：%s，防止交易密度过大，跳过", last_deal_datetime)
                return

        with self._mutex:

            # 撤销所有相关订单
            self.cancel_order(instrument_id)

            # 计算目标仓位方向及交易数量
            position_date_pos_info_dic = self.get_position(instrument_id)
            if position_date_pos_info_dic is None:
                # 如果当前无持仓，直接按照目标仓位进行开仓动作
                if instrument_id not in self.target_position:
                    # 当前无持仓，目标仓位也没有
                    pass
                else:
                    # 当前无持仓，直接按照目标仓位进行开仓动作
                    direction, position = self.target_position[instrument_id]
                    self.do_order(md_dic, instrument_id, position, direction,
                                  msg='当前无持仓')
            else:
                # 如果当前有持仓
                # 比较当前持仓总量与目标仓位是否一致
                if instrument_id not in self.target_position:
                    # 如果当前有持仓，目标仓位为空，则当前持仓无论多空全部平仓
                    for position_date, pos_info in position_date_pos_info_dic.items():
                        direction = pos_info.direction
                        position = pos_info.position
                        offset_flag = OF_Close if PositionDateType.Today == position_date else OF_CloseYesterday
                        self.do_order(md_dic, instrument_id, -position, direction,
                                      msg='目标仓位0，全部平仓', offset_flag=offset_flag)
                else:
                    # 如果当前有持仓，目标仓位也有持仓，则需要进一步比对
                    direction_target, position_target = self.target_position[instrument_id]
                    # 汇总全部同方向持仓，如果不够目标仓位，则加仓
                    # 对全部的反方向持仓进行平仓
                    position_holding = 0
                    for position_date, pos_info in position_date_pos_info_dic.items():
                        direction = pos_info.direction
                        position = pos_info.position
                        if direction != direction_target:
                            offset_flag = OF_Close if PositionDateType.Today == position_date else OF_CloseYesterday
                            self.do_order(md_dic, instrument_id, -position, direction,
                                          msg="目标仓位反向 %d，平仓" % position, offset_flag=offset_flag)
                            continue
                        else:
                            position_holding += position

                    # 如果持仓超过目标仓位，则平仓多出的部分，如果不足则补充多的部分
                    position_gap = position_target - position_holding
                    if position_gap > 0:
                        # 如果不足则补充多的部分
                        self.do_order(md_dic, instrument_id, position_gap, direction_target,
                                      msg="补充仓位")
                    elif position_gap < 0:
                        # 如果持仓超过目标仓位，则平仓多出的部分
                        # 优先平历史仓位
                        if PositionDateType.History in position_date_pos_info_dic:
                            pos_info = position_date_pos_info_dic[PositionDateType.History]
                            position_holding_tmp = pos_info.position
                            position_close = max([-position_holding_tmp, position_gap])
                            self.do_order(md_dic, instrument_id, position_close, direction_target,
                                          msg="持仓超量，平昨仓 %d" % position_holding_tmp, offset_flag=OF_CloseYesterday)
                            position_gap -= position_close

                        if position_gap < 0 and PositionDateType.Today in position_date_pos_info_dic:
                            pos_info = position_date_pos_info_dic[PositionDateType.Today]
                            position_holding_tmp = pos_info.position
                            position_close = max([-position_holding_tmp, position_gap])
                            self.do_order(md_dic, instrument_id, position_close, direction_target,
                                          msg="持仓超量，平今仓 %d" % position_holding_tmp, offset_flag=OF_CloseToday)
                            position_gap -= position_close

                        if position_gap < 0:
                            # 这种情况应该是程序出错，或者时间差导致的问题
                            self.logger.error("目前全部持仓不足以平掉多出的仓位")

        # 更新最近执行时间
        self.instrument_last_deal_datetime[instrument_id] = datetime.now()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    # 参数设置
    strategy_params = {}
    md_agent_params_list = [
        # {
        #     'name': 'min1',
        #     'md_period': PeriodType.Min1,
        #     'instrument_id_list': ['rb1805', 'i1801'],  # ['jm1711', 'rb1712', 'pb1801', 'IF1710'],
        #     'init_md_date_to': '2017-9-1',
        #     'dict_or_df_as_param': dict
        # },
        {
            'name': 'tick',
            'md_period': PeriodType.Tick,
            'instrument_id_list': ['rb1905', 'i1905'],  # ['jm1711', 'rb1712', 'pb1801', 'IF1710'],
        }]
    run_mode_realtime_params = {
        'run_mode': RunMode.Realtime,
        'enable_timer_thread': True,
        'seconds_of_timer_interval': 15,
    }
    run_mode_backtest_params = {
        'run_mode': RunMode.Backtest,
        'date_from': '2017-9-4',
        'date_to': '2017-9-27',
        'init_cash': 1000000,
        'trade_mode': BacktestTradeMode.Order_2_Deal
    }
    # run_mode = RunMode.BackTest
    # 初始化策略处理器
    stghandler = StgHandlerBase.factory(stg_class_obj=ReadFileStg,
                                        strategy_params=strategy_params,
                                        md_agent_params_list=md_agent_params_list,
                                        **run_mode_realtime_params)
    stghandler.start()
    time.sleep(120)
    stghandler.keep_running = False
    stghandler.join()
    logging.info("执行结束")
    # print(os.path.abspath(r'..\file_order'))

