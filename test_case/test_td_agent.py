# -*- coding: utf-8 -*-
"""
Created on 2017/10/15
@author: MG
"""

import logging
import unittest
import pandas as pd
from unittest.mock import Mock, patch
from pyctp_api import MyMdApi
from config import Config, Direction, Action, RunMode
import time
from datetime import timedelta
import sys
from td_agent import BacktestTraderAgent, RealTimeTraderAgent
from backend.orm import OrderInfo, TradeInfo, PosStatusInfo
logger = logging.getLogger(__name__)


class TestBacktestTraderAgent(unittest.TestCase):

    def setUp(self):
        # 清空数据库相关记录
        stg_run_id = 1
        OrderInfo.remove_order_info(stg_run_id)
        TradeInfo.remove_trade_info(stg_run_id)
        PosStatusInfo.remove_pos_status_info(stg_run_id)

    def test_open_long(self):
        stg_run_id = 1
        trade_agent = BacktestTraderAgent(stg_run_id)
        trade_agent.connect()
        trade_agent.curr_md = {'ActionDay': '2017-2-1', 'ActionTime': '10:30:21', 'ActionMillisec': 500}
        instrument_id = 'rb1712'
        order_price = 12345.5
        order_vol = 2
        trade_agent.open_long(instrument_id, order_price, order_vol)
        # 检查 order_info 表
        data_df = pd.read_sql('select * from order_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Long))
        self.assertEqual(data_df["action"][0], int(Action.Open))
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)
        # 检查 trade_info 表
        data_df = pd.read_sql('select * from trade_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Long))
        self.assertEqual(data_df["action"][0], int(Action.Open))
        self.assertEqual(data_df["trade_price"][0], order_price)
        self.assertEqual(data_df["trade_vol"][0], order_vol)
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)

        # 检查 pos_status_info 表
        data_df = pd.read_sql('select * from pos_status_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Long))
        self.assertEqual(data_df["avg_price"][0], order_price)
        self.assertEqual(data_df["position"][0], order_vol)
        self.assertEqual(data_df["cur_price"][0], order_price)

    def test_open_long_ActionTime(self):
        stg_run_id = 1
        run_mode_realtime_params = {
            'run_mode': RunMode.Backtest,
            'enable_timer_thread': True,
            'seconds_of_timer_interval': 15,
        }
        trade_agent = BacktestTraderAgent(stg_run_id, run_mode_realtime_params)
        trade_agent.connect()
        trade_agent.curr_md = {'ActionDay': '2017-2-1',
                               'ActionTime': pd.Timedelta(hours=10, minutes=30, seconds=21),  # '10:30:21',
                               'ActionMillisec': 500}
        instrument_id = 'rb1712'
        order_price = 12345.5
        order_vol = 2
        trade_agent.open_long(instrument_id, order_price, order_vol)
        # 检查 order_info 表
        data_df = pd.read_sql('select * from order_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Long))
        self.assertEqual(data_df["action"][0], int(Action.Open))
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)
        # 检查 trade_info 表
        data_df = pd.read_sql('select * from trade_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Long))
        self.assertEqual(data_df["action"][0], int(Action.Open))
        self.assertEqual(data_df["trade_price"][0], order_price)
        self.assertEqual(data_df["trade_vol"][0], order_vol)
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)

        # 检查 pos_status_info 表
        data_df = pd.read_sql('select * from pos_status_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Long))
        self.assertEqual(data_df["avg_price"][0], order_price)
        self.assertEqual(data_df["position"][0], order_vol)
        self.assertEqual(data_df["cur_price"][0], order_price)

    def test_close_long(self):
        stg_run_id = 1
        trade_agent = BacktestTraderAgent(stg_run_id)
        trade_agent.connect()
        trade_agent.curr_md = {'ActionDay': '2017-2-1', 'ActionTime': '10:30:21', 'ActionMillisec': 500}
        instrument_id = 'rb1712'
        order_price = 12345.5
        order_vol = 2
        self.assertRaises(ValueError,
                          trade_agent.close_long,
                          instrument_id, order_price, order_vol)
        # 检查 order_info 表
        data_df = pd.read_sql('select * from order_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Long))
        self.assertEqual(data_df["action"][0], int(Action.Close))
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)
        # 检查 trade_info 表
        data_df = pd.read_sql('select * from trade_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Long))
        self.assertEqual(data_df["action"][0], int(Action.Close))
        self.assertEqual(data_df["trade_price"][0], order_price)
        self.assertEqual(data_df["trade_vol"][0], order_vol)
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)

        # 检查 pos_status_info 表
        data_df = pd.read_sql('select * from pos_status_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 0)

    def test_open_short(self):
        stg_run_id = 1
        trade_agent = BacktestTraderAgent(stg_run_id)
        trade_agent.connect()
        trade_agent.curr_md = {'ActionDay': '2017-2-1', 'ActionTime': '10:30:21', 'ActionMillisec': 500}
        instrument_id = 'rb1712'
        order_price = 12345.5
        order_vol = 2
        trade_agent.open_short(instrument_id, order_price, order_vol)
        # 检查 order_info 表
        data_df = pd.read_sql('select * from order_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Short))
        self.assertEqual(data_df["action"][0], int(Action.Open))
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)
        # 检查 trade_info 表
        data_df = pd.read_sql('select * from trade_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Short))
        self.assertEqual(data_df["action"][0], int(Action.Open))
        self.assertEqual(data_df["trade_price"][0], order_price)
        self.assertEqual(data_df["trade_vol"][0], order_vol)
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)

        # 检查 pos_status_info 表
        data_df = pd.read_sql('select * from pos_status_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Short))
        self.assertEqual(data_df["avg_price"][0], order_price)
        self.assertEqual(data_df["position"][0], order_vol)
        self.assertEqual(data_df["cur_price"][0], order_price)

    def test_close_short(self):
        stg_run_id = 1
        trade_agent = BacktestTraderAgent(stg_run_id)
        trade_agent.connect()
        trade_agent.curr_md = {'ActionDay': '2017-2-1', 'ActionTime': '10:30:21', 'ActionMillisec': 500}
        instrument_id = 'rb1712'
        order_price = 12345.5
        order_vol = 2
        self.assertRaises(ValueError,
                          trade_agent.close_short,
                          instrument_id, order_price, order_vol)
        data_df = pd.read_sql('select * from order_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        # 检查 order_info 表
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Short))
        self.assertEqual(data_df["action"][0], int(Action.Close))
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)
        # 检查 trade_info 表
        data_df = pd.read_sql('select * from trade_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 1)
        self.assertEqual(data_df["direction"][0], int(Direction.Short))
        self.assertEqual(data_df["action"][0], int(Action.Close))
        self.assertEqual(data_df["trade_price"][0], order_price)
        self.assertEqual(data_df["trade_vol"][0], order_vol)
        self.assertEqual(data_df["order_price"][0], order_price)
        self.assertEqual(data_df["order_vol"][0], order_vol)

        # 检查 pos_status_info 表
        data_df = pd.read_sql('select * from pos_status_info where stg_run_id=%s',  #  and instrument_id=%s and order_price=%f and order_vol=%d
                              trade_agent.engine,
                              params=[stg_run_id])  # , instrument_id, order_price, order_vol
        self.assertEqual(data_df.shape[0], 0)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    unittest.main()
