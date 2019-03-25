#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-3-21 下午2:57
@File    : test_tdapi_qry_investor_position.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import logging
import sys
import time
import unittest
from unittest.mock import Mock, patch
from test_case import get_trader_api_and_login

logger = logging.getLogger(__name__)


class SomeTest(unittest.TestCase):  # 继承unittest.TestCase
    def tearDown(self):
        self.trader_api.Release()

    def setUp(self):
        # 每个测试用例执行之前做操作
        self.trader_api = get_trader_api_and_login()

    @classmethod
    def tearDownClass(cls):
        # 必须使用 @ classmethod装饰器, 所有test运行完后运行一次
        pass

    @classmethod
    def setUpClass(cls):
        pass

    def test_req_qry_investor_position(self):
        """
        检查 ReqQryInvestorPosition -> OnRspQryInvestorPosition 请求查询投资者持仓
        :return:
        """
        logger.info(sys._getframe().f_code.co_name)
        self.call_count = 0

        def replaced_method(pInvestorPosition, pRspInfo, nRequestID, bIsLast):
            """替代模块内相应函数"""
            self.call_count += 1
            logger.info('OnRspQryInvestorPosition called with: %s %s %s %s', pInvestorPosition, pRspInfo,
                        nRequestID, bIsLast)
            # self.assertEqual(pRspInfo.ErrorID, 0)
            # self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
            self.assertEqual(nRequestID, 3)
            self.assertIn(bIsLast, {True, False})

        with patch.object(self.trader_api, 'OnRspQryInvestorPosition', Mock(wraps=replaced_method)) as mock_method:
            self.trader_api.RegisterFront()
            self.trader_api.Init()
            time.sleep(1)
            self.trader_api.ReqQryInvestorPosition()
            time.sleep(1)
            self.assertGreaterEqual(mock_method.call_count, 1)
            self.assertEqual(mock_method.call_count, self.call_count)


if __name__ == '__main__':
    unittest.main()  # 运行所有的测试用例
