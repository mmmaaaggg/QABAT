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

from pyctp_api import MyTraderApi


logger = logging.getLogger(__name__)


class SomeTest(unittest.TestCase):  # 继承unittest.TestCase
    def tearDown(self):
        self.traderapi.Release()

    def setUp(self):
        # 每个测试用例执行之前做操作
        self.InvestorID = b'071001'
        self.InstrumentID = b'rb1712'
        self.traderapi = MyTraderApi()

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

        def replaced_method(pInvestorPosition, pRspInfo, nRequestID, bIsLast):
            """替代模块内相应函数"""
            logger.info('OnRspQryInvestorPosition called with: %s %s %s %s', pInvestorPosition, pRspInfo,
                        nRequestID, bIsLast)
            # self.assertEqual(pRspInfo.ErrorID, 0)
            # self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
            self.assertEqual(nRequestID, 3)
            self.assertEqual(bIsLast, True)

        with patch.object(self.traderapi, 'OnRspQryInvestorPosition', Mock(wraps=replaced_method)) as mock_method:
            self.traderapi.RegisterFront()
            self.traderapi.Init()
            time.sleep(1)
            self.traderapi.ReqQryInvestorPosition()
            time.sleep(1)
            self.assertEqual(mock_method.call_count, 1)


if __name__ == '__main__':
    unittest.main()  # 运行所有的测试用例
