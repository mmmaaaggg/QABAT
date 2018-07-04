# -*- coding: utf-8 -*-
"""
Created on 2017/6/11
@author: MG
"""
import logging
import unittest
from unittest.mock import Mock, patch
from pyctp_api import MyTraderApi
from ctp import ApiStruct
from config import Config
import time
import sys
logger = logging.getLogger(__name__)


class InputOrderAction(unittest.TestCase):
    """
    测试InputOrderAction中的相关信息
    """

    def setUp(self):
        self.InvestorID = b'071001'
        self.InstrumentID = b'rb1712'
        self.traderapi = MyTraderApi()
        self.InputOrderAction=ApiStruct.InputOrderAction

    def tearDown(self):
        self.traderapi.Release()

    def test_login(self):
        """
        检查接口 OnFrontConnected
        :return:
        """
        logger.info(sys._getframe().f_code.co_name)
        OnFrontConnected = Mock()
        with patch.object(self.traderapi, 'OnFrontConnected', OnFrontConnected):
            self.traderapi.RegisterFront()
            self.traderapi.Init()
            time.sleep(1)
            OnFrontConnected.assert_called_once_with()

    def test_init(self):
        logger.info(sys._getframe().f_code.co_name)

        def replaced_method(self, BrokerID, InvestorID, OrderActionRef):
            """替代模块内相应函数"""
            logger.info('init called with: %s %s %s %s %s',  BrokerID, InvestorID, OrderActionRef, nRequestID, bIsLast)
            self.assertEqual(mock_method.call_count, 1)
            self.assertEqual(init.BrokerID, b'9999')
            self.assertEqual(init.InvestorID, b'071001')
            self.assertEqual(init.OrderActionRef, 0)
            #self.assertEqual(OrderRef, '')
            #self.assertEqual(pRspInfo.ErrorID, 0)
            #self.assertEqual(pRspInfo.ErrorMsg, b'CTP:No Error')
            self.assertEqual(nRequestID, 0)
            self.assertEqual(bIsLast, True)

        # mock test
        with patch.object(self.InputOrderAction, 'init', Mock(wraps=replaced_method)) as mock_method:
            self.traderapi.RegisterFront()
            self.traderapi.Init()
            time.sleep(1)
            self.assertEqual(mock_method.call_count, 1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    unittest.main()


