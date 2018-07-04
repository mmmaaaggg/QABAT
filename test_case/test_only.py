import logging
import unittest
from unittest.mock import Mock, patch
from pyctp_api import MyTraderApi
from ctp import ApiStruct
from config import Config
import time
import sys

logger = logging.getLogger(__name__)

class TestMyTraderApi(unittest.TestCase):
    """
    MyTraderApi class 的自动化测试案例
    """

    def setUp(self):
        self.InvestorID = b'071001'
        self.InstrumentID = b'rb1712'
        self.traderapi = MyTraderApi()

    def tearDown(self):
        self.traderapi.Release()

    def test_OnRspOrderInsert(self):
         """
         检查 是否发出报单录入请求
         :param pInputOrder:
         :param pRspInfo:
         :param nRequestID:
         :param bIsLast:
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pInputOrder, pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspOrderInsert called with: %s %s %s %s', pInputOrder, pRspInfo,
                          nRequestID, bIsLast)
             self.assertEqual(pRspInfo.ErrorID, 0)
             self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
             self.assertEqual(nRequestID, 1)
             self.assertEqual(bIsLast, True)

         with patch.object(self.traderapi, 'OnRspOrderInsert', Mock(wraps=replaced_method)) as mock_method:
             self.traderapi.RegisterFront()
             self.traderapi.Init()
             time.sleep(1)
             InputOrder=ApiStruct.InputOrder
             # RequestID=InputOrder.RequestID
             self.traderapi.ReqOrderInsert()
             # [InputOrder], [InputOrder.RequestID])
             time.sleep(1)
             self.assertEqual(mock_method.call_count, 1)

    # def test_open_long(self):
    #     """
    #     检查 是否发出报单录入请求
    #     :param pInputOrder:
    #     :param pRspInfo:
    #     :param nRequestID:
    #     :param bIsLast:
    #     :return:
    #     """
    #     logger.info(sys._getframe().f_code.co_name)
    #     instrument_id, price, vol = b'rb1712', 2993, 1
    #     # InputOrder = ApiStruct.InputOrder
    #     def replaced_method(pInputOrder, pRspInfo, nRequestID, bIsLast):
    #         """替代模块内相应函数"""
    #         InputOrder = ApiStruct.InputOrder
    #         logger.info('OnRspOrderInsert called with: %s %s %s', instrument_id, price, vol)
    #         self.assertEqual(InputOrder.InstrumentID, instrument_id)
    #         self.assertEqual(InputOrder.LimitPrice, price)
    #         self.assertEqual(InputOrder.MinVolume, vol)
    #         # self.assertEqual(self.traderapi.strategy_id, '')
    #         self.assertEqual(pRspInfo.ErrorID, 0)
    #         self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
    #         self.assertEqual(nRequestID, 1)
    #         self.assertEqual(bIsLast, True)
    #
    #     with patch.object(self.traderapi, 'OnRspOrderInsert', Mock(wraps=replaced_method)) as mock_method:
    #         self.traderapi.RegisterFront()
    #         self.traderapi.Init()
    #         time.sleep(1)
    #         #self.traderapi._request_action_order_insert(self.traderapi.p_struct, self.traderapi.strategy_id)
    #         self.traderapi.open_long(self, instrument_id, price, vol)
    #         time.sleep(0.5)
    #         self.assertEqual(mock_method.call_count, 1)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    unittest.main()
#[self.pInputOrderID, self.nRequestID]