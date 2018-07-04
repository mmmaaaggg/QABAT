# -*- coding: utf-8 -*-
"""
Created on 2017/6/9
@author: MG
"""
import logging
import unittest
from unittest.mock import Mock, patch
from pyctp_api import MyMdApi
from config import Config
import time
import sys
logger = logging.getLogger(__name__)


class TestMyMdApi(unittest.TestCase):
    """ 
    MyMdApi class 的自动化测试案例
    """

    def setUp(self):
        self.InstrumentID = b'rb1712'
        self.mdapi = MyMdApi([self.InstrumentID])

    def tearDown(self):
        # self.mdapi.Join()
        self.mdapi.Release()

    def test_login(self):
         """
         检查接口 OnFrontConnected
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)
         OnFrontConnected = Mock()
         with patch.object(self.mdapi, 'OnFrontConnected', OnFrontConnected):
             self.mdapi.RegisterFront()
             self.mdapi.Init()
             time.sleep(1)
             OnFrontConnected.assert_called_once_with()

    def test_OnFrontConnected(self):
         """
         检查接口 OnRspUserLogin
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pRspUserLogin, pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspUserLogin called with: %s %s %s %s', pRspUserLogin, pRspInfo, nRequestID, bIsLast)
             self.assertEqual(pRspInfo.ErrorID, 0)
             self.assertEqual(pRspInfo.ErrorMsg, b'CTP:No Error')
             self.assertEqual(nRequestID, 0)
             self.assertEqual(bIsLast, True)
         # mock test
         with patch.object(self.mdapi, 'OnRspUserLogin', Mock(wraps=replaced_method)) as mock_method:
             self.mdapi.RegisterFront()
             self.mdapi.Init()
             time.sleep(1)
             self.assertEqual(mock_method.call_count, 1)

    # def test_OnRspUserLogin(self):
    #     """
    #     检查登录请求相应
    #     :return:
    #     """
    #     logger.info(sys._getframe().f_code.co_name)
    #
    #     def

    def test_OnRspSubMarketData(self):
         """
         检查接口 OnRspSubMarketData
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspSubMarketData called with: %s %s %s %s', pSpecificInstrument, pRspInfo, nRequestID, bIsLast)
             self.assertEqual(pSpecificInstrument.InstrumentID, self.InstrumentID)
             self.assertEqual(pRspInfo.ErrorID, 0)
             self.assertEqual(pRspInfo.ErrorMsg, b'CTP:No Error')
             self.assertEqual(nRequestID, 0)
             self.assertEqual(bIsLast, True)

         with patch.object(self.mdapi, 'OnRspSubMarketData', Mock(wraps=replaced_method)) as mock_method:
             self.mdapi.RegisterFront()
             self.mdapi.Init()
             time.sleep(1)
             # self.mdapi.SubscribeMarketData([b'asd', b'fdgdf'])
             # time.sleep(0.5)
             self.assertEqual(mock_method.call_count, 1)

    def test_OnRspUnSubMarketData(self):
        """
        检查接口 OnRspUnSubMarketData
        :return: 
        """
        logger.info(sys._getframe().f_code.co_name)

        def replaced_method(pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
            """替代模块内相应函数"""
            logger.info('OnRspUnSubMarketData called with: %s %s %s %s', pSpecificInstrument, pRspInfo, nRequestID,
                         bIsLast)
            self.assertEqual(pSpecificInstrument.InstrumentID, self.InstrumentID)
            self.assertEqual(pRspInfo.ErrorID, 0)
            self.assertEqual(pRspInfo.ErrorMsg, b'CTP:No Error')
            self.assertEqual(nRequestID, 0)
            self.assertEqual(bIsLast, True)

        with patch.object(self.mdapi, 'OnRspUnSubMarketData', Mock(wraps=replaced_method)) as mock_method:
            self.mdapi.RegisterFront()
            self.mdapi.Init()
            time.sleep(1)
            # self.mdapi.UnSubscribeMarketData([b'ctp'])
            self.mdapi.UnSubscribeMarketData([self.InstrumentID])
            time.sleep(1)
            self.assertEqual(mock_method.call_count, 1)

    def test_OnRspError(self):
        """
         检查接口 OnRspError
         :return:
        """
        logger.info(sys._getframe().f_code.co_name)

        def replaced_method(pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspError called with: %s %s %s ',  pRspInfo, nRequestID, bIsLast)
             self.assertEqual(self.InstrumentID)
             self.assertEqual(pRspInfo.ErrorID, 0)
             self.assertEqual(pRspInfo.ErrorMsg, b'CTP:No Error')
             self.assertEqual(nRequestID, 0)
             self.assertEqual(bIsLast, True)

        with patch.object(self.mdapi, 'OnRspError', Mock(wraps=replaced_method)) as mock_method:
            self.mdapi.RegisterFront()
            self.mdapi.Init()
            time.sleep(1)
            # self.mdapi.Error([self.InstrumentID])
            self.assertEqual(mock_method.call_count, 0)

    def test_OnRspUserLogout(self):
         """
         检查接口 OnRspUserLogout
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pRspUserLogout, pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspUserLogout called with: %s %s %s %s', pRspUserLogout, pRspInfo, nRequestID, bIsLast)
             self.assertEqual(pRspInfo.ErrorID, 0)
             self.assertEqual(pRspInfo.ErrorMsg, b'CTP:No Error')
             self.assertEqual(nRequestID, 0)
             self.assertEqual(bIsLast, True)
    #     # mock test

         with patch.object(self.mdapi, 'OnRspUserLogout', Mock(wraps=replaced_method)) as mock_method:
             self.mdapi.RegisterFront()
             self.mdapi.Init()
             time.sleep(1)
             self.assertEqual(mock_method.call_count, 1)

   # def test_OnRtnDepthMarketData(self):
       # """
        #检查接口 OnRtnDepthMarketData
        #:return:
        #"""
    # def test_logout(self):
    #     self.mdapi.UnSubscribeMarketData([b'rb1712'])
    #     self.mdapi.ReqUserLogout()

if __name__ == '__main__':
    # formatter = logging.Formatter(Config.LOG_FORMAT)
    # console_handle = logging.StreamHandler()
    # console_handle.setFormatter(formatter)
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    unittest.main()
