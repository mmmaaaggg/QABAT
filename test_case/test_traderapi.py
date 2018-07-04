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

test_OnRspSettlementInfoConfirm = False


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

    # 交易初始化

     def test_OnFrontConnected(self):
         """
         检查接口  OnRspUserLogin
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pRspUserLogin, pRspInfo, nRequestID, bIsLast):
            """替代模块内相应函数"""
            logger.info('OnRspUserLogin called with: %s %s %s %s', pRspUserLogin, pRspInfo, nRequestID, bIsLast)
            self.assertEqual(pRspInfo.ErrorID, 0)
            # self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
            self.assertEqual(nRequestID, 1)
            self.assertEqual(bIsLast, True)
    # mock test

         with patch.object(self.traderapi, 'OnRspUserLogin', Mock(wraps=replaced_method)) as mock_method:
            self.traderapi.RegisterFront()
            self.traderapi.Init()
            time.sleep(1)
            self.traderapi.ReqUserLogin()
            time.sleep(0.5)
            self.assertEqual(mock_method.call_count, 1)

     def test_OnRspError(self):
         """
         检查接口 OnRspError
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspError called with: %s %s %s ', pRspInfo, nRequestID, bIsLast)
             self.assertEqual(self.InstrumentID)
             self.assertEqual(pRspInfo.ErrorID, 0)
             self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
             self.assertEqual(nRequestID, 1)
             self.assertEqual(bIsLast, True)

         with patch.object(self.traderapi, 'OnRspError', Mock(wraps=replaced_method)) as mock_method:
             self.traderapi.RegisterFront()
             self.traderapi.Init()
             time.sleep(1)
             self.assertEqual(mock_method.call_count, 0)  # 这里调用次数为0的时候，可以正常运行

     def test_OnRspQrySettlementInfoConfirm(self):
         """
         检查 查询结算单确认的日期响应
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspQrySettlementInfoConfirm called with: %s %s %s %s', pSettlementInfoConfirm, pRspInfo,
                         nRequestID,
                         bIsLast)
             # Sself.assertEqual(pRspInfo.ErrorID, 0)
             # self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
             self.assertEqual(nRequestID, 2)  # nRequestID 2或是3
             self.assertEqual(bIsLast, True)

         with patch.object(self.traderapi, 'OnRspQrySettlementInfoConfirm', Mock(wraps=replaced_method)) as mock_method:
             self.traderapi.RegisterFront()
             self.traderapi.Init()
             time.sleep(1)
             self.traderapi.ReqQrySettlementInfoConfirm()
             time.sleep(1)
             self.assertEqual(mock_method.call_count, 2)

     def test_OnRspQrySettlementInfo(self):
         """
         检查 查询投资者结算信息响应
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pSettlementInfo, pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspQrySettlementInfo called with: %s %s %s %s', pSettlementInfo, pRspInfo, nRequestID,
                         bIsLast)
             # self.assertEqual(pRspInfo.ErrorID, 0)
             # self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
             self.assertEqual(nRequestID, 3)
             self.assertEqual(bIsLast, True)

         with patch.object(self.traderapi, 'OnRspQrySettlementInfo', Mock(wraps=replaced_method)) as mock_method:
             self.traderapi.RegisterFront()
             self.traderapi.Init()
             time.sleep(1)
             self.traderapi.ReqQrySettlementInfo()
             time.sleep(1)
             self.assertEqual(mock_method.call_count, 21)  # 调用次数每次都有变化

     def test_OnRspSettlementInfoConfirm(self):
         """
         检查 投资者结算结果确认响应
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspSettlementInfoConfirm called with: %s %s %s %s', pSettlementInfoConfirm, pRspInfo, nRequestID,
                        bIsLast)
            # self.assertEqual(pRspInfo.ErrorID, 0)
             self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
             self.assertEqual(nRequestID, 3)
             self.assertEqual(bIsLast, True)

         if test_OnRspSettlementInfoConfirm:
             with patch.object(self.traderapi, 'OnRspSettlementInfoConfirm', Mock(wraps=replaced_method)) as mock_method:
                 self.traderapi.RegisterFront()
                 self.traderapi.Init()
                 time.sleep(1)
                 self.traderapi.ReqSettlementInfoConfirm()
                 time.sleep(1)
                 self.assertEqual(mock_method.call_count, 1)

     # 交易准备
     def test_OnRspQryInstrumentMarginRate(self):
         """
         检查 查询合约保证金率响应
         :return:
         """
         logger.info(sys._getframe().f_code.co_name)

         def replaced_method(pInstrumentMarginRate, pRspInfo, nRequestID, bIsLast):
             """替代模块内相应函数"""
             logger.info('OnRspQryInstrumentMarginRate called with: %s %s %s %s', pInstrumentMarginRate, pRspInfo,
                            nRequestID, bIsLast)
             #self.assertEqual(pRspInfo.ErrorID, 0)
             # self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
             self.assertEqual(nRequestID, 2)
             self.assertEqual(bIsLast, True)

         with patch.object(self.traderapi, 'OnRspQryInstrumentMarginRate', Mock(wraps=replaced_method)) as mock_method:
             self.traderapi.RegisterFront()
             self.traderapi.Init()
             time.sleep(1)
             self.traderapi.ReqQryInstrumentMarginRate()
             time.sleep(1)
             self.assertEqual(mock_method.call_count, 1)

     def test_OnRspQryInstrument(self):
        """
        检查 查询合约响应
        :return:
        """
        logger.info(sys._getframe().f_code.co_name)

        def replaced_method(pInstrument, pRspInfo, nRequestID, bIsLast):
            """替代模块内相应函数"""
            logger.info('OnRspQryInstrument called with: %s %s %s %s', pInstrument, pRspInfo,
                        nRequestID, bIsLast)
            #self.assertEqual(pRspInfo.ErrorID, 0)
            #self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
            self.assertEqual(nRequestID, 3)
            self.assertEqual(bIsLast, False)

        with patch.object(self.traderapi, 'OnRspQryInstrument', Mock(wraps=replaced_method)) as mock_method:
            self.traderapi.RegisterFront()
            self.traderapi.Init()
            time.sleep(1)
            self.traderapi.ReqQryInstrument(b'rb17')
            time.sleep(1)
            self.assertEqual(mock_method.call_count, 6)# 以'rb17'开头的合约有6个

     def test_OnRspQryTradingAccount(self):
        """
        检查 查询资金账户响应
        :return:
        """
        logger.info(sys._getframe().f_code.co_name)

        def replaced_method(pTradingAccount, pRspInfo, nRequestID, bIsLast):
            """替代模块内相应函数"""
            logger.info('OnRspQryTradingAccount called with: %s %s %s %s', pTradingAccount, pRspInfo,
                            nRequestID, bIsLast)
            # self.assertEqual(pRspInfo.ErrorID, 0)
            # self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
            self.assertEqual(nRequestID, 3)
            self.assertEqual(bIsLast, True)

        with patch.object(self.traderapi, 'OnRspQryTradingAccount', Mock(wraps=replaced_method)) as mock_method:
            self.traderapi.RegisterFront()
            self.traderapi.Init()
            time.sleep(1)
            self.traderapi.ReqQryTradingAccount()
            time.sleep(1)
            self.assertEqual(mock_method.call_count, 1)

     def test_OnRspQryInvestorPosition(self):
        """
        检查 查询投资者持仓响应'
        :return:
        """
        logger.info(sys._getframe().f_code.co_name)

        def replaced_method(pInvestorPosition, pRspInfo, nRequestID, bIsLast):
            """替代模块内相应函数"""
            logger.info('OnRspQryInvestorPosition called with: %s %s %s %s', pInvestorPosition, pRspInfo,
                            nRequestID, bIsLast)
            #self.assertEqual(pRspInfo.ErrorID, 0)
            #self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
            self.assertEqual(nRequestID, 1)
            self.assertEqual(bIsLast, True)

        with patch.object(self.traderapi, 'OnRspQryInvestorPosition', Mock(wraps=replaced_method)) as mock_method:
            self.traderapi.RegisterFront()
            self.traderapi.Init()
            time.sleep(1)
            self.traderapi.ReqQryInvestorPosition()
            time.sleep(0.5)
            self.assertEqual(mock_method.call_count,1)

     def test_OnRspQryInvestorPositionDetail(self):
        """
        检查 查询投资者持仓明细响应
        :return:
        """
        logger.info(sys._getframe().f_code.co_name)

        def replaced_method(pInvestorPositionDetail, pRspInfo, nRequestID, bIsLast):
            """替代模块内相应函数"""
            logger.info('OnRspQryInvestorPositionDetail called with: %s %s %s %s', pInvestorPositionDetail, pRspInfo,
                            nRequestID, bIsLast)
            #self.assertEqual(pRspInfo.ErrorID, 0)
            #self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
            self.assertEqual(nRequestID, 3)
            self.assertEqual(bIsLast, True)

        with patch.object(self.traderapi, 'OnRspQryInvestorPositionDetail', Mock(wraps=replaced_method)) as mock_method:
            self.traderapi.RegisterFront()
            self.traderapi.Init()
            time.sleep(1)
            self.traderapi.ReqQryInvestorPositionDetail()
            time.sleep(1)
            self.assertEqual(mock_method.call_count, 9)

    #
    # def test_OnRspQryOrder(self):
    #     """
    #     检查 查询报单响应
    #     :return:
    #     """
    #     logger.info(sys._getframe().f_code.co_name)
    #
    #     def replaced_method(pOrder, pRspInfo, nRequestID, bIsLast):
    #         """替代模块内相应函数"""
    #         logger.info('OnRspQryOrder called with: %s %s %s %s', pOrder, pRspInfo,
    #                     nRequestID, bIsLast)
    #         self.assertEqual(pRspInfo.ErrorID, 0)
    #         self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
    #         self.assertEqual(nRequestID, 1)
    #         self.assertEqual(bIsLast, True)
    #
    #     with patch.object(self.traderapi, 'OnRspQryOrder', Mock(wraps=replaced_method)) as mock_method:
    #         self.traderapi.RegisterFront()
    #         self.traderapi.Init()
    #         time.sleep(1)
    #         self.traderapi.ReqQryOrder()
    #         time.sleep(1)
    #         self.assertEqual(mock_method.call_count, 1)
    #
    # def test_OnRspQryTrade(self):
    #     """
    #     检查 查询成交响应
    #     :return:
    #     """
    #     logger.info(sys._getframe().f_code.co_name)
    #
    #     def replaced_method(pTrade, pRspInfo, nRequestID, bIsLast):
    #         """替代模块内相应函数"""
    #         logger.info('OnRspQryTrade called with: %s %s %s %s', pTrade, pRspInfo,
    #                     nRequestID, bIsLast)
    #         self.assertEqual(pRspInfo.ErrorID, 0)
    #         self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
    #         self.assertEqual(nRequestID, 1)
    #         self.assertEqual(bIsLast, True)
    #
    #     with patch.object(self.traderapi, 'OnRspQryTrade', Mock(wraps=replaced_method)) as mock_method:
    #         self.traderapi.RegisterFront()
    #         self.traderapi.Init()
    #         time.sleep(1)
    #         self.traderapi.ReqQryTrade()
    #         time.sleep(0.5)
    #         self.assertEqual(mock_method.call_count, 1)

    #  # 交易操作
    # def test_OnRspOrderInsert(self):
    #     """
    #     检查 报单是否通过参数校验
    #     :return:
    #     """
    #     logger.info(sys._getframe().f_code.co_name)
    #
    #     def replaced_method(pInputOrder, pRspInfo, nRequestID, bIsLast):
    #         """替代模块内相应函数"""
    #         logger.info('OnRspOrderInsert called with: %s %s %s %s', pInputOrder, pRspInfo,
    #                      nRequestID, bIsLast)
    #         self.assertEqual(pRspInfo.ErrorID, 0)
    #         self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
    #         self.assertEqual(nRequestID, 1)
    #         self.assertEqual(bIsLast, True)
    #
    #     with patch.object(self.traderapi, 'OnRspOrderInsert', Mock(wraps=replaced_method)) as mock_method:
    #         self.traderapi.RegisterFront()
    #         self.traderapi.Init()
    #         time.sleep(1)
    #         self.traderapi.ReqOrderInsert[self.pInputOrderID, self.nRequestID]
    #         time.sleep(0.5)
    #         self.assertEqual(mock_method.call_count, 1)

     # def test_OnRspOrderAction(self):
     #     """
     #     检查 ctp撤单校验是否错误
     #     :param self:
     #     :return:
     #     """
     #
     #     logger.info(sys._getframe().f_code.co_name)
     #
     #     def replaced_method(pInputOrderAction, pRspInfo, nRequestID, bIsLast):
     #         """替代模块内相应函数"""
     #         logger.info('OnRspOrderAction called with: %s %s %s %s', pInputOrderAction, pRspInfo,
     #                           nRequestID, bIsLast)
     #         self.assertEqual(pRspInfo.ErrorID, 0)
     #         self.assertEqual(str(pRspInfo.ErrorMsg, encoding='GBK'), '正确')
     #         self.assertEqual(nRequestID, 1)
     #         self.assertEqual(bIsLast, True)
     #         my_name = ''
     #         print(my_name)
     #
     #     with patch.object(self.traderapi, 'OnRspOrderAction', Mock(wraps=replaced_method)) as mock_method:
     #        self.traderapi.RegisterFront()
     #        self.traderapi.Init()
     #        time.sleep(1)
     #        InputOrderAction=ApiStruct.InputOrderAction
     #        self.traderapi.ReqOrderAction[InputOrderAction, nRequestID]
     #        time.sleep(0.5)
     #        self.assertEqual(mock_method.call_count, 1)

     # def test_InputOrderAction(self):
     #     logger.info(sys._getframe().f_code.co_name)
     #
     #     def replaced_method(pInputOrderAction, pRspInfo, nRequestID, bIsLast):
     #        """替代模块内相应函数"""
     #        logger.info('InputOrderAction called with: %s %s %s %s',  BrokerID, InvestorID, OrderActionRef, nRequestID, bIsLast)
     #        self.assertEqual(pInputOrderAction.BrokerID, b'9999')
     #        self.assertEqual(pInputOrderAction.InvestorID, b'071001')
     #        self.assertEqual(pInputOrderAction.OrderActionRef, 0)
     #        self.assertEqual(pInputOrderAction.OrderRef, '')
     #        #self.assertEqual(pRspInfo.ErrorID, 0)
     #        #self.assertEqual(pRspInfo.ErrorMsg, b'CTP:No Error')
     #        self.assertEqual(nRequestID, 0)
     #        self.assertEqual(bIsLast, True)
     #
     #     # mock test
     #     with patch.object(self.traderapi, 'InputOrderAction', Mock(wraps=replaced_method)) as mock_method:
     #        self.traderapi.RegisterFront()
     #        self.traderapi.Init()
     #        time.sleep(1)
     #        self.assertEqual(mock_method.call_count, 1)
#
#
#
     # def test_logout(self):
     #     self.mdapi.UnSubscribeMarketData([b'rb1712'])
     #    self.mdapi.ReqUserLogout()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    unittest.main()


