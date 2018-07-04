# -*- coding: utf-8 -*-
"""
Created on 2017/6/24
@author: MG
"""

import logging
import unittest
from config import Config, with_mongo_collection
from collections import OrderedDict
from pymongo.errors import DuplicateKeyError
import time
from datetime import datetime, timedelta

logger = logging.getLogger()


class ConfigTest(unittest.TestCase):
    def test_update_instrument_info(self):
        Config.available_instrument_info_dic = {}
        Config.available_instrument_info_dic['cu1802'] = {
            'PositionType': '2', 'UnderlyingMultiple': 0, 'ProductClass': '1',
            'ExchangeInstID': 'cu1802', 'VolumeMultiple': 5, 'ExchangeID': 'SHFE',
            'StrikePrice': 0,
            'CombinationType': '0', 'DeliveryMonth': 2, 'ProductID': 'cu', 'MinLimitOrderVolume': 1,
            'CreateDate': '20170111', 'MinMarketOrderVolume': 1, 'PriceTick': 10.0, 'EndDelivDate': '20180223',
            'InstrumentID': 'cu1802', 'OptionsType': '\x00', 'ShortMarginRatio': 0.08, 'PositionDateType': '1',
            'InstLifePhase': '1', 'InstrumentName': '铜1802修改一下', 'MaxLimitOrderVolume': 500, 'DeliveryYear': 2018,
            'ExpireDate': '20180215', 'LongMarginRatio': 0.08, 'IsTrading': 1, 'StartDelivDate': '20180219',
            'UnderlyingInstrID': '', 'OpenDate': '20170216', 'MaxMarketOrderVolume': 500, 'MaxMarginSideAlgorithm': '1'}
        Config.update_instrument()

        Config.available_instrument_info_dic = {}
        Config.available_instrument_info_dic['cu1802'] = {
            'PositionType': '2', 'UnderlyingMultiple': 0, 'ProductClass': '1',
            'ExchangeInstID': 'cu1802', 'VolumeMultiple': 5, 'ExchangeID': 'SHFE',
            'StrikePrice': 0,
            'CombinationType': '0', 'DeliveryMonth': 2, 'ProductID': 'cu', 'MinLimitOrderVolume': 1,
            'CreateDate': '20170111', 'MinMarketOrderVolume': 1, 'PriceTick': 10.0, 'EndDelivDate': '20180223',
            'InstrumentID': 'cu1802', 'OptionsType': '\x00', 'ShortMarginRatio': 0.08, 'PositionDateType': '1',
            'InstLifePhase': '1', 'InstrumentName': '铜1802', 'MaxLimitOrderVolume': 500, 'DeliveryYear': 2018,
            'ExpireDate': '20180215', 'LongMarginRatio': 0.08, 'IsTrading': 1, 'StartDelivDate': '20180219',
            'UnderlyingInstrID': '', 'OpenDate': '20170216', 'MaxMarketOrderVolume': 500, 'MaxMarginSideAlgorithm': '1'}
        Config.update_instrument()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    unittest.main()
