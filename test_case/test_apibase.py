# -*- coding: utf-8 -*-
"""
Created on 2017/6/22
@author: MG
"""

import logging
import unittest
from pyctp_api import ApiBase
from ctp import ApiStruct
from config import Config, with_mongo_collection
from datetime import datetime
import sys
import numpy as np

logger = logging.getLogger(__name__)


class TestMyTraderApi(unittest.TestCase):
    """
    MyTraderApi class 的自动化测试案例
    """

    def test_insert_one_2_db(self):
        price = np.random.random(1)[0]*10000
        p_struct = ApiStruct.InputOrder(BrokerID=b'123',  # 这一项作为标示，用于完成assert后的删除操作，因此要前后一致
                                        InvestorID=b'',
                                        InstrumentID=b'rb1710',
                                        UserID=b'asdf',
                                        OrderPriceType=ApiStruct.OPT_LimitPrice,
                                        Direction=ApiStruct.D_Buy,
                                        CombOffsetFlag=ApiStruct.OF_Open,
                                        CombHedgeFlag=ApiStruct.HF_Speculation,
                                        LimitPrice=price,
                                        VolumeTotalOriginal=1,
                                        TimeCondition=ApiStruct.TC_GFD
                                        )

        with_mongo_collection(lambda c:
                                   c.delete_many({'BrokerID': '123'}),
                                   'InputOrder')
        dic = ApiBase.insert_one_2_db(p_struct)
        # deal_with_mongo_collection(lambda c:c.insert_one(json_str),'InputOrder')
        with_mongo_collection(lambda c:
                                   self.assertEqual(len([raw for raw in c.find({'LimitPrice': price})]), 1),
                                   'InputOrder')
        with_mongo_collection(lambda c:
                                   c.delete_many({'BrokerID': '123'}),
                                   'InputOrder')

    def test_insert_one_2_db_2(self):
        price = np.random.random(1)[0]*10000
        p_struct = ApiStruct.InputOrder(BrokerID=b'123',  # 这一项作为标示，用于完成assert后的删除操作，因此要前后一致
                                        InvestorID=b'',
                                        InstrumentID=b'rb1710',
                                        UserID=b'asdf',
                                        OrderPriceType=ApiStruct.OPT_LimitPrice,
                                        Direction=ApiStruct.D_Buy,
                                        CombOffsetFlag=ApiStruct.OF_Open,
                                        CombHedgeFlag=ApiStruct.HF_Speculation,
                                        LimitPrice=price,
                                        VolumeTotalOriginal=1,
                                        TimeCondition=ApiStruct.TC_GFD
                                        )

        with_mongo_collection(lambda c:
                                   c.delete_many({'BrokerID': '123'}),
                                   'InputOrder')
        dic = ApiBase.insert_one_2_db(p_struct, **{'FrontID': 1, 'SessionID': 423})

        def check(collection):
            n = -1
            for n, raw in enumerate(collection.find({'LimitPrice': price})):
                print(raw)
                self.assertEqual(raw['FrontID'], 1)
                self.assertEqual(raw['SessionID'], 423)
            self.assertEqual(n, 0)

        with_mongo_collection(check, 'InputOrder')
        with_mongo_collection(lambda c:
                                   c.delete_many({'BrokerID': '123'}),
                                   'InputOrder')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    unittest.main()
