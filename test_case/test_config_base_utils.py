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


class MongoTest(unittest.TestCase):
    """
    config.py 的自动化测试案例
    """

    def test_deal_with_mongo_collection(self):
        """
        mongodb 连续插入重复数据会报错，原因不详
        
        :return: 
        """
        odic = OrderedDict()
        odic['abc'] = 123
        odic['defg'] = 4567
        odic['xyz'] = 980
        odic['fhg'] = 456
        print(odic)
        collection_name = 'Hello'

        with self.assertRaises(DuplicateKeyError):
            for n in range(2):
                time.sleep(2)
                with_mongo_collection(lambda col: col.insert_one(odic), collection_name)
        with_mongo_collection(lambda col: col.delete_many(), collection_name)

    def test_deal_with_mongo_collection(self):
        """
        插入mongodb中记录字段的顺序，无关，都可以被find出来
        :return:
        """
        dic_list = [
            OrderedDict([('abc', 123), ('defg', 4567), ('xyz', 980), ('fhg', 456), ('n', 0)]),
            OrderedDict([('defg', 4567), ('abc', 123), ('xyz', 980), ('fhg', 456), ('n', 1)]),
            OrderedDict([('xyz', 980), ('abc', 123), ('defg', 4567), ('fhg', 456), ('n', 2)]),
            OrderedDict([('fhg', 456), ('abc', 123), ('defg', 4567), ('xyz', 980), ('n', 3)]),
            OrderedDict([('n', 4), ('abc', 123), ('defg', 4567), ('xyz', 980), ('fhg', 456)]),
        ]
        for odic in dic_list:
            print(odic)
        collection_name = 'Hello'
        with_mongo_collection(lambda c:
                                   c.delete_many({'abc': 123}), collection_name)
        with_mongo_collection(lambda col: col.insert_many(dic_list), collection_name)

        def check(col):
            """
            检查collection是否存在相应记录，顺序是否匹配
            :param col:
            :return:
            """
            for n, raw in enumerate(col.find({'abc': 123})):
                print(n, raw)
                self.assertEqual(raw['n'], n)

        with_mongo_collection(check, collection_name)
        with_mongo_collection(lambda c:
                                   c.delete_many({'abc': 123}), collection_name)


class RedisTest(unittest.TestCase):
    """
    config redis test
    """

    def test_redis(self):
        r = Config.get_redis()
        r.set('key1', 'hello world')
        self.assertEqual(r.get('key1'), b'hello world')
        r.delete('key1')
        self.assertFalse(r.exists('key1'))
        self.assertIsNone(r.get('key1'))


class THoursTest(unittest.TestCase):
    """
    测试 structed_thours(thours_str, target_date=None)
    """

    def test_if(self):
        instrument_id = 'if1712'
        datetime_range_list_result = Config.get_trade_datetime_range_list(instrument_id)
        datetime_now = datetime.now()
        datetime_next_day = datetime_now + timedelta(days=1)
        datetime_range_list_target = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15)),
        ]
        self.assertEqual(datetime_range_list_result, datetime_range_list_target)

    def test_tf(self):
        instrument_id = 'tf1712'
        datetime_range_list_result = Config.get_trade_datetime_range_list(instrument_id)
        datetime_now = datetime.now()
        datetime_next_day = datetime_now + timedelta(days=1)
        datetime_range_list_target = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 15),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 15)),
        ]
        self.assertEqual(datetime_range_list_result, datetime_range_list_target)


    def test_rb(self):
        instrument_id = 'rb1712'
        datetime_range_list_result = Config.get_trade_datetime_range_list(instrument_id)
        datetime_now = datetime.now()
        datetime_next_day = datetime_now + timedelta(days=1)
        datetime_range_list_target = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 15)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 0)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 21, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 23, 0)),
        ]
        self.assertEqual(datetime_range_list_result, datetime_range_list_target)

    def test_au(self):
        instrument_id = 'au1712'
        datetime_range_list_result = Config.get_trade_datetime_range_list(instrument_id)
        datetime_now = datetime.now()
        datetime_next_day = datetime_now + timedelta(days=1)
        datetime_range_list_target = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 15)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 0)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 21, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 2, 30) + timedelta(days=1)),
        ]
        self.assertEqual(datetime_range_list_result, datetime_range_list_target)

    def test_cu(self):
        instrument_id = 'cu1712'
        datetime_range_list_result = Config.get_trade_datetime_range_list(instrument_id)
        datetime_now = datetime.now()
        datetime_next_day = datetime_now + timedelta(days=1)
        datetime_range_list_target = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 15)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 0)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 21, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 1, 0) + timedelta(days=1)),
        ]
        self.assertEqual(datetime_range_list_result, datetime_range_list_target)

    def test_cf(self):
        instrument_id = 'cf1712'
        datetime_range_list_result = Config.get_trade_datetime_range_list(instrument_id)
        datetime_now = datetime.now()
        datetime_next_day = datetime_now + timedelta(days=1)
        datetime_range_list_target = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 15)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 0)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 21, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 23, 30) + timedelta(days=1)),
        ]
        self.assertEqual(datetime_range_list_result, datetime_range_list_target)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    unittest.main()
