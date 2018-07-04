# -*- coding: utf-8 -*-
"""
Created on 2017/6/23
@author: MG
"""

import logging
import unittest
from pyctp_api import ApiBase, StrategyOrder
from ctp import ApiStruct
from config import Config, with_mongo_collection
from datetime import datetime
import sys
import numpy as np

logger = logging.getLogger(__name__)


class UnitTest1(unittest.TestCase):
    """
    StrategyOrder class 的自动化测试案例
    """

    def test_class_str(self):
        stg_order = StrategyOrder(0, 1, 2, b'1', None)
        self.assertEqual(str(stg_order), "StrategyOrder(front_id=1, session_id=2, order_ref=b'1')")

    def test_class_repr(self):
        stg_order = StrategyOrder(0, 1, 2, b'1', None)
        self.assertEqual('%r' % stg_order, "StrategyOrder(front_id=1, session_id=2, order_ref=b'1')")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    unittest.main()
