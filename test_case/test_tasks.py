# -*- coding: utf-8 -*-
import logging
import unittest
from config import Config, with_mongo_collection
from collections import OrderedDict
from pymongo.errors import DuplicateKeyError
import time

logger = logging.getLogger()


class TaskTest(unittest.TestCase):

    def test_check_process_alive(self):
        r = Config.get_redis(db=Config.CELERY_INFO_DIC['REDIS_DB_OTHER'])
        key = Config.CELERY_INFO_DIC['TASK_PID_KEY']
        pid = r.get(key)
        r.delete(key)
        self.assertFalse(r.get(key))
        if pid is not None:
            r.set(key, pid)
            self.assertEqual(r.get(key), pid)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    unittest.main()
