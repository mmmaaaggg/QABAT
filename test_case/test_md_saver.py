# -*- coding: utf-8 -*-
"""
Created on 2017/11/17
@author: MG
"""
import pandas as pd
import time
from event_agent import event_agent, EventType
from config import Config, PeriodType
from md_saver import MdMinNSaver
from backend.fh_utils import timedelta_2_str, date_2_str
import unittest
import logging
logger = logging.getLogger(__name__)


class TestMDSaver(unittest.TestCase):
    """ 
    MyMdApi class 的自动化测试案例
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_md_saver(self):
        instrument_id = 'rb1712'
        engine = Config.get_db_engine(Config.DB_SCHEMA_MD)
        data_df = pd.read_sql('select DISTINCT * from md_min_1 where InstrumentID=%s order by TradingDay, UpdateTime',
                              engine,
                              params=[instrument_id]
                              )
        data_df['TradingDay'] = data_df['TradingDay'].apply(date_2_str)
        data_df['ActionDay'] = data_df['ActionDay'].apply(date_2_str)
        data_df['UpdateTime'] = data_df['UpdateTime'].apply(timedelta_2_str)
        data_df['ActionTime'] = data_df['ActionTime'].apply(timedelta_2_str)
        md_dic_list = data_df.to_dict('record')
        logging.info('清理数据')
        with Config.with_db_session(engine) as session:
            session.execute('delete from md_min_n where InstrumentID=:instrument_id',
                            params={'instrument_id': instrument_id})
        period_shift_min_set_dic = {PeriodType.Min5: {0, 1},
                                    PeriodType.Min15: {0, 3},
                                    }
        # period_shift_min_set_dic = {PeriodType.Min5: {1}
        #                             }
        md_saver = MdMinNSaver(instrument_id, period_shift_min_set_dic)
        md_saver.start()

        for md_dic in md_dic_list:
            event_agent.send_event(EventType.Min1_MD_Event, md_dic, instrument_id)
        time.sleep(5)
        logging.info('md_saver.join()')
        md_saver.keep_running = False
        md_saver.join()