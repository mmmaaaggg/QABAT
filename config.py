# -*- coding: utf-8 -*-
"""
Created on 2017/6/9
@author: MG
"""
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import Table
import logging, threading, re, numpy as np
from logging.handlers import RotatingFileHandler
from backend.fh_utils import str_2_bytes, bytes_2_str
from redis import StrictRedis, ConnectionPool
from redis.client import PubSub
from datetime import timedelta, datetime
from celery.schedules import crontab
from enum import IntEnum, unique
import pandas as pd
from ctp.ApiStruct import D_Buy, D_Sell, PD_Long, PD_Short, PSD_Today, PSD_History
D_Buy_str, D_Sell_str = bytes_2_str(D_Buy), bytes_2_str(D_Sell)
PD_Long_str, PD_Short_str = bytes_2_str(PD_Long), bytes_2_str(PD_Short)
PSD_Today_str, PSD_History_str = bytes_2_str(PSD_Today), bytes_2_str(PSD_History)
logger = logging.getLogger()

@unique
class PeriodType(IntEnum):
    """
    周期类型
    """
    Tick = 0
    Min1 = 1
    Min5 = 2
    Min10 = 3
    Min15 = 4
    Min30 = 5
    Hour1 = 10
    Day1 = 20
    Week1 = 30
    Mon1 = 40

    @staticmethod
    def get_min_count(period_type) -> int:
        """
        返回给的周期类型对应的分钟数
        :param period_type: 
        :return: 
        """
        if PeriodType.Min1 == period_type:
            min_count = 1
        elif PeriodType.Min5 == period_type:
            min_count = 5
        elif PeriodType.Min10 == period_type:
            min_count = 10
        elif PeriodType.Min15 == period_type:
            min_count = 15
        elif PeriodType.Min30 == period_type:
            min_count = 30
        else:
            raise ValueError('不支持 %s 周期' % period_type)
        return min_count

@unique
class RunMode(IntEnum):
    """
    运行模式，目前支持两种：实时行情模式，回测模式
    """
    Realtime = 0
    Backtest = 1


@unique
class Direction(IntEnum):
    """买卖方向"""
    Short = 0  # 空头
    Long = 1  # 多头

    @staticmethod
    def create_by_direction(direction_str):
        if isinstance(direction_str, str):
            if direction_str == D_Buy_str:
                return Direction.Long
            elif direction_str == D_Sell_str:
                return Direction.Short
            else:
                raise ValueError('Direction不支持 %s' % direction_str)
        else:
            if direction_str == D_Buy:
                return Direction.Long
            elif direction_str == D_Sell:
                return Direction.Short
            else:
                raise ValueError('Direction不支持 %s' % direction_str)


    @staticmethod
    def create_by_posi_direction(posi_direction):
        if isinstance(posi_direction, str):
            if posi_direction == PD_Long_str:
                return Direction.Long
            elif posi_direction == PD_Short_str:
                return Direction.Short
            else:
                raise ValueError('Direction不支持 %s' % posi_direction)
        else:
            if posi_direction == PD_Long:
                return Direction.Long
            elif posi_direction == PD_Short:
                return Direction.Short
            else:
                raise ValueError('Direction不支持 %s' % posi_direction)


@unique
class Action(IntEnum):
    """开仓平仓"""
    Open = 0  # 开仓
    Close = 1  # 平仓
    ForceClose = 2  # 强平
    CloseToday = 3  # 平今
    CloseYesterday = 4  # 平昨
    ForceOff = 5  # 强减
    LocalForceClose = 6  # 本地强平

    @staticmethod
    def create_by_offsetflag(offset_flag):
        """
        将 Api 中 OffsetFlag 变为 Action 类型
        :param offset_flag: 
        :return: 
        """
        if offset_flag == '0':
            return Action.Open
        elif offset_flag in {'1', '5'}:
            return Action.Close
        elif offset_flag == '3':
            return Action.CloseToday
        elif offset_flag == '4':
            return Action.CloseYesterday
        elif offset_flag == '2':
            return Action.ForceClose
        elif offset_flag == '6':
            return Action.LocalForceClose
        else:
            raise ValueError('Action不支持 %s' % offset_flag)


@unique
class PositionDateType(IntEnum):
    """今日持仓历史持仓标示"""
    Today = 1  # 今日持仓
    History = 2  # 历史持仓

    @staticmethod
    def create_by_position_date(position_date):
        """
        将 Api 中 position_date 变为 PositionDate 类型
        :param position_date: 
        :return: 
        """
        if isinstance(position_date, str):
            if position_date == PSD_Today_str:
                return PositionDateType.Today
            elif position_date == PSD_History_str:
                return PositionDateType.History
            else:
                raise ValueError('PositionDate 不支持 %s' % position_date)
        else:
            if position_date == PSD_Today:
                return PositionDateType.Today
            elif position_date == PSD_History:
                return PositionDateType.History
            else:
                raise ValueError('PositionDate 不支持 %s' % position_date)


@unique
class BacktestTradeMode(IntEnum):
    """
    回测模式下的成交模式
    """
    Order_2_Deal = 0  # 一种简单回测模式，无论开仓平仓等操作，下单即成交
    MD_2_Deal = 1  # 根据下单后行情变化确定何时成交


@unique
class ContextKey(IntEnum):
    """
    策略执行逻辑中 context[key] 的部分定制化key
    """
    instrument_id_list = 0


class ClientWrapper:
    """用于对 MongoClient 对象进行封装，方便使用with语句进行close控制"""

    def __init__(self, client):
        self.client = client

    def __enter__(self) -> MongoClient:
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
        logger.debug('mongodb client closed')


class SessionWrapper:
    """用于对session对象进行封装，方便使用with语句进行close控制"""

    def __init__(self, session):
        self.session = session

    def __enter__(self) -> Session:
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        # logger.debug('db session closed')


class ConfigBase:
    # 配置环境变量
    DATETIME_FORMAT_STR = '%Y-%m-%d %H:%M:%S'
    DATE_FORMAT_STR = '%Y-%m-%d'
    TIME_FORMAT_STR = '%H:%M:%S'
    DATE_FORMAT_STR_CTP = '%Y%m%d'
    TIME_FORMAT_STR_CTP = '%H%M%S'
    # api configuration
    BROKER_ID = b'***'
    USER_ID = b'***'
    INVESTOR_ID = b'***'
    PASSWORD = b'***'
    # FRONT_TRADER_ADDRESS = b'tcp://0.0.0.0:10030'  # SIMNOW 第二套【7x24】
    FRONT_TRADER_ADDRESS = b'tcp://0.0.0.0:10000'  # SIMNOW
    FRONT_MD_ADDRESS = b'tcp://0.0.0.0:10010'  # SIMNOW

    WIND_REST_URL = "http://10.0.5.61:5000/wind/"  # "http://10.0.5.110:5000/wind/"  "http://10.0.3.78:5000/wind/"

    # mysql db info
    DB_SCHEMA_DEFAULT = 'default'
    DB_SCHEMA_MD = 'future_md'
    DB_SCHEMA_QABAT = 'qabat'
    DB_SCHEMA_PROD = 'fof_ams_db'
    DB_SCHEMA_PROPHETS_MD = 'md_db'
    DB_INFO_DIC = {
        DB_SCHEMA_DEFAULT: {'type': 'mysql',
                            'DB_IP': '10.0.3.66',
                            'DB_PORT': '3306',
                            'DB_NAME': 'fof_ams_dev',
                            'DB_USER': 'mg',
                            'DB_PASSWORD': '***'
                            },
        DB_SCHEMA_MD: {'type': 'mysql',
                       'DB_IP': '10.0.3.66',
                       'DB_PORT': '3306',
                       'DB_NAME': 'future_md',
                       'DB_USER': 'mg',
                       'DB_PASSWORD': '***'
                       },
        'future_md_test': {'type': 'mysql',
                       'DB_IP': '10.0.3.66',
                       'DB_PORT': '3306',
                       'DB_NAME': 'future_md_test',
                       'DB_USER': 'mg',
                       'DB_PASSWORD': '***'
                       },
        DB_SCHEMA_QABAT: {'type': 'mysql',
                          'DB_IP': '10.0.3.66',
                          'DB_PORT': '3306',
                          'DB_NAME': 'qabat',
                          'DB_USER': 'mg',
                          'DB_PASSWORD': '***'
                          },
        DB_SCHEMA_PROD: {'type': 'mysql',
                         'DB_IP': '10.0.5.111',
                         'DB_PORT': '3306',
                         'DB_NAME': 'fof_ams_db',
                         'DB_USER': 'mg',
                         'DB_PASSWORD': '***'
                         },
        DB_SCHEMA_PROPHETS_MD: {'type': 'mysql',
                         'DB_IP': '39.104.20.82',
                         'DB_PORT': '3306',
                         'DB_NAME': DB_SCHEMA_PROPHETS_MD,
                         'DB_USER': 'mg',
                         'DB_PASSWORD': '***'
                         },
    }
    # mongo db info
    MONGO_INFO_DIC = {'MONGO_DB_URL': '10.0.3.66',
                      'MONGO_DB_PORT': 27017,
                      'MONGO_DB_NAME': 'test',
                      }
    _MONGO_CLIENT = None

    # redis info
    # REDIS_INFO_DIC = {'REDIS_HOST': '127.0.0.1',
    #                   'REDIS_PORT': '6379',
    #                   }
    REDIS_INFO_DIC = {'REDIS_HOST': '192.168.239.131',
                      'REDIS_PORT': '6379',
                      }

    # celery schedule info
    CELERYBEAT_SCHEDULE = {
        'receive_save_md_moning': {
            'task': 'tasks.save_md',
            'schedule': timedelta(seconds=5)
        },
    }
    # evn configuration
    LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s %(filename)s.%(funcName)s:%(lineno)d|%(message)s'

    # 与预加载历史K线数据文件
    OUTPUT_MIN_K_FILE_PRELOAD_HIS_DATA_ENABLE = False
    # 与预加载历史K线数据文件，历史长度
    OUTPUT_MIN_K_FILE_PRELOAD_HIS_DATA_LEN = 100
    # 生成分钟K线文件到指定目录，该功能仅为王淳使用
    OUTPUT_MIN_K_FILE_ENABLE = False
    # 生产制定周期 分钟线文件
    OUTPUT_MIN_K_FILE_PERIOD_TYPE = {PeriodType.Min15}
    # 专供博士1分钟K线文件输出使用
    OUTPUT_FOLDER_PATH_K_FILES = r"d:\Downloads\KFiles"
    # 结算单文件保存位置
    OUTPUT_FOLDER_PATH_SETTLEMENT_FILES = r"d:\Downloads\Settlements"

    def __init__(self):
        """
        初始化一些基本配置信息
        """
        # 设置rest调用日志输出级别
        logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)

        # 数据库 engine 池
        self._db_engine_dic = {}
        # celery info
        self.CELERY_INFO_DIC = {
            'TASK_PID_KEY': "task:save_md:pid",
            'CELERY_TIMEZONE': 'Asia/Shanghai',
            'BROKER_URL': 'redis://%(REDIS_HOST)s:%(REDIS_PORT)s/3' % self.REDIS_INFO_DIC,
            'CELERY_RESULT_BACKEND': 'redis://%(REDIS_HOST)s:%(REDIS_PORT)s/4' % self.REDIS_INFO_DIC,
            'REDIS_DB_OTHER': '5',
        }

        # 当前可交易的合约信息，通过 TradeApi.ReqQryInstrument 查询后更新
        self._instrument_info_dic = {}
        self._instrument_info_dic_mutex = threading.Lock()
        # self.instrument_info_dic = {}
        self._instrument_commission_rate_dic = {}
        self._redis_connection_pool_dic = {}
        self._redis_client_dic = {}
        self._redis_pub_sub_dic = {}

        # 初始化channel信息，初始化后的信息类似如下：
        # {<PeriodType.Tick: 1>: 'md.Tick.',
        #  <PeriodType.Min1: 2>: 'md.Min1.',
        #  <PeriodType.Min5: 3>: 'md.Min5.',
        #  <PeriodType.Min15: 4>: 'md.Min15.',
        #  <PeriodType.Hour1: 5>: 'md.Hour1.',
        #  <PeriodType.Day1: 6>: 'md.Day1.',
        #  <PeriodType.Week1: 7>: 'md.Week1.',
        #  <PeriodType.Mon1: 8>: 'md.Mon1.'}
        self.REDIS_CHANNEL = {period: 'md.%s.' % str(period).split('.')[1] for period in PeriodType}

        # 上期所时间
        self._SHFETimeDelta = timedelta()
        # 大商所时间
        self._DCETimeDelta = timedelta()
        # 郑商所时间
        self._CZCETimeDelta = timedelta()
        # 中金所时间
        self._FFEXTimeDelta = timedelta()
        # 能源中心时间
        self._INETimeDelta = timedelta()
        # 服务器时差
        self._exch_timedelta = timedelta()

        datetime_now = datetime.now()
        self._day_thour_4_commodity = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 15)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 10, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 0)),
        ]
        self._moning_thour_4_FFEX_IFICIH = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 30),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 0)),
        ]
        self._moning_thour_4_FFEX_T = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 9, 15),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 11, 30)),
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 15)),
        ]
        self._night_thour_4_rb = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 21, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 23, 0)),
        ]
        self._night_thour_4_au_ag = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 21, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 2, 30) + timedelta(days=1)),
        ]
        self._night_thour_4_cu_al = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 21, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 1, 0) + timedelta(days=1)),
        ]
        self._night_thour_4_cf_jm = [
            (datetime(datetime_now.year, datetime_now.month, datetime_now.day, 21, 0),
             datetime(datetime_now.year, datetime_now.month, datetime_now.day, 23, 30) + timedelta(days=1)),
        ]

        # 期货合约列表、合约与交易所对照表 需要通过 load_instrument_list 函数进行初始化后才能使用
        # 合约与交易所对照表
        # self.instrument_market_dic = {}
        # 期货合约列表
        # self.subscribe_instrument_list = []
        # 加载 instrument_info 信息
        # self.load_instrument()  # 改功能已被 self.instrument_info_dic 属性替代
        # self.load_instrument_commission_rate()  # 改功能已被 self.instrument_commission_rate 属性替代
        # 当 调用api 完成 instrument 列表更新后，重置标志位
        self.update_instrument_finished = False

    @property
    def instrument_info_dic(self):
        with self._instrument_info_dic_mutex:
            if len(self._instrument_info_dic) == 0:
                engine = self.get_db_engine(schema_name=self.DB_SCHEMA_MD)
                try:
                    instrument_info_df = pd.read_sql('select * from instrument', engine)
                    self._instrument_info_dic = {record['InstrumentID']: record for record in
                                                instrument_info_df.to_dict('record')}
                except:
                    logger.exception("加载合约信息失败")
        return self._instrument_info_dic

    @property
    def instrument_commission_rate_dic(self):
        with self._instrument_info_dic_mutex:
            if len(self._instrument_commission_rate_dic) == 0:
                engine = self.get_db_engine(schema_name=self.DB_SCHEMA_MD)
                try:
                    instrument_info_df = pd.read_sql('select * from instrumentcommissionrate', engine)
                    self._instrument_commission_rate_dic = {record['InstrumentID']: record for record in
                                                           instrument_info_df.to_dict('record')}
                except:
                    logger.exception("加载合约手续费率信息失败")
        return self._instrument_commission_rate_dic

    def _init_redis_channel(self):
        """
        初始化channel信息，初始化后的信息类似如下：
        {<PeriodType.Tick: 1>: 'md.Tick.',
         <PeriodType.Min1: 2>: 'md.Min1.',
         <PeriodType.Min5: 3>: 'md.Min5.',
         <PeriodType.Min15: 4>: 'md.Min15.',
         <PeriodType.Hour1: 5>: 'md.Hour1.',
         <PeriodType.Day1: 6>: 'md.Day1.',
         <PeriodType.Week1: 7>: 'md.Week1.',
         <PeriodType.Mon1: 8>: 'md.Mon1.'}
        """
        # REDIS_CHANNEL_MD_DIC = 'md.%s.' % MD_PERIOD_TICK
        # REDIS_CHANNEL_MD_MINUTES = 'md.%s.' % MD_PERIOD_MIN
        self.REDIS_CHANNEL = {period: 'md.%s.' % str(period).split('.')[1] for period in PeriodType}

    @property
    def SHFETimeDelta(self):
        return self._SHFETimeDelta

    @SHFETimeDelta.setter
    def SHFETimeDelta(self, delta):
        self._SHFETimeDelta = delta
        delta_list = [self._FFEXTimeDelta, self._DCETimeDelta, self._SHFETimeDelta, self._INETimeDelta,
                      self._CZCETimeDelta]
        delta_list = [d for d in delta_list if d.total_seconds() != 0]
        if len(delta_list) > 0:
            self._exch_timedelta = min(delta_list)

    @property
    def FFEXTimeDelta(self):
        return self._FFEXTimeDelta

    @FFEXTimeDelta.setter
    def FFEXTimeDelta(self, delta):
        self._FFEXTimeDelta = delta
        delta_list = [self._FFEXTimeDelta, self._DCETimeDelta, self._SHFETimeDelta, self._INETimeDelta,
                      self._CZCETimeDelta]
        delta_list = [d for d in delta_list if d.total_seconds() != 0]
        if len(delta_list) > 0:
            self._exch_timedelta = min(delta_list)

    @property
    def DCETimeDelta(self):
        return self._DCETimeDelta

    @DCETimeDelta.setter
    def DCETimeDelta(self, delta):
        self._DCETimeDelta = delta
        delta_list = [self._FFEXTimeDelta, self._DCETimeDelta, self._SHFETimeDelta, self._INETimeDelta,
                      self._CZCETimeDelta]
        delta_list = [d for d in delta_list if d.total_seconds() != 0]
        if len(delta_list) > 0:
            self._exch_timedelta = min(delta_list)

    @property
    def INETimeDelta(self):
        return self._INETimeDelta

    @INETimeDelta.setter
    def INETimeDelta(self, delta):
        self._INETimeDelta = delta
        delta_list = [self._FFEXTimeDelta, self._DCETimeDelta, self._SHFETimeDelta, self._INETimeDelta,
                      self._CZCETimeDelta]
        delta_list = [d for d in delta_list if d.total_seconds() != 0]
        if len(delta_list) > 0:
            self._exch_timedelta = min(delta_list)

    @property
    def CZCETimeDelta(self):
        return self._CZCETimeDelta

    @CZCETimeDelta.setter
    def CZCETimeDelta(self, delta):
        self._CZCETimeDelta = delta
        delta_list = [self._FFEXTimeDelta, self._DCETimeDelta, self._SHFETimeDelta, self._INETimeDelta,
                      self._CZCETimeDelta]
        delta_list = [d for d in delta_list if d.total_seconds() != 0]
        if len(delta_list) > 0:
            self._exch_timedelta = min(delta_list)

    @property
    def exch_timedelta(self):
        return self._exch_timedelta

    @property
    def exch_datetime(self):
        """返回交易所服务器时间"""
        return datetime.now() + self._exch_timedelta

    def get_trade_datetime_range_list(self, instrument_id):
        """
        获取合约交易时间段
        :param instrument_id: 
        :return: 
        """
        if re.match('(ic|if|ih)\d{4}', instrument_id) is not None:
            datetime_range = self._moning_thour_4_FFEX_IFICIH.copy()
        elif re.match('(t|tf)\d{4}', instrument_id) is not None:
            datetime_range = self._moning_thour_4_FFEX_T.copy()
        else:
            datetime_range = self._day_thour_4_commodity.copy()
            if re.match('(ru|rb|bu|hc)\d{4}', instrument_id) is not None:
                # 橡胶 螺纹钢 沥青 热轧卷板 21:00-23:00
                [datetime_range.append(dt_range) for dt_range in self._night_thour_4_rb]
            elif re.match('(au|ag)\d{4}', instrument_id) is not None:
                # 黄金 白银 21:00-次日2:30
                [datetime_range.append(dt_range) for dt_range in self._night_thour_4_au_ag]
            elif re.match('(cu|al|zn|pb|ni|sn)\d{4}', instrument_id) is not None:
                # 铜 铝 锌 铅 镍 锡 21:00-次日1:00
                [datetime_range.append(dt_range) for dt_range in self._night_thour_4_cu_al]
            elif re.match('(cf|sr|rm|ma|ta|oi|fg|zc|p|j|jm|i|m|y|a|b)\d{4}', instrument_id) is not None:
                # 棉花 白糖 菜粕 甲醇 PTA 菜油 玻璃 动力煤 21:00-23:30
                # 棕榈油 焦炭 焦煤 铁矿石 豆粕 豆油 豆一 豆二 21:00-23:30
                [datetime_range.append(dt_range) for dt_range in self._night_thour_4_cf_jm]
            else:
                # 上海期货交易所:线材 燃油
                # 郑州商品交易所:菜籽 强麦 普麦 粳稻 硅铁 锰硅 早籼稻 晚籼稻
                # 大连商品交易所:玉米 玉米淀粉 鸡蛋 胶合板 纤维板 塑料  PVC 聚炳烯
                pass
        return datetime_range

    def get_mongo_client(self) -> MongoClient:
        """
        获取mongodb client
        :return: 
        """
        if self._MONGO_CLIENT is None:
            self._MONGO_CLIENT = MongoClient('mongodb://%s:%s/' % (self.MONGO_INFO_DIC['MONGO_DB_URL'],
                                                                   self.MONGO_INFO_DIC['MONGO_DB_PORT']))
        return self._MONGO_CLIENT

    def get_mongo_db(self, db_name=None) -> Database:
        """
        获取 mongodb database
        :param db_name: 
        :return: 
        """
        client = self.get_mongo_client()
        if db_name is None:
            db_name = self.MONGO_INFO_DIC['MONGO_DB_NAME']
        return client[db_name]

    def do_for_mongo_db(self, action, db_name=None):
        """
        对database执行相关操作
        :param action: 
        :param db_name: 
        :return: 
        """
        action(self.get_mongo_db(db_name))

    def get_mongo_collection(self, collection_name, db_name=None) -> Collection:
        """
        获取 mongodb collection
        :param collection_name: 
        :param db_name: 
        :return: 
        """
        database = self.get_mongo_db(db_name)
        return database[collection_name]

    def do_for_mongo_collection(self, action, collection_name, db_name=None):
        """
        对 collection 执行相关操作
        :param action: 
        :param collection_name: 
        :param db_name: 
        :return: 
        """
        action(self.get_mongo_collection(collection_name, db_name))

    def release(self):
        """
        释放资源
        :return: 
        """
        if self._MONGO_CLIENT is not None:
            self._MONGO_CLIENT.close()

    def get_db_engine(self, schema_name='default') -> Engine:
        """初始化数据库engine"""
        if schema_name is None:
            schema_name = 'default'
        if schema_name in self._db_engine_dic:
            return self._db_engine_dic[schema_name]
        else:
            db_info_dic = self.DB_INFO_DIC[schema_name]
            if db_info_dic['type'] == 'mysql':
                engine = create_engine(
                    "mysql+pymysql://%(DB_USER)s:%(DB_PASSWORD)s@%(DB_IP)s:%(DB_PORT)s/%(DB_NAME)s?charset=utf8"
                    % db_info_dic,  # dev_db fh_db
                    echo=False, encoding="utf-8")
                # logger.debug('开始连接DB: %s', db_info_dic['DB_IP'])
            elif db_info_dic['type'] == 'sqlite':
                engine = create_engine(db_info_dic['URL'], echo=False, encoding="utf-8")
            else:
                raise ValueError("DB_INFO_DIC[%s]['type']=%s", schema_name, db_info_dic['type'])
            self._db_engine_dic[schema_name] = engine
            # logger.debug("数据库已连接")
        return engine

    def get_db_session(self, engine=None) -> Session:
        """创建session对象 使用后需注意主动关闭"""
        if engine is None:
            engine = self.get_db_engine()
        db_session = sessionmaker(bind=engine)
        session = db_session()
        return session

    def with_db_session(self, engine=None, schema_name=None, expire_on_commit=True):
        """创建session对象，返回 session_wrapper 可以使用with语句进行调用"""
        if engine is None:
            engine = self.get_db_engine(schema_name)
        db_session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)
        session = db_session()
        return SessionWrapper(session)

    def get_redis(self, db=0) -> StrictRedis:
        """
        get StrictRedis object
        :param db: 
        :return: 
        """
        if db in self._redis_client_dic:
            redis_client = self._redis_client_dic[db]
        else:
            if db in self._redis_connection_pool_dic:
                conn = self._redis_connection_pool_dic[db]
            else:
                conn = ConnectionPool(host=self.REDIS_INFO_DIC['REDIS_HOST'],
                                      port=self.REDIS_INFO_DIC['REDIS_PORT'],
                                      db=db)
                self._redis_connection_pool_dic[db] = conn
            redis_client = StrictRedis(connection_pool=conn)
            self._redis_client_dic[db] = redis_client
        return redis_client

    def get_pub_sub(self, db=0) -> PubSub:
        """
        获取 redis 广播/订阅 对象
        :param db: 
        :return: 
        """
        if db in self._redis_pub_sub_dic:
            pub_sub = self._redis_pub_sub_dic[db]
        else:
            pub_sub = self.get_redis(db).pubsub()
            self._redis_pub_sub_dic[db] = pub_sub
        return pub_sub

    def update_instrument(self, instrument_info_dic):
        """
        插入或更新合约信息列表 Config.instrument_market_dic
        :return: 
        """
        self._instrument_info_dic = instrument_info_dic
        engine = self.get_db_engine(schema_name=Config.DB_SCHEMA_MD)
        instrument_table = Table('instrument', MetaData(engine), autoload=True)

        # 临时定义一个class 用来绑定 instrument_table
        class InstrumentModel(object):
            pass

        mapper(InstrumentModel, instrument_table)
        with self.with_db_session(engine) as session:
            # 循环执行 merge 更新全部 instrument_obj
            try:
                n = -1  # 这里初始化 n = -1 是为了避免，当 available_instrument_info_dic 为空时 log.info 中 n 未定义错误
                for n, (instrument_id, instrument_info_dic) in enumerate(self._instrument_info_dic.items()):
                    instrument_obj = InstrumentModel()
                    for name, val in instrument_info_dic.items():
                        setattr(instrument_obj, name, val)
                    # instrument_obj_list.append(instrument_obj)
                    session.merge(instrument_obj)
                # 提交 commit
                session.commit()  # 稍后提到 for循环外面
                logger.info('%d 条 合约 信息被更新', n + 1)
                self.update_instrument_finished = True
            except:
                logger.exception("%d) %s 合约 merge error:\n%s", n, instrument_id, instrument_info_dic)

    # def load_instrument(self):
    #     engine = self.get_db_engine(schema_name=self.DB_SCHEMA_MD)
    #     try:
    #         instrument_info_df = pd.read_sql('select * from instrument', engine)
    #         self._instrument_info_dic = {record['InstrumentID']: record for record in
    #                                     instrument_info_df.to_dict('record')}
    #     except:
    #         logger.exception("加载合约信息失败")
    #
    # def update_instrument_commission_rate(self, new_added_instrument_id_list):
    #     logger.info('%d 条合约手续费信息将被更新到数据库\n%s', len(new_added_instrument_id_list), new_added_instrument_id_list)
    #     engine = self.get_db_engine(schema_name=Config.DB_SCHEMA_MD)
    #     instrument_commission_rate_table = Table('instrumentcommissionrate', MetaData(engine), autoload=True)
    #     # 临时定义一个class 用来绑定 instrument_table
    #     class CommissionRateModel(object): pass
    #     mapper(CommissionRateModel, instrument_commission_rate_table)
    #     with self.with_db_session(engine) as session:
    #         # 循环执行 merge 更新全部 instrument_obj
    #         try:
    #             n = -1  # 这里初始化 n = -1 是为了避免，当 available_instrument_info_dic 为空时 log.info 中 n 未定义错误
    #             # for n, (instrument_id, commission_rate_dic) in enumerate(Config.instrument_commission_rate_dic.items()):
    #             for n, instrument_id in enumerate(new_added_instrument_id_list):
    #                 commission_rate_dic = Config.instrument_commission_rate_dic[instrument_id]
    #                 commission_rate_obj = CommissionRateModel()
    #                 for name, val in commission_rate_dic.items():
    #                     setattr(commission_rate_obj, name, val)
    #                 # instrument_obj_list.append(instrument_obj)
    #                 session.merge(commission_rate_obj)
    #             # 提交 commit
    #             session.commit()  # 稍后提到 for循环外面
    #             logger.info('%d 条 合约手续费率 信息被更新', n + 1)
    #         except:
    #             logger.exception("%d) %s 合约手续费率 merge error:\n%s", n, instrument_id, instrument_info_dic)
    #
    # def load_instrument_commission_rate(self):
    #     engine = self.get_db_engine(schema_name=self.DB_SCHEMA_MD)
    #     try:
    #         instrument_info_df = pd.read_sql('select * from instrumentcommissionrate', engine)
    #         self.instrument_commission_rate_dic = {record['InstrumentID']: record for record in
    #                                                instrument_info_df.to_dict('record')}
    #     except:
    #         logger.exception("加载合约手续费率信息失败")


class ConfigProduct(ConfigBase):
    # 国投安信
    # api configuration
    BROKER_ID = b'***'
    USER_ID = b'***'
    INVESTOR_ID = b'***'
    PASSWORD = b'***'
    FRONT_TRADER_ADDRESS = b'tcp://0.0.0.0:41205'
    FRONT_MD_ADDRESS = b'tcp://0.0.0.0:41213'

    # redis info
    # REDIS_INFO_DIC = {'REDIS_HOST': '127.0.0.1',
    #                   'REDIS_PORT': '6379',
    #                   }
    REDIS_INFO_DIC = {'REDIS_HOST': '192.168.239.131',
                      'REDIS_PORT': '6379',
                      }
    _redis_client_dic = None

    # celery schedule info
    CELERYBEAT_SCHEDULE = {
        'receive_save_md_moning': {
            'task': 'tasks.save_md',
            'schedule': crontab(minute=55, hour=8, day_of_week='1-5'),
        },
        'receive_save_md_night': {
            'task': 'tasks.save_md',
            'schedule': crontab(minute=55, hour=20, day_of_week='1-5'),
        },
    }

    def __init__(self):
        super().__init__()
        self.MONGO_INFO_DIC['MONGO_DB_NAME'] = 'ctp'
        # celery info
        self.CELERY_INFO_DIC = {
            'TASK_PID_KEY': "task:save_md:pid",
            'CELERY_TIMEZONE': 'Asia/Shanghai',
            'BROKER_URL': 'redis://%(REDIS_HOST)s:%(REDIS_PORT)s/13' % self.REDIS_INFO_DIC,
            'CELERY_RESULT_BACKEND': 'redis://%(REDIS_HOST)s:%(REDIS_PORT)s/14' % self.REDIS_INFO_DIC,
            'REDIS_DB_OTHER': '15',
        }


class ConfigTest(ConfigBase):
    DB_SCHEMA_MD = 'future_md_test'

# 开发配置（SIMNOW MD + Trade）
# Config = ConfigBase()
# 测试配置（测试行情库）
# Config = ConfigTest()
# 生产配置
Config = ConfigProduct()

# 设定默认日志格式
logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)
logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
logging.getLogger('MdCombiner').setLevel(logging.INFO)
logging.getLogger('MdMin1Combiner').setLevel(logging.INFO)
logging.getLogger('EventAgent').setLevel(logging.INFO)


# 配置文件日至
Rthandler = RotatingFileHandler('log.log', maxBytes=10*1024*1024,backupCount=5)
Rthandler.setLevel(logging.INFO)
formatter = logging.Formatter(Config.LOG_FORMAT)
Rthandler.setFormatter(formatter)
logging.getLogger('').addHandler(Rthandler)

# Celery 的调度器的配置是在 CELERYBEAT_SCHEDULE 这个全局变量上配置的，
# 我们可以将配置写在一个独立的 Python 模块，在定义 Celery 对象的时候加载这个模块。
CELERY_RESULT_BACKEND = Config.CELERY_INFO_DIC['CELERY_RESULT_BACKEND']
BROKER_URL = Config.CELERY_INFO_DIC['BROKER_URL']
CELERYBEAT_SCHEDULE = Config.CELERYBEAT_SCHEDULE


def with_mongo_client():
    return ClientWrapper(MongoClient('mongodb://%(MONGO_DB_URL)s:%(MONGO_DB_PORT)s/' % Config.MONGO_INFO_DIC))


def with_mongo_db(action, db_name=None):
    if db_name is None:
        db_name = Config.MONGO_INFO_DIC['MONGO_DB_NAME']
    with with_mongo_client() as client:
        return action(client[db_name])


def with_mongo_collection(action, collectoin_name, db_name=None):
    if db_name is None:
        db_name = Config.MONGO_INFO_DIC['MONGO_DB_NAME']
    with with_mongo_client() as client:
        return action(client[db_name][collectoin_name])


if __name__ == "__main__":
    pass
    # with with_mongo_client() as client:
    #     client[Config.MONGO_INFO_DIC['MONGO_DB_NAME']].create_collection('hello')

    # with_mongo_collection(lambda collection: [print(raw) for raw in collection.find()],
    #                            'hello', db_name='test')
    # client = MongoClient('mongodb://%s:%s/' % (Config.MONGO_DB_URL, Config.MONGO_DB_PORT))  # 'mongodb://mg:***@10.0.3.66:27017/'
    # client = MongoClient('10.0.3.66", "27017')  # 'mongodb://mg:***@10.0.3.66:27017/'
    # collection = client['test']['hello']
    # [print(raw) for raw in collection.find()]
