# -*- coding: utf-8 -*-
"""
将API struct 相关对象镜像为数据库表
由于表示自动建立的，因此，默认不添加主键
所有表的主键需要手动建立
Created on 2017/7/9
@author: MG
"""
from sqlalchemy import create_engine, MetaData, Column, String, Date, Time, SmallInteger
from sqlalchemy.dialects.mysql import DOUBLE, INTEGER, TINYINT, SMALLINT
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import Table
import logging
from ctp import ApiStruct
from config import Config
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger()

COL_NAME_DB_TYPE_DIC = {
    'InstrumentID': String(30),
    'TradingDay': Date,
    'ExchangeID': String(8),
    'InstrumentName': String(20),
    'ExchangeInstID': String(30),
    'ProductID': String(30),
    'ProductClass': String(20),
    'DeliveryYear': SMALLINT(),
    'DeliveryMonth': TINYINT(),
    'CreateDate': Date,
    'OpenDate': Date,
    'ExpireDate': Date,
    'StartDelivDate': Date,
    'EndDelivDate': Date,
    'InvestorID': String(12),
    'BrokerID': String(10),
}


def get_col_type(col_value, col_name):
    if col_name in COL_NAME_DB_TYPE_DIC:
        col_type = COL_NAME_DB_TYPE_DIC[col_name]
    else:
        val_type = type(col_value)
        if val_type == bytes:
            col_type = String(20)
        elif val_type == float:
            col_type = DOUBLE
        elif val_type == int:
            col_type = INTEGER
        else:
            raise TypeError('unsupported type of %s' % val_type)
    return col_type


def create_table(metadata, base_struct_obj, pk_name_set=None):
    logger.debug(base_struct_obj.__class__.__name__)
    table_name = base_struct_obj.__class__.__name__
    # [print(col_name, getattr( base_struct_obj, col_name)) for col_name, col_type in base_struct_obj._fields_]
    col_attr_list = []
    for col_name, col_value in base_struct_obj._fields_:
        col_attr = Column(col_name,
                          get_col_type(getattr(base_struct_obj, col_name), col_name),
                          primary_key=(False if (pk_name_set is None) or (col_name not in pk_name_set) else True),
                          autoincrement=False
                          )
        col_attr_list.append(col_attr)
    # col_attr_list = (Column(col_name, get_col_type(getattr(base_struct_obj, col_name), col_name)) for col_name, col_value in base_struct_obj._fields_)
    table_obj = Table(table_name, metadata, *col_attr_list)


if __name__ == "__main__":
    engine = Config.get_db_engine(Config.DB_SCHEMA_MD)
    metadata = MetaData()
    create_table(metadata, ApiStruct.InputOrderAction())
    create_table(metadata, ApiStruct.DepthMarketData(), {'InstrumentID', 'TradingDay', 'UpdateTime', 'UpdateMillisec'})
    create_table(metadata, ApiStruct.Instrument(), {'InstrumentID'})
    create_table(metadata, ApiStruct.InstrumentCommissionRate(), {'InstrumentID'})
    metadata.create_all(engine)  # 创建表结构
