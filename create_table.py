# -*- coding: utf-8 -*-
"""
Created on 2017/7/9
@author: MG
"""
from sqlalchemy import create_engine, MetaData, Column, Integer, String, VARBINARY, Float
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import Table
import logging
from ctp import ApiStruct
from config import Config
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
logger = logging.getLogger()


def get_col_type(col_value):
    val_type = type(col_value)
    if val_type == bytes:
        col_type = VARBINARY(20)
    elif val_type == float:
        col_type = Float
    elif val_type == int:
        col_type = Integer
    else:
        raise TypeError('unsupported type of %s' % val_type)
    return col_type


def create_table(metadata, base_struct_obj):
    logger.debug(base_struct_obj.__class__.__name__)
    table_name = base_struct_obj.__class__.__name__
    # [print(col_name, getattr( base_struct_obj, col_name)) for col_name, col_type in base_struct_obj._fields_]
    col_attr = (Column(col_name, get_col_type(getattr(base_struct_obj, col_name))) for col_name, col_value in base_struct_obj._fields_)
    table_obj = Table(table_name, metadata, *col_attr)


if __name__ == "__main__":
    engine = Config.get_db_engine(Config.DB_SCHEMA_MD)
    metadata = MetaData()
    create_table(metadata, ApiStruct.InputOrderAction())
    create_table(metadata, ApiStruct.DepthMarketData())
    metadata.create_all(engine)  # 创建表结构
