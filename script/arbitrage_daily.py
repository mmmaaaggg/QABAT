# -*- coding: utf-8 -*-
"""
Created on 2017/11/26
@author: MG
"""
import os
import pandas as pd
from sqlalchemy.dialects.mysql import DOUBLE
import logging
from config import Config
from sqlalchemy.types import String, Date


def import_data_from_folder(folder_path):
    # 获取文件列表
    file_name_list = os.listdir(folder_path)
    if file_name_list is None:
        # self.logger.info('No file')
        return
    # 读取所有csv 文件
    position_df = None
    engine = Config.get_db_engine(schema_name=Config.DB_SCHEMA_MD)
    for file_name in file_name_list:
        file_base_name, file_extension = os.path.splitext(file_name)
        if file_extension.lower() != '.csv':
            continue
        file_path = os.path.join(folder_path, file_name)
        position_df = pd.read_csv(file_path)
        # position_df_tmp = pd.read_csv(file_path)
        # if position_df is None:
        #     position_df = position_df_tmp
        # else:
        #     position_df = position_df.append(position_df_tmp)

        if position_df.shape[0] == 0:
            return
        try:
            position_df.set_index(['Date', 'Contract'], inplace=True)
            position_df.to_sql('arbitrage_md_daily', engine, index_label=['Date', 'Contract'], if_exists='append',
                               dtype={
                                   'Date': Date,
                                   'Contract': String(20),
                                   'ContractNext': String(20),
                                   'Close': DOUBLE,
                                   'CloseNext': DOUBLE,
                                   'TermStructure': DOUBLE,
                                   'Volume': DOUBLE,
                                   'VolumeNext': DOUBLE,
                                   'OI': DOUBLE,
                                   'OINext': DOUBLE,
                                   'Open': DOUBLE,
                                   'OpenNext': DOUBLE,
                                   'High': DOUBLE,
                                   'HighNext': DOUBLE,
                                   'Low': DOUBLE,
                                   'LowNext': DOUBLE,
                                   'WarehouseWarrant': DOUBLE,
                                   'WarehouseWarrantNext': DOUBLE,
                               })
        except:
            logging.exception("%s 导入异常", file_name)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    folder_path = r"d:\Downloads\MarketInfo"
    import_data_from_folder(folder_path)
