# -*- coding: utf-8 -*-
"""
Created on 2018/2/12
@author: MG
"""

import math, os
from collections import OrderedDict
import numpy as np
import pandas as pd
from config import Config, PeriodType
import logging, re
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date, Time, DateTime, Integer

logger = logging.getLogger()
# 对于郑商所部分合约需要进行特殊的名称匹配规则
re_pattern_instrument_type_3num_by_wind_code = re.compile(r"(?<=(SR|CF))\d{3}(?=.CZC)", re.IGNORECASE)
re_pattern_instrument_header_by_wind_code = re.compile(r"\d{3,4}(?=.\w)", re.IGNORECASE)
re_pattern_instrument_type_3num_by_instrument_id = re.compile(r"(?<=(SR|CF))\d{3}$", re.IGNORECASE)
re_pattern_instrument_header_by_instrument_id = re.compile(r"\d{3,4}$", re.IGNORECASE)


def get_instrument_num(instrument_str, by_wind_code=True):
    """
    获取合约的年月数字
    郑商所部分合约命名规则需要区别开来
    例如：白糖SR、棉花CF 
    SR0605.CZC 200605交割
    SR1605.CZC 201605交割
    SR607.CZC 201607交割
    :param instrument_str: 
    :param by_wind_code: 
    :return: 
    """
    if by_wind_code:
        m = re_pattern_instrument_type_3num_by_wind_code.search(instrument_str)
    else:
        m = re_pattern_instrument_type_3num_by_instrument_id.search(instrument_str)
    if m is not None:
        # 郑商所部分合约命名规则需要区别开来，3位数字的需要+1000
        # 例如：白糖SR、棉花CF
        # SR0605.CZC 200605交割
        # SR1605.CZC 201605交割
        # SR607.CZC 201607交割  + 1000
        inst_num = int(m.group())
        inst_num = inst_num + 1000
    else:
        if by_wind_code:
            m = re_pattern_instrument_header_by_wind_code.search(instrument_str)
        else:
            m = re_pattern_instrument_header_by_instrument_id.search(instrument_str)
        if m is None:
            raise ValueError('%s 不是有效的合约' % instrument_str)
        else:
            # RU9507.SHF 199507 交割
            # RU0001.SHF 200001 交割
            # RU1811.SHF 201811 交割
            inst_num = int(m.group())
            inst_num = inst_num if inst_num < 9000 else inst_num - 10000

    return inst_num


def is_earlier_instruments(inst_a, inst_b, by_wind_code=True):
    """
    比较两个合约交割日期 True
    :param inst_a: 
    :param inst_b: 
    :param by_wind_code: 
    :return: 
    """
    inst_num_a = get_instrument_num(inst_a, by_wind_code)
    inst_num_b = get_instrument_num(inst_b, by_wind_code)
    return inst_num_a < inst_num_b


def data_reorg_daily(instrument_type) -> pd.DataFrame:
    """
    将每一个交易日主次合约合约行情信息进行展示
    :param instrument_type: 
    :return: 
    """
    # engine = Config.get_db_engine(Config.DB_SCHEMA_PROD)
    engine = Config.get_db_engine(Config.DB_SCHEMA_DEFAULT)
    sql_str = r"select wind_code, trade_date, open, high, low, close, volume, position, st_stock from wind_future_daily where wind_code regexp %s"
    data_df = pd.read_sql(sql_str, engine, params=[r'^%s[0-9]+\.[A-Z]+' % (instrument_type)])
    date_instrument_vol_df = data_df.pivot(index="trade_date", columns="wind_code", values="volume").sort_index()
    date_instrument_open_df = data_df.pivot(index="trade_date", columns="wind_code", values="open")
    date_instrument_low_df = data_df.pivot(index="trade_date", columns="wind_code", values="low")
    date_instrument_high_df = data_df.pivot(index="trade_date", columns="wind_code", values="high")
    date_instrument_close_df = data_df.pivot(index="trade_date", columns="wind_code", values="close")
    date_instrument_position_df = data_df.pivot(index="trade_date", columns="wind_code", values="position")
    date_instrument_st_stock_df = data_df.pivot(index="trade_date", columns="wind_code", values="st_stock")
    logger.info("查询 %s 包含 %d 条记录 %d 个合约", instrument_type, *date_instrument_vol_df.shape)

    instrument_id_main, instrument_id_secondary = None, None
    date_instrument_id_dic = {}
    # 按合约号排序
    instrument_id_list_sorted = list(date_instrument_vol_df.columns)
    instrument_id_list_sorted.sort(key=get_instrument_num)
    date_instrument_vol_df = date_instrument_vol_df[instrument_id_list_sorted]
    date_reorg_data_dic = {}
    trade_date_error = None
    # 逐日检查主力合约，次主力合约列表
    # 主要逻辑：
    # 每天检查次一日的主力合约，次主力合约
    # 主力合约为当日成交量最大合约，合约号只能前进不能后退
    # 次主力合约的交割日期要大于主力合约的交割日期，
    trade_date_list = list(date_instrument_vol_df.index)
    trade_date_available_list = []
    for nday, trade_date in enumerate(trade_date_list):
        if instrument_id_main is not None:
            date_instrument_id_dic[trade_date] = (instrument_id_main, instrument_id_secondary)
        vol_s = date_instrument_vol_df.loc[trade_date].dropna()

        instrument_id_main_cur, instrument_id_secondary_cur = instrument_id_main, instrument_id_secondary
        if vol_s.shape[0] == 0:
            logger.warning("%s 没有交易量数据", trade_date)
            continue
        # 当日没有交易量数据，或者主力合约已经过期
        if instrument_id_main not in vol_s.index:
            instrument_id_main = None
        for try_time in [1, 2]:
            for instrument_id in vol_s.index:
                if instrument_id_main is not None and is_earlier_instruments(instrument_id, instrument_id_main):
                    continue
                if instrument_id_main is None and instrument_id_main_cur is not None and is_earlier_instruments(
                        instrument_id, instrument_id_main_cur):
                    continue
                # if math.isnan(vol_s[instrument_id]):
                #     continue
                # 比较出主力合约 和次主力合约
                if instrument_id_main is None:
                    instrument_id_main = instrument_id
                elif instrument_id_main in vol_s and vol_s[instrument_id_main] < vol_s[instrument_id]:
                    instrument_id_main, instrument_id_secondary = instrument_id, None
                elif instrument_id_secondary is None:
                    if instrument_id_main != instrument_id:
                        instrument_id_secondary = instrument_id
                elif is_earlier_instruments(instrument_id_secondary, instrument_id) \
                        and instrument_id_secondary in vol_s and vol_s[instrument_id_secondary] < vol_s[instrument_id]:
                    instrument_id_secondary = instrument_id
            if instrument_id_main is None:
                logger.warning("%s 主力合约%s缺少成交量数据，其他合约没有可替代数据，因此继续使用当前行情数据",
                               trade_date, instrument_id_main_cur)
                instrument_id_main = instrument_id_main_cur
            else:
                break

        # 异常处理
        if instrument_id_secondary is None:
            if trade_date_error is None:
                trade_date_error = trade_date
                logger.warning("%s 当日主力合约 %s, 没有次主力合约", trade_date, instrument_id_main)
        elif trade_date_error is not None:
            logger.warning("%s 当日主力合约 %s, 次主力合约 %s", trade_date, instrument_id_main, instrument_id_secondary)
            trade_date_error = None

        if nday > 0:
            # 如果主力合约切换，则计算调整因子
            if instrument_id_main_cur is not None and instrument_id_main_cur != instrument_id_main:
                trade_date_last = trade_date_available_list[-1]
                close_last_main_cur = date_instrument_close_df[instrument_id_main_cur][trade_date_last]
                close_last_main = date_instrument_close_df[instrument_id_main][trade_date_last]
                adj_chg_main = close_last_main / close_last_main_cur
                if trade_date_last in date_reorg_data_dic:
                    date_reorg_data_dic[trade_date_last]['adj_chg_main'] = adj_chg_main
            # 如果次主力合约切换，则计算调整因子
            if instrument_id_secondary_cur is not None and instrument_id_secondary_cur != instrument_id_secondary:
                if instrument_id_secondary is not None:
                    trade_date_last = trade_date_available_list[-1]
                    close_last_secondary_cur = date_instrument_close_df[instrument_id_secondary_cur][trade_date_last]
                    close_last_secondary = date_instrument_close_df[instrument_id_secondary][trade_date_last]
                    adj_chg_secondary = close_last_secondary / close_last_secondary_cur
                    if trade_date_last in date_reorg_data_dic:
                        date_reorg_data_dic[trade_date_last]['adj_chg_secondary'] = adj_chg_secondary

            # 数据重组
            close_main = date_instrument_close_df[instrument_id_main][trade_date]
            close_last_day_main = date_instrument_close_df[instrument_id_main][trade_date_list[nday - 1]]
            close_secondary = date_instrument_close_df[instrument_id_secondary][
                trade_date] if instrument_id_secondary is not None else math.nan
            if math.isnan(close_secondary) and math.isnan(close_main):
                logger.warning("%s 主力合约 %s 次主力合约 %s 均无收盘价数据",
                               trade_date, instrument_id_main, instrument_id_secondary)
                continue
            date_reorg_data_dic[trade_date] = {
                "Date": trade_date,
                "Contract": instrument_id_main,
                "ContractNext": instrument_id_secondary if instrument_id_secondary is not None else None,
                "Close": close_main,
                "CloseNext": close_secondary,
                "TermStructure": math.nan if math.isnan(close_last_day_main) else (
                    (close_main - close_last_day_main) / close_last_day_main),
                "Volume": date_instrument_vol_df[instrument_id_main][trade_date],
                "VolumeNext": date_instrument_vol_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "OI": date_instrument_position_df[instrument_id_main][trade_date],
                "OINext": date_instrument_position_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "Open": date_instrument_open_df[instrument_id_main][trade_date],
                "OpenNext": date_instrument_open_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "High": date_instrument_high_df[instrument_id_main][trade_date],
                "HighNext": date_instrument_high_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "Low": date_instrument_low_df[instrument_id_main][trade_date],
                "LowNext": date_instrument_low_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "WarehouseWarrant": date_instrument_st_stock_df[instrument_id_main][trade_date],
                "WarehouseWarrantNext": date_instrument_st_stock_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "adj_chg_main": 1,
                "adj_chg_secondary": 1,
            }
        trade_date_available_list.append(trade_date)
    # 建立DataFrame
    if len(date_reorg_data_dic) == 0:
        date_noadj_df, date_adj_df = None, None
    else:
        date_reorg_data_df = pd.DataFrame(date_reorg_data_dic).T
        col_list = ["Date", "Contract", "ContractNext", "Close", "CloseNext", "TermStructure",
                    "Volume", "VolumeNext", "OI", "OINext", "Open", "OpenNext", "High", "HighNext",
                    "Low", "LowNext", "WarehouseWarrant", "WarehouseWarrantNext"]
        # 无复权价格
        date_noadj_df = date_reorg_data_df[col_list].copy()
        # 前复权价格
        date_reorg_data_df['adj_factor_main'] = date_reorg_data_df.sort_index(ascending=False)['adj_chg_main'].cumprod()
        date_reorg_data_df['Open'] = date_reorg_data_df['Open'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['High'] = date_reorg_data_df['High'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['Low'] = date_reorg_data_df['Low'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['Close'] = date_reorg_data_df['Close'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['adj_factor_secondary'] = date_reorg_data_df.sort_index(ascending=False)[
            'adj_chg_secondary'].cumprod()
        date_reorg_data_df['OpenNext'] = date_reorg_data_df['OpenNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['HighNext'] = date_reorg_data_df['HighNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['LowNext'] = date_reorg_data_df['LowNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['CloseNext'] = date_reorg_data_df['CloseNext'] * date_reorg_data_df['adj_factor_secondary']
        col_list = ["Date", "Contract", "ContractNext", "Close", "CloseNext", "TermStructure",
                    "Volume", "VolumeNext", "OI", "OINext", "Open", "OpenNext", "High", "HighNext",
                    "Low", "LowNext", "WarehouseWarrant", "WarehouseWarrantNext",
                    "adj_factor_main", "adj_factor_secondary"]
        date_adj_df = date_reorg_data_df[col_list].copy()

        # date_adj_df.reset_index(inplace=True)
        date_adj_df.rename(columns={
            "Date": "trade_date"
        }, inplace=True)
        # 为了解决 AttributeError: 'numpy.float64' object has no attribute 'translate' 错误，需要将数据类型转换成 float
        date_adj_df["Close"] = date_adj_df["Close"].apply(float)
        date_adj_df["CloseNext"] = date_adj_df["CloseNext"].apply(float)
        date_adj_df["TermStructure"] = date_adj_df["TermStructure"].apply(float)
        date_adj_df["Volume"] = date_adj_df["Volume"].apply(float)
        date_adj_df["VolumeNext"] = date_adj_df["VolumeNext"].apply(float)
        date_adj_df["OI"] = date_adj_df["OI"].apply(float)
        date_adj_df["OINext"] = date_adj_df["OINext"].apply(float)
        date_adj_df["Open"] = date_adj_df["Open"].apply(float)
        date_adj_df["OpenNext"] = date_adj_df["OpenNext"].apply(float)
        date_adj_df["High"] = date_adj_df["High"].apply(float)
        date_adj_df["HighNext"] = date_adj_df["HighNext"].apply(float)
        date_adj_df["Low"] = date_adj_df["Low"].apply(float)
        date_adj_df["LowNext"] = date_adj_df["LowNext"].apply(float)
        date_adj_df["WarehouseWarrant"] = date_adj_df["WarehouseWarrant"].apply(float)
        date_adj_df["WarehouseWarrantNext"] = date_adj_df["WarehouseWarrantNext"].apply(float)
        date_adj_df["adj_factor_main"] = date_adj_df["adj_factor_main"].apply(float)
        date_adj_df["adj_factor_secondary"] = date_adj_df["adj_factor_secondary"].apply(float)
        date_adj_df['instrument_type'] = instrument_type

        table_name = 'reorg_md_daily'
        engine_md = Config.get_db_engine(Config.DB_SCHEMA_MD)
        # 清理历史记录
        with Config.with_db_session(engine_md) as session:
            sql_str = """SELECT table_name FROM information_schema.TABLES WHERE table_name = :table_name and TABLE_SCHEMA=(select database())"""
            is_existed = session.execute(sql_str, params={"table_name": table_name}).fetchone()
            if is_existed is not None:
                session.execute("delete from %s where instrument_type = :instrument_type" % table_name,
                                params={"instrument_type": instrument_type})
                logger.debug("删除%s 中的 %s 历史数据", table_name, instrument_type)
        # 插入数据库
        pd.DataFrame.to_sql(date_adj_df, table_name, engine_md, if_exists='append', index=False, dtype={
            'trade_date': Date,
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
            'adj_factor_main': DOUBLE,
            'adj_factor_secondary': DOUBLE,
            'instrument_type': String(20),
        })

    return date_noadj_df, date_adj_df


def data_reorg_half_daily(instrument_type) -> pd.DataFrame:
    """
    将每一个交易日主次合约合约行情信息进行展示
    :param instrument_type: 
    :return: 
    """
    # engine = Config.get_db_engine(Config.DB_SCHEMA_PROD)
    engine = Config.get_db_engine(Config.DB_SCHEMA_DEFAULT)
    sql_str = r"select lower(instrument_id) instrument_id, trade_date, close, volume, st_stock from wind_future_daily where wind_code regexp %s"
    data_df = pd.read_sql(sql_str, engine, params=[r'^%s[0-9]+\.[A-Z]+' % (instrument_type)])
    date_instrument_vol_df = data_df.pivot(index="trade_date", columns="instrument_id", values="volume").sort_index()
    date_instrument_close_df = data_df.pivot(index="trade_date", columns="instrument_id", values="close")
    date_instrument_st_stock_df = data_df.pivot(index="trade_date", columns="instrument_id", values="st_stock")
    logger.info("查询 %s 包含 %d 条记录 %d 个合约", instrument_type, *date_instrument_vol_df.shape)

    instrument_id_main, instrument_id_secondary = None, None
    date_instrument_id_dic = {}
    # 按合约号排序
    instrument_id_list_sorted = list(date_instrument_vol_df.columns)
    instrument_id_list_sorted.sort(key=lambda x: get_instrument_num(x, by_wind_code=False))
    date_instrument_vol_df = date_instrument_vol_df[instrument_id_list_sorted]
    date_reorg_data_dic = OrderedDict()
    trade_date_error = None
    # 逐日检查主力合约，次主力合约列表
    # 主要逻辑：
    # 每天检查次一日的主力合约，次主力合约
    # 主力合约为当日成交量最大合约，合约号只能前进不能后退
    # 次主力合约的交割日期要大于主力合约的交割日期，
    trade_date_list = list(date_instrument_vol_df.index)
    trade_date_available_list = []
    for nday, trade_date in enumerate(trade_date_list):
        if instrument_id_main is not None:
            date_instrument_id_dic[trade_date] = (instrument_id_main, instrument_id_secondary)
        vol_s = date_instrument_vol_df.loc[trade_date].dropna()

        instrument_id_main_cur, instrument_id_secondary_cur = instrument_id_main, instrument_id_secondary
        if vol_s.shape[0] == 0:
            logger.warning("%s 没有交易量数据", trade_date)
            continue
        # 当日没有交易量数据，或者主力合约已经过期
        if instrument_id_main not in vol_s.index:
            instrument_id_main = None
        for try_time in [1, 2]:
            for instrument_id in vol_s.index:
                if instrument_id_main is not None and is_earlier_instruments(instrument_id, instrument_id_main,
                                                                             by_wind_code=False):
                    continue
                if instrument_id_main is None and instrument_id_main_cur is not None and is_earlier_instruments(
                        instrument_id, instrument_id_main_cur, by_wind_code=False):
                    continue
                # if math.isnan(vol_s[instrument_id]):
                #     continue
                # 比较出主力合约 和次主力合约
                if instrument_id_main is None:
                    instrument_id_main = instrument_id
                elif instrument_id_main in vol_s and vol_s[instrument_id_main] < vol_s[instrument_id]:
                    instrument_id_main, instrument_id_secondary = instrument_id, None
                elif instrument_id_secondary is None:
                    if instrument_id_main != instrument_id:
                        instrument_id_secondary = instrument_id
                elif is_earlier_instruments(instrument_id_secondary, instrument_id, by_wind_code=False) \
                        and instrument_id_secondary in vol_s and vol_s[instrument_id_secondary] < vol_s[instrument_id]:
                    instrument_id_secondary = instrument_id
            if instrument_id_main is None:
                logger.warning("%s 主力合约%s缺少成交量数据，其他合约没有可替代数据，因此继续使用当前行情数据",
                               trade_date, instrument_id_main_cur)
                instrument_id_main = instrument_id_main_cur
            else:
                break

        # 异常处理
        if instrument_id_secondary is None:
            if trade_date_error is None:
                trade_date_error = trade_date
                logger.warning("%s 当日主力合约 %s, 没有次主力合约", trade_date, instrument_id_main)
        elif trade_date_error is not None:
            logger.warning("%s 当日主力合约 %s, 次主力合约 %s", trade_date, instrument_id_main, instrument_id_secondary)
            trade_date_error = None

        if nday > 0:
            # 如果主力合约切换，则计算调整因子
            if instrument_id_main_cur is not None and instrument_id_main_cur != instrument_id_main:
                trade_date_last = trade_date_available_list[-1]
                close_last_main_cur = date_instrument_close_df[instrument_id_main_cur][trade_date_last]
                close_last_main = date_instrument_close_df[instrument_id_main][trade_date_last]
                adj_chg_main = close_last_main / close_last_main_cur
                if trade_date_last in date_reorg_data_dic:
                    date_reorg_data_dic[trade_date_last]['adj_chg_main'] = adj_chg_main
            # 如果次主力合约切换，则计算调整因子
            if instrument_id_secondary_cur is not None and instrument_id_secondary_cur != instrument_id_secondary:
                if instrument_id_secondary is not None:
                    trade_date_last = trade_date_available_list[-1]
                    close_last_secondary_cur = date_instrument_close_df[instrument_id_secondary_cur][trade_date_last]
                    close_last_secondary = date_instrument_close_df[instrument_id_secondary][trade_date_last]
                    adj_chg_secondary = close_last_secondary / close_last_secondary_cur
                    if trade_date_last in date_reorg_data_dic:
                        date_reorg_data_dic[trade_date_last]['adj_chg_secondary'] = adj_chg_secondary

            # 数据重组
            close_main = date_instrument_close_df[instrument_id_main][trade_date]
            close_last_day_main = date_instrument_close_df[instrument_id_main][trade_date_list[nday - 1]]
            close_secondary = date_instrument_close_df[instrument_id_secondary][
                trade_date] if instrument_id_secondary is not None else math.nan
            if math.isnan(close_secondary) and math.isnan(close_main):
                logger.warning("%s 主力合约 %s 次主力合约 %s 均无收盘价数据",
                               trade_date, instrument_id_main, instrument_id_secondary)
                continue
            date_reorg_data_dic[trade_date] = {
                "Date": trade_date,
                "Contract": instrument_id_main,
                "ContractNext": instrument_id_secondary if instrument_id_secondary is not None else None,
                "TermStructure": math.nan if math.isnan(close_last_day_main) else (
                    (close_main - close_last_day_main) / close_last_day_main),
                "Volume": date_instrument_vol_df[instrument_id_main][trade_date],
                "VolumeNext": date_instrument_vol_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "WarehouseWarrant": date_instrument_st_stock_df[instrument_id_main][trade_date],
                "WarehouseWarrantNext": date_instrument_st_stock_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "adj_chg_main": 1,
                "adj_chg_secondary": 1,
            }
        trade_date_available_list.append(trade_date)

    engine_md = Config.get_db_engine(Config.DB_SCHEMA_MD)
    sql_str = r"""select lower(InstrumentID) instrument_id, TradingDay trade_date, 
if('8:30:00' < UpdateTime and UpdateTime < '12:00:00',1, if('1:00:00' < UpdateTime and UpdateTime < '15:30:00', 2, 3)) half_day, 
Open, High, Low, Close, OpenInterest OI, vol Volume 
from md_half_day where InstrumentID regexp %s and UpdateTime BETWEEN '8:30:00' AND '15:30:00'"""
    data_df = pd.read_sql(sql_str, engine_md, params=['^%s[0-9]+' % instrument_type])
    data_df_dic = {instrument_id: data_sub_df.set_index("trade_date") for instrument_id, data_sub_df in
                   data_df.groupby("instrument_id")}

    # 建立DataFrame
    if len(date_reorg_data_dic) == 0:
        date_noadj_df, date_adj_df = None, None
        return date_noadj_df, date_adj_df
    else:
        merge_df_list = []
        for trade_date, info_dic in date_reorg_data_dic.items():
            # 获取每天主力，次主力合约
            instrument_id_main = info_dic["Contract"]
            if instrument_id_main not in data_df_dic:
                logger.info("%s 没有主力合约%s半日数据，合约不存在", trade_date, instrument_id_main)
                continue
            data_sub_df_main = data_df_dic[instrument_id_main]
            if trade_date not in data_sub_df_main.index:
                logger.warning("%s 没有主力合约%s半日数据", trade_date, instrument_id_main)
                continue
            data_sub_df_main_cur_day = data_sub_df_main.loc[[trade_date]]
            data_sub_df_main_cur_day["WarehouseWarrant"] = info_dic["WarehouseWarrant"]
            data_sub_df_main_cur_day.rename(columns={
                "instrument_id": "Contract",
            }, inplace=True)

            # 获取每天次主力合约
            data_sub_df_secondary_cur_day = None
            instrument_id_secondary = info_dic["ContractNext"]
            if instrument_id_secondary is None:
                data_sub_df_secondary = None
            elif instrument_id_secondary not in data_df_dic:
                data_sub_df_secondary = None
                logger.info("%s 没有次主力合约%s半日数据，合约不存在", trade_date, instrument_id_main)
            else:
                data_sub_df_secondary = data_df_dic[instrument_id_secondary]
                if trade_date not in data_sub_df_secondary.index:
                    logger.warning("%s 没有次主力合约%s半日数据", trade_date, instrument_id_secondary)
                    data_sub_df_secondary_cur_day = None
                else:
                    data_sub_df_secondary_cur_day = data_sub_df_secondary.loc[[trade_date]]
                    data_sub_df_secondary_cur_day.rename(columns={
                        "instrument_id": "ContractNext",
                        "Open": "OpenNext",
                        "High": "HighNext",
                        "Low": "LowNext",
                        "Close": "CloseNext",
                        "OI": "OINext",
                        "Volume": "VolumeNext",
                    }, inplace=True)
                    data_sub_df_secondary_cur_day["WarehouseWarrantNext"] = info_dic["WarehouseWarrantNext"]

            # 合并主力、次主力合约数据
            if data_sub_df_secondary_cur_day is not None:
                merge_df = pd.merge(data_sub_df_main_cur_day, data_sub_df_secondary_cur_day, how="outer", on="half_day")
            else:
                merge_df = data_sub_df_main_cur_day
            if merge_df.shape[0] == 0:
                continue

            # 设置 Date "TermStructure"
            merge_df.loc[:, "Contract"] = instrument_id_main
            merge_df.loc[:, "ContractNext"] = instrument_id_secondary
            merge_df.loc[:, "Date"] = trade_date
            merge_df.loc[:, "TermStructure"] = info_dic["TermStructure"]
            # 设置 adj_chg_main
            adj_chg_main = info_dic["adj_chg_main"]
            vals = np.ones(merge_df.shape[0])
            vals[0] = adj_chg_main
            merge_df.loc[:, "adj_chg_main"] = vals

            # 设置 adj_chg_secondary
            adj_chg_secondary = info_dic["adj_chg_secondary"]
            vals = np.ones(merge_df.shape[0])
            vals[0] = adj_chg_secondary
            merge_df.loc[:, "adj_chg_secondary"] = vals
            # 加入列表
            merge_df_list.append(merge_df)

        date_reorg_data_df = pd.concat(merge_df_list)
        date_reorg_data_df["trade_date"] = date_reorg_data_df["Date"]
        date_reorg_data_df["half_day_idx"] = date_reorg_data_df["half_day"]
        date_reorg_data_df = date_reorg_data_df.set_index(["trade_date", "half_day_idx"])
        col_list = ["Date", "half_day", "Contract", "ContractNext", "Close", "CloseNext", "TermStructure",
                    "Volume", "VolumeNext", "OI", "OINext", "Open", "OpenNext", "High", "HighNext",
                    "Low", "LowNext", "WarehouseWarrant", "WarehouseWarrantNext"]
        # 无复权价格
        date_noadj_df = date_reorg_data_df[col_list].copy()
        # 前复权价格
        date_reorg_data_df['adj_factor_main'] = date_reorg_data_df.sort_index(ascending=False)['adj_chg_main'].cumprod()
        date_reorg_data_df['Open'] = date_reorg_data_df['Open'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['High'] = date_reorg_data_df['High'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['Low'] = date_reorg_data_df['Low'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['Close'] = date_reorg_data_df['Close'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['adj_factor_secondary'] = date_reorg_data_df.sort_index(ascending=False)[
            'adj_chg_secondary'].cumprod()
        date_reorg_data_df['OpenNext'] = date_reorg_data_df['OpenNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['HighNext'] = date_reorg_data_df['HighNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['LowNext'] = date_reorg_data_df['LowNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['CloseNext'] = date_reorg_data_df['CloseNext'] * date_reorg_data_df['adj_factor_secondary']
        col_list = ["Date", "half_day", "Contract", "ContractNext", "Close", "CloseNext", "TermStructure",
                    "Volume", "VolumeNext", "OI", "OINext", "Open", "OpenNext", "High", "HighNext",
                    "Low", "LowNext", "WarehouseWarrant", "WarehouseWarrantNext",
                    "adj_factor_main", "adj_factor_secondary"]
        date_adj_df = date_reorg_data_df[col_list].copy()

    # date_adj_df.reset_index(inplace=True)
    date_adj_df.rename(columns={
        "Date": "trade_date"
    }, inplace=True)
    # 为了解决 AttributeError: 'numpy.float64' object has no attribute 'translate' 错误，需要将数据类型转换成 float
    date_adj_df["Close"] = date_adj_df["Close"].apply(float)
    date_adj_df["CloseNext"] = date_adj_df["CloseNext"].apply(float)
    date_adj_df["TermStructure"] = date_adj_df["TermStructure"].apply(float)
    date_adj_df["Volume"] = date_adj_df["Volume"].apply(float)
    date_adj_df["VolumeNext"] = date_adj_df["VolumeNext"].apply(float)
    date_adj_df["OI"] = date_adj_df["OI"].apply(float)
    date_adj_df["OINext"] = date_adj_df["OINext"].apply(float)
    date_adj_df["Open"] = date_adj_df["Open"].apply(float)
    date_adj_df["OpenNext"] = date_adj_df["OpenNext"].apply(float)
    date_adj_df["High"] = date_adj_df["High"].apply(float)
    date_adj_df["HighNext"] = date_adj_df["HighNext"].apply(float)
    date_adj_df["Low"] = date_adj_df["Low"].apply(float)
    date_adj_df["LowNext"] = date_adj_df["LowNext"].apply(float)
    date_adj_df["WarehouseWarrant"] = date_adj_df["WarehouseWarrant"].apply(float)
    date_adj_df["WarehouseWarrantNext"] = date_adj_df["WarehouseWarrantNext"].apply(float)
    date_adj_df["adj_factor_main"] = date_adj_df["adj_factor_main"].apply(float)
    date_adj_df["adj_factor_secondary"] = date_adj_df["adj_factor_secondary"].apply(float)
    date_adj_df['instrument_type'] = instrument_type

    table_name = 'reorg_md_half_day'
    engine_md = Config.get_db_engine(Config.DB_SCHEMA_MD)
    # 清理历史记录
    with Config.with_db_session(engine_md) as session:
        sql_str = """SELECT table_name FROM information_schema.TABLES WHERE table_name = :table_name and TABLE_SCHEMA=(select database())"""
        is_existed = session.execute(sql_str, params={"table_name": table_name}).fetchone()
        if is_existed is not None:
            session.execute("delete from %s where instrument_type = :instrument_type" % table_name,
                            params={"instrument_type": instrument_type})
            logger.debug("删除%s 中的 %s 历史数据", table_name, instrument_type)
    # 插入数据库
    pd.DataFrame.to_sql(date_adj_df, table_name, engine_md, if_exists='append', index=False, dtype={
        'trade_date': Date,
        'Contract': String(20),
        'ContractNext': String(20),
        'half_day': Integer,
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
        'adj_factor_main': DOUBLE,
        'adj_factor_secondary': DOUBLE,
        'instrument_type': String(20),
    })
    return date_noadj_df, date_adj_df


def data_reorg_min_n(instrument_type, period_type: PeriodType) -> pd.DataFrame:
    """
    将每一个交易日主次合约合约行情信息进行展示
    :param instrument_type: 
    :return: 
    """
    engine = Config.get_db_engine(Config.DB_SCHEMA_DEFAULT)
    sql_str = r"select lower(instrument_id) instrument_id, trade_date, close, volume, st_stock from wind_future_daily where wind_code regexp %s"
    data_df = pd.read_sql(sql_str, engine, params=[r'^%s[0-9]+\.[A-Z]+' % (instrument_type)])
    date_instrument_vol_df = data_df.pivot(index="trade_date", columns="instrument_id", values="volume").sort_index()
    date_instrument_close_df = data_df.pivot(index="trade_date", columns="instrument_id", values="close")
    date_instrument_st_stock_df = data_df.pivot(index="trade_date", columns="instrument_id", values="st_stock")
    logger.info("查询 %s 包含 %d 条记录 %d 个合约", instrument_type, *date_instrument_vol_df.shape)

    instrument_id_main, instrument_id_secondary = None, None
    date_instrument_id_dic = {}
    # 按合约号排序
    instrument_id_list_sorted = list(date_instrument_vol_df.columns)
    instrument_id_list_sorted.sort(key=lambda x: get_instrument_num(x, by_wind_code=False))
    date_instrument_vol_df = date_instrument_vol_df[instrument_id_list_sorted]
    date_reorg_data_dic = OrderedDict()
    trade_date_error = None
    # 逐日检查主力合约，次主力合约列表
    # 主要逻辑：
    # 每天检查次一日的主力合约，次主力合约
    # 主力合约为当日成交量最大合约，合约号只能前进不能后退
    # 次主力合约的交割日期要大于主力合约的交割日期，
    trade_date_list = list(date_instrument_vol_df.index)
    trade_date_available_list = []
    for nday, trade_date in enumerate(trade_date_list):
        if instrument_id_main is not None:
            date_instrument_id_dic[trade_date] = (instrument_id_main, instrument_id_secondary)
        vol_s = date_instrument_vol_df.loc[trade_date].dropna()

        instrument_id_main_cur, instrument_id_secondary_cur = instrument_id_main, instrument_id_secondary
        if vol_s.shape[0] == 0:
            logger.warning("%s 没有交易量数据", trade_date)
            continue
        # 当日没有交易量数据，或者主力合约已经过期
        if instrument_id_main not in vol_s.index:
            instrument_id_main = None
        for try_time in [1, 2]:
            for instrument_id in vol_s.index:
                if instrument_id_main is not None and is_earlier_instruments(instrument_id, instrument_id_main,
                                                                             by_wind_code=False):
                    continue
                if instrument_id_main is None and instrument_id_main_cur is not None and is_earlier_instruments(
                        instrument_id, instrument_id_main_cur, by_wind_code=False):
                    continue
                # if math.isnan(vol_s[instrument_id]):
                #     continue
                # 比较出主力合约 和次主力合约
                if instrument_id_main is None:
                    instrument_id_main = instrument_id
                elif instrument_id_main in vol_s and vol_s[instrument_id_main] < vol_s[instrument_id]:
                    instrument_id_main, instrument_id_secondary = instrument_id, None
                elif instrument_id_secondary is None:
                    if instrument_id_main != instrument_id:
                        instrument_id_secondary = instrument_id
                elif is_earlier_instruments(instrument_id_secondary, instrument_id, by_wind_code=False) \
                        and instrument_id_secondary in vol_s and vol_s[instrument_id_secondary] < vol_s[instrument_id]:
                    instrument_id_secondary = instrument_id
            if instrument_id_main is None:
                logger.warning("%s 主力合约%s缺少成交量数据，其他合约没有可替代数据，因此继续使用当前行情数据",
                               trade_date, instrument_id_main_cur)
                instrument_id_main = instrument_id_main_cur
            else:
                break

        # 异常处理
        if instrument_id_secondary is None:
            if trade_date_error is None:
                trade_date_error = trade_date
                logger.warning("%s 当日主力合约 %s, 没有次主力合约", trade_date, instrument_id_main)
        elif trade_date_error is not None:
            logger.warning("%s 当日主力合约 %s, 次主力合约 %s", trade_date, instrument_id_main, instrument_id_secondary)
            trade_date_error = None

        if nday > 0:
            # 如果主力合约切换，则计算调整因子
            if instrument_id_main_cur is not None and instrument_id_main_cur != instrument_id_main:
                trade_date_last = trade_date_available_list[-1]
                close_last_main_cur = date_instrument_close_df[instrument_id_main_cur][trade_date_last]
                close_last_main = date_instrument_close_df[instrument_id_main][trade_date_last]
                adj_chg_main = close_last_main / close_last_main_cur
                if trade_date_last in date_reorg_data_dic:
                    date_reorg_data_dic[trade_date_last]['adj_chg_main'] = adj_chg_main
            # 如果次主力合约切换，则计算调整因子
            if instrument_id_secondary_cur is not None and instrument_id_secondary_cur != instrument_id_secondary:
                if instrument_id_secondary is not None:
                    trade_date_last = trade_date_available_list[-1]
                    close_last_secondary_cur = date_instrument_close_df[instrument_id_secondary_cur][trade_date_last]
                    close_last_secondary = date_instrument_close_df[instrument_id_secondary][trade_date_last]
                    adj_chg_secondary = close_last_secondary / close_last_secondary_cur
                    if trade_date_last in date_reorg_data_dic:
                        date_reorg_data_dic[trade_date_last]['adj_chg_secondary'] = adj_chg_secondary

            # 数据重组
            close_main = date_instrument_close_df[instrument_id_main][trade_date]
            close_last_day_main = date_instrument_close_df[instrument_id_main][trade_date_list[nday - 1]]
            close_secondary = date_instrument_close_df[instrument_id_secondary][
                trade_date] if instrument_id_secondary is not None else math.nan
            if math.isnan(close_secondary) and math.isnan(close_main):
                logger.warning("%s 主力合约 %s 次主力合约 %s 均无收盘价数据",
                               trade_date, instrument_id_main, instrument_id_secondary)
                continue
            date_reorg_data_dic[trade_date] = {
                "Date": trade_date,
                "Contract": instrument_id_main,
                "ContractNext": instrument_id_secondary if instrument_id_secondary is not None else None,
                "TermStructure": math.nan if math.isnan(close_last_day_main) else (
                    (close_main - close_last_day_main) / close_last_day_main),
                "Volume": date_instrument_vol_df[instrument_id_main][trade_date],
                "VolumeNext": date_instrument_vol_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "WarehouseWarrant": date_instrument_st_stock_df[instrument_id_main][trade_date],
                "WarehouseWarrantNext": date_instrument_st_stock_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "adj_chg_main": 1,
                "adj_chg_secondary": 1,
            }
        trade_date_available_list.append(trade_date)

    min_count = PeriodType.get_min_count(period_type)
    engine_md = Config.get_db_engine(Config.DB_SCHEMA_MD)
    sql_str = r"""select lower(InstrumentID) instrument_id, TradingDay trade_date, 
date_format(UpdateTime,'%%H:%%i:%%s') UpdateTime, 
Open, High, Low, Close, OpenInterest OI, vol Volume 
from md_min_n where InstrumentID regexp %s and period_min=%s and shift_min=0
and UpdateTime BETWEEN '8:30:00' AND '15:30:00'"""
    data_df = pd.read_sql(sql_str, engine_md, params=['^%s[0-9]+' % instrument_type, min_count])
    data_df_dic = {instrument_id: data_sub_df.set_index("trade_date") for instrument_id, data_sub_df in
                   data_df.groupby("instrument_id")}

    # 建立DataFrame
    if len(date_reorg_data_dic) == 0:
        date_noadj_df, date_adj_df = None, None
        return date_noadj_df, date_adj_df
    else:
        merge_df_list = []
        for trade_date, info_dic in date_reorg_data_dic.items():
            # 获取每天主力，次主力合约
            instrument_id_main = info_dic["Contract"]
            if instrument_id_main not in data_df_dic:
                logger.info("%s 没有主力合约%s %d 分钟数据，合约不存在", trade_date, instrument_id_main, min_count)
                continue
            data_sub_df_main = data_df_dic[instrument_id_main]
            if trade_date not in data_sub_df_main.index:
                logger.warning("%s 没有主力合约%s %d 分钟数据", trade_date, instrument_id_main, min_count)
                continue
            data_sub_df_main_cur_day = data_sub_df_main.loc[[trade_date]]
            data_sub_df_main_cur_day["WarehouseWarrant"] = info_dic["WarehouseWarrant"]
            data_sub_df_main_cur_day.rename(columns={
                "instrument_id": "Contract",
            }, inplace=True)

            # 获取每天次主力合约
            instrument_id_secondary = info_dic["ContractNext"]
            data_sub_df_secondary_cur_day = None
            if instrument_id_secondary is None:
                data_sub_df_secondary = None
            elif instrument_id_secondary not in data_df_dic:
                data_sub_df_secondary = None
                logger.info("%s 没有次主力合约%s %d 分钟数据，合约不存在", trade_date, instrument_id_main, min_count)
            else:
                data_sub_df_secondary = data_df_dic[instrument_id_secondary]
                if trade_date not in data_sub_df_secondary.index:
                    logger.warning("%s 没有次主力合约%s %d 分钟数据", trade_date, instrument_id_secondary, min_count)
                else:
                    data_sub_df_secondary_cur_day = data_sub_df_secondary.loc[[trade_date]]
                    data_sub_df_secondary_cur_day.rename(columns={
                        "instrument_id": "ContractNext",
                        "Open": "OpenNext",
                        "High": "HighNext",
                        "Low": "LowNext",
                        "Close": "CloseNext",
                        "OI": "OINext",
                        "Volume": "VolumeNext",
                    }, inplace=True)
                    data_sub_df_secondary_cur_day["WarehouseWarrantNext"] = info_dic["WarehouseWarrantNext"]

            # 合并主力、次主力合约数据
            if data_sub_df_secondary_cur_day is not None:
                merge_df = pd.merge(data_sub_df_main_cur_day, data_sub_df_secondary_cur_day, how="outer",
                                    on="UpdateTime")
            else:
                merge_df = data_sub_df_main_cur_day
            if merge_df.shape[0] == 0:
                continue

            # 设置 Date "TermStructure"
            merge_df.loc[:, "Contract"] = instrument_id_main
            merge_df.loc[:, "ContractNext"] = instrument_id_secondary
            merge_df.loc[:, "Date"] = trade_date
            merge_df.loc[:, "TermStructure"] = info_dic["TermStructure"]
            # 设置 adj_chg_main
            adj_chg_main = info_dic["adj_chg_main"]
            vals = np.ones(merge_df.shape[0])
            vals[0] = adj_chg_main
            merge_df.loc[:, "adj_chg_main"] = vals

            # 设置 adj_chg_secondary
            adj_chg_secondary = info_dic["adj_chg_secondary"]
            vals = np.ones(merge_df.shape[0])
            vals[0] = adj_chg_secondary
            merge_df.loc[:, "adj_chg_secondary"] = vals
            # 加入列表
            merge_df_list.append(merge_df)

        date_reorg_data_df = pd.concat(merge_df_list)
        date_reorg_data_df["trade_date"] = date_reorg_data_df["Date"]
        date_reorg_data_df["UpdateTime_idx"] = date_reorg_data_df["UpdateTime"]
        date_reorg_data_df = date_reorg_data_df.set_index(["trade_date", "UpdateTime_idx"])
        col_list = ["Date", "UpdateTime", "Contract", "ContractNext", "Close", "CloseNext", "TermStructure",
                    "Volume", "VolumeNext", "OI", "OINext", "Open", "OpenNext", "High", "HighNext",
                    "Low", "LowNext", "WarehouseWarrant", "WarehouseWarrantNext"]
        # 无复权价格
        date_noadj_df = date_reorg_data_df[col_list].copy()
        # 前复权价格
        date_reorg_data_df['adj_factor_main'] = date_reorg_data_df.sort_index(ascending=False)['adj_chg_main'].cumprod()
        date_reorg_data_df['Open'] = date_reorg_data_df['Open'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['High'] = date_reorg_data_df['High'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['Low'] = date_reorg_data_df['Low'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['Close'] = date_reorg_data_df['Close'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['adj_factor_secondary'] = date_reorg_data_df.sort_index(ascending=False)[
            'adj_chg_secondary'].cumprod()
        date_reorg_data_df['OpenNext'] = date_reorg_data_df['OpenNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['HighNext'] = date_reorg_data_df['HighNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['LowNext'] = date_reorg_data_df['LowNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['CloseNext'] = date_reorg_data_df['CloseNext'] * date_reorg_data_df['adj_factor_secondary']
        col_list = ["Date", "UpdateTime", "Contract", "ContractNext", "Close", "CloseNext", "TermStructure",
                    "Volume", "VolumeNext", "OI", "OINext", "Open", "OpenNext", "High", "HighNext",
                    "Low", "LowNext", "WarehouseWarrant", "WarehouseWarrantNext",
                    "adj_factor_main", "adj_factor_secondary"]
        date_adj_df = date_reorg_data_df[col_list].copy()

    date_adj_df.rename(columns={
        "Date": "trade_date"
    }, inplace=True)
    # 为了解决 AttributeError: 'numpy.float64' object has no attribute 'translate' 错误，需要将数据类型转换成 float
    date_adj_df["Close"] = date_adj_df["Close"].apply(float)
    date_adj_df["CloseNext"] = date_adj_df["CloseNext"].apply(float)
    date_adj_df["TermStructure"] = date_adj_df["TermStructure"].apply(float)
    date_adj_df["Volume"] = date_adj_df["Volume"].apply(float)
    date_adj_df["VolumeNext"] = date_adj_df["VolumeNext"].apply(float)
    date_adj_df["OI"] = date_adj_df["OI"].apply(float)
    date_adj_df["OINext"] = date_adj_df["OINext"].apply(float)
    date_adj_df["Open"] = date_adj_df["Open"].apply(float)
    date_adj_df["OpenNext"] = date_adj_df["OpenNext"].apply(float)
    date_adj_df["High"] = date_adj_df["High"].apply(float)
    date_adj_df["HighNext"] = date_adj_df["HighNext"].apply(float)
    date_adj_df["Low"] = date_adj_df["Low"].apply(float)
    date_adj_df["LowNext"] = date_adj_df["LowNext"].apply(float)
    date_adj_df["WarehouseWarrant"] = date_adj_df["WarehouseWarrant"].apply(float)
    date_adj_df["WarehouseWarrantNext"] = date_adj_df["WarehouseWarrantNext"].apply(float)
    date_adj_df["adj_factor_main"] = date_adj_df["adj_factor_main"].apply(float)
    date_adj_df["adj_factor_secondary"] = date_adj_df["adj_factor_secondary"].apply(float)
    date_adj_df['instrument_type'] = instrument_type
    date_adj_df['period_min'] = min_count
    table_name = 'reorg_md_min_n'
    engine_md = Config.get_db_engine(Config.DB_SCHEMA_MD)
    # 清理历史记录
    with Config.with_db_session(engine_md) as session:
        sql_str = """SELECT table_name FROM information_schema.TABLES WHERE table_name = :table_name and TABLE_SCHEMA=(select database())"""
        is_existed = session.execute(sql_str, params={"table_name": table_name}).fetchone()
        if is_existed is not None:
            session.execute("delete from %s where instrument_type = :instrument_type" % table_name,
                            params={"instrument_type": instrument_type})
            logger.debug("删除%s 中的 %s 历史数据", table_name, instrument_type)
    # 插入数据库
    pd.DataFrame.to_sql(date_adj_df, table_name, engine_md, if_exists='append', index=False, dtype={
        'trade_date': Date,
        'UpdateTime': Time,
        'Contract': String(20),
        'ContractNext': String(20),
        'period_min': Integer,
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
        'adj_factor_main': DOUBLE,
        'adj_factor_secondary': DOUBLE,
        'instrument_type': String(20),
    })
    return date_noadj_df, date_adj_df


def export_2_csv(instrument_type_list, period="daily"):
    folder_path = os.path.join(os.path.abspath('.'), 'Commodity_%s' % period)
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    for instrument_type in instrument_type_list:
        logger.info("开始导出 %s 相关数据", instrument_type)
        if period == "daily":
            date_noadj_df, date_adj_df = data_reorg_daily(instrument_type)
        elif period == "half_day":
            date_noadj_df, date_adj_df = data_reorg_half_daily(instrument_type)
        elif period in (PeriodType.Min1, PeriodType.Min5, PeriodType.Min15):
            date_noadj_df, date_adj_df = data_reorg_min_n(instrument_type, period)
        else:
            raise ValueError("%s 不支持" % period)
        if date_noadj_df is not None:
            file_name = "%s ConInfoFull.csv" % instrument_type
            file_path = os.path.join(folder_path, file_name)
            logger.info(file_path)
            date_noadj_df.to_csv(file_path, index=False)

            file_name = "%s ConInfoFull_Adj.csv" % instrument_type
            file_path = os.path.join(folder_path, file_name)
            date_adj_df.to_csv(file_path, index=False)
            logger.info(file_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    #
    instrument_type_list = ["RU", "AG", "AU", "RB", "HC", "J", "JM", "I", "CU",
                            "AL", "ZN", "PB", "NI", "SN",
                            "SR", "CF"]
    # instrument_type_list = ["JM"]
    # export_2_csv(instrument_type_list, period="daily")
    export_2_csv(instrument_type_list, period="half_day")
    # export_2_csv(instrument_type_list, period=PeriodType.Min15)
    export_2_csv(instrument_type_list, period=PeriodType.Min5)

    # 单独生成某合约历史数据
    # instrument_type = "RU"
    # period = PeriodType.Min15
    # data_reorg_daily(instrument_type)
    # data_reorg_half_daily(instrument_type)
    # data_reorg_min_n(instrument_type, period)