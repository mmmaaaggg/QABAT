# -*- coding: utf-8 -*-
"""
Created on 2018/1/14
@author: MG
"""
import matplotlib.pyplot as plt
from backend.fh_utils import return_risk_analysis
import pandas as pd, numpy as np, logging
from config import Config
logger = logging.getLogger()


def get_account_balance(stg_run_id):
    """
    获取 account_info 账户走势数据
    :param stg_run_id: 
    :return: 
    """
    engine = Config.get_db_engine(schema_name=Config.DB_SCHEMA_QABAT)
    sql_str = """SELECT concat(trade_date, " ", trade_time) trade_datetime, available_cash, curr_margin, balance_tot FROM account_status_info where stg_run_id=%s order by trade_date, trade_time"""
    data_df = pd.read_sql(sql_str, engine, params=[stg_run_id])
    data_df["return_rate"] = (data_df["balance_tot"].pct_change().fillna(0) + 1).cumprod()
    data_df = data_df.set_index("trade_datetime")
    return data_df


if __name__ == "__main__":
    with Config.with_db_session(schema_name=Config.DB_SCHEMA_QABAT) as session:
        stg_run_id = session.execute("select max(stg_run_id) from stg_run_info").fetchone()[0]
    # stg_run_id = 2
    data_df = get_account_balance(stg_run_id)

    logger.info("\n%s", data_df)
    data_df.plot(ylim=[min(data_df["available_cash"]), max(data_df["balance_tot"])])
    data_df.plot(ylim=[min(data_df["curr_margin"]), max(data_df["curr_margin"])])
    stat_df = return_risk_analysis(data_df['return_rate'], freq=None)
    logger.info("\n%s", stat_df)
