# -*- coding: utf-8 -*-
"""
Created on 2018/1/11
@author: MG
"""
import logging
import shutil
from unittest.mock import Mock, patch
import time, re, os
from datetime import datetime, timedelta
from backend.fh_utils import str_2_bytes, bytes_2_str, str_2_date, date_2_str
from config import Config, ConfigProduct
from pyctp_api import MyTraderApi, ApiBase
from ctp import ApiStruct
import pandas as pd
import numpy as np

logger = logging.getLogger()


def get_account_info():
    """
    获取当前账户权益，保证金，风险度等信息
    :return: 
    """
    trader_api = MyTraderApi(
        enable_background_thread=False,
        auto_update_instrument_list=False,
        auto_update_position=False)
    trader_api.RegisterFront()
    trader_api.SubscribePrivateTopic(ApiStruct.TERT_QUICK)
    trader_api.SubscribePublicTopic(ApiStruct.TERT_QUICK)
    trader_api.Init()
    # 检查交易端登录情况
    logger.info("等待 登录确认")
    for _ in range(5):
        if trader_api.has_login:
            logger.info("交易端登录成功")
            break
        else:
            time.sleep(1)
    else:
        logging.warning('交易端登录超时，交易所时间同步可能不准，导致分钟线存在误差')
        return
    # trader_api.trading_day
    job_info = {"done": False}

    def replaced_method(pTradingAccount: ApiStruct.TradingAccount, pRspInfo, nRequestID, bIsLast):
        """替代模块内相应函数"""
        logger.info('OnRspQryTradingAccount called with: %s %s %s %s',
                    pTradingAccount, pRspInfo, nRequestID, bIsLast)
        status = trader_api.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        # trading_account_dic = ApiBase.struct_2_dic(pTradingAccount)
        trading_day = bytes_2_str(pTradingAccount.TradingDay)
        trading_account_dic = {
            "日期": "%s-%s-%s" % (trading_day[:4], trading_day[4:6], trading_day[6:8]),
            "累计净值": "",
            "静态权益": pTradingAccount.PreBalance,
            "动态权益": pTradingAccount.Balance,
            "占用保证金": pTradingAccount.CurrMargin,  # pTradingAccount.ExchangeMargin,
            "总盈亏": "",
            "持仓盈亏": pTradingAccount.PositionProfit,
            "可用资金": pTradingAccount.Available,
            "手续费": pTradingAccount.Commission,
            "风险度": (pTradingAccount.CurrMargin / pTradingAccount.Balance) if pTradingAccount.Balance != 0 else np.nan,
            # 期货风险度=持仓保证金/客户权益
        }
        logger.info("trading_account_dic:%s", trading_account_dic)
        cur_df = pd.DataFrame([trading_account_dic])
        logger.info("当前账户信息：%s", cur_df)
        job_info["cur_df"] = cur_df
        job_info["done"] = True

    try:
        with patch.object(trader_api, 'OnRspQryTradingAccount', Mock(wraps=replaced_method)) as mock_method:
            trader_api.ReqQryTradingAccount()
            for n_times in range(10):
                if job_info["done"]:
                    break
                time.sleep(0.5 * n_times)
                # logger.info("waiting")
    finally:
        trader_api.ReqUserLogout()
        trader_api.Release()

    return job_info.setdefault("cur_df", None)


def auto_record_account_info(file_path):
    """
    自动追加生成每日账户结算信息excel文件
    :param file_path: 
    :return: 
    """
    fild_path_name, file_extension = os.path.splitext(file_path)
    cur_df = get_account_info()
    if cur_df is None:
        logger.debug('没有获取到数据')
        return
    col_name_list = ["日期", "累计净值", "静态权益", "动态权益", "占用保证金", "总盈亏", "持仓盈亏",
                     "可用资金", "手续费", "风险度", "资金净流入", "备注"]
    if os.path.exists(file_path):
        data_df = pd.read_excel(file_path)
        tot_df = data_df.append(cur_df)[col_name_list]
    else:
        tot_df = cur_df[[_ for _ in col_name_list if _ in cur_df]]

    tot_df["日期"] = tot_df["日期"].apply(date_2_str)
    logger.info("最新账户信息：\n%s", tot_df)
    file_path_new = fild_path_name + date_2_str(tot_df["日期"].iloc[-1]) + file_extension
    logger.info("最新文件路径：\n%s", file_path_new)
    tot_df.to_excel(file_path_new, index=False)
    shutil.copyfile(file_path_new, file_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    file_path = r"C:\Users\26559\Documents\工作\自营操盘\资金及净值记录.xlsx"
    auto_record_account_info(file_path)
