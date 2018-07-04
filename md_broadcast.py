# -*- coding: utf-8 -*-
"""
Created on 2017/6/25
@author: MG
"""
import logging
import time, re
from datetime import datetime, timedelta
from backend.fh_utils import str_2_bytes
from config import Config
from pyctp_api import MyMdApi, MyTraderApi
from ctp import ApiStruct
from backend.check import check
logger = logging.getLogger()


def broadcast_md():
    """
    对mdapi 进行创建、登录、订阅等操作
    :return: 
    """
    # 环境检查，全部通过后方可继续
    if not check():
        logger.error("检测未通过")
        return
    # 开启 api
    trader_api = MyTraderApi(
        enable_background_thread=True,
        auto_update_instrument_list=True,
        auto_update_position=False)
    trader_api.RegisterFront()
    trader_api.SubscribePrivateTopic(ApiStruct.TERT_QUICK)
    trader_api.SubscribePublicTopic(ApiStruct.TERT_QUICK)
    trader_api.Init()
    # 检查交易端登录情况
    logger.info("等待 登录确认")
    for _ in range(5):
        if trader_api.has_login:
            # logger.info("交易端登录成功")
            break
        else:
            time.sleep(1)
    else:
        logging.warning('交易端登录超时，交易所时间同步可能不准，导致分钟线存在误差')
        return

    # 检查合约信息更新情况
    logger.info("等待 合约列表 更新")
    for _ in range(60):
        if trader_api.has_login and trader_api.has_update_instruemnt:
            logger.info("合约列表 更新成功")
            break
        else:
            time.sleep(1)
    else:
        logging.warning('合约列表 更新超时')
        return

    # 初始化 md_api
    # Config.load_instrument_list()
    # subscribe_instrument_list = Config.subscribe_instrument_list
    instrument_type_set = {"IF", "IH", "IC",
                           "rb", "i", "jm", "j", "pb", "ru", "au", "ag", "cu", "al", "zn", "hc", "ni", "sn",
                           "cf", "sr", "zc", "ap"}
    # 测试使用少订阅几个合约
    # instrument_type_set = {"IF", "IH", "IC",
    #                        "rb", "i", "jm", "j",
    #                        "cf", "ap"}
    re_pattern_instrument_type = re.compile(r"^[A-Za-z]+(?=\d{3,4}$)")
    instrument_list_all = list(Config.instrument_info_dic.keys())
    instrument_type_match_list = [re_pattern_instrument_type.match(instrument_id) for instrument_id in instrument_list_all]
    subscribe_instrument_list = [str_2_bytes(instrument_id)
                                 for instrument_id, instrument_type in zip(instrument_list_all, instrument_type_match_list)
                                 if instrument_type is not None
                                 and instrument_type.group() in instrument_type_set]
    # subscribe_instrument_list = [b'ru1801', b'rb1805', b'i1805', b'IF1801']
    logger.info('订阅合约列表[%d]：%s', len(subscribe_instrument_list), subscribe_instrument_list)
    md_api = MyMdApi(subscribe_instrument_list)  # [b'rb1710', b'rb1712']
    # 3． 向 API 实例注册前置地址。交易接口需要注册交易前置地址，行情接口需要注册行情前置地址。
    md_api.RegisterFront()
    md_api.Init()
    time.sleep(1)
    try:
        max_count = 20
        date_time_now = datetime.now()
        min_exch_timedelta = Config.exch_timedelta
        # 流出1分钟时间防止出现本地时间与服务器时间偏差
        trade_time_range = [
            (datetime(date_time_now.year, date_time_now.month, date_time_now.day, 8) + min_exch_timedelta,
             datetime(date_time_now.year, date_time_now.month, date_time_now.day, 15, 3) + min_exch_timedelta),
            (datetime(date_time_now.year, date_time_now.month, date_time_now.day, 20) + min_exch_timedelta,
             datetime(date_time_now.year, date_time_now.month, date_time_now.day, 2, 33) + timedelta(days=1) + min_exch_timedelta),
        ]
        # for n in range(max_count):
        logger.info("服务器运行时间范围：")
        for n, (datetime_from, datetime_to) in enumerate(trade_time_range):
            logger.info("%d) %s ~ %s", n+1, datetime_from.strftime("%Y-%m-%d %H:%M:%S.%f"), datetime_to.strftime("%Y-%m-%d %H:%M:%S.%f"))
        # 用于一次性更新合约手续费信息
        do_once = True
        while md_api.has_login:
            time.sleep(1)
            datetime_curr = datetime.now()
            is_in_time_range = False
            for datetime_from, datetime_to in trade_time_range:
                if datetime_from < datetime_curr < datetime_to:
                    is_in_time_range = True
                    break
            if not is_in_time_range:
                logger.info('行情时段结束 %s', datetime_curr)
                break
            if do_once and Config.update_instrument_finished:
                trader_api.req_qry_instrument_commission_rate_all()
                do_once = False
            # if n == 1:
            # md_api.UnSubscribeMarketData([b'rb1712'])
    except KeyboardInterrupt:
        pass
    finally:
        md_api.ReqUserLogout()
        md_api.Release()

        trader_api.ReqUserLogout()
        trader_api.Release()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    broadcast_md()
    # test_trader_api()
