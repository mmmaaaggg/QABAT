# -*- coding: utf-8 -*-
"""
Created on 2017/9/1
@author: MG
"""


def get_trader_api_and_login():
    import logging
    from pyctp_api import MyTraderApi
    from ctp import ApiStruct
    import time

    logger = logging.getLogger(__name__)
    trader_api = MyTraderApi()
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
        raise ConnectionRefusedError("登录错误，无法执行后续测试")

    return trader_api
