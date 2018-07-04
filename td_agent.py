# -*- coding: utf-8 -*-
"""
Created on 2017/10/3
@author: MG
"""
from abc import abstractmethod, ABC
from collections import OrderedDict
import time
from datetime import datetime
from ctp import ApiStruct
from pyctp_api import MyTraderApi
from backend.orm import OrderInfo, TradeInfo, PosStatusInfo, AccountStatusInfo
from config import Direction, Action, Config, BacktestTradeMode, PositionDateType
import logging
from ctp.ApiStruct import OF_Close, OF_CloseToday, OF_CloseYesterday
from backend.fh_utils import pd_timedelta_2_timedelta


class TraderAgent(ABC):
    """
    交易代理（抽象类），回测交易代理，实盘交易代理的父类
    """

    def __init__(self, stg_run_id, run_mode_params: dict):
        """
        stg_run_id 作为每一次独立的执行策略过程的唯一标识
        :param stg_run_id: 
        """
        self.stg_run_id = stg_run_id
        self.run_mode_params = run_mode_params
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def connect(self):
        raise NotImplementedError()

    @abstractmethod
    def open_long(self, instrument_id, price, vol):
        raise NotImplementedError()

    @abstractmethod
    def close_long(self, instrument_id, price, vol, offset_flag=ApiStruct.OF_Close):
        raise NotImplementedError()

    @abstractmethod
    def open_short(self, instrument_id, price, vol):
        raise NotImplementedError()

    @abstractmethod
    def close_short(self, instrument_id, price, vol, offset_flag=ApiStruct.OF_Close):
        raise NotImplementedError()

    @abstractmethod
    def get_position(self, instrument_id) -> dict:
        raise NotImplementedError()

    @abstractmethod
    def get_order(self, instrument_id) -> OrderInfo:
        raise NotImplementedError()

    @abstractmethod
    def release(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def datetime_last_update_position(self) -> datetime:
        raise NotImplementedError()

    @property
    @abstractmethod
    def datetime_last_rtn_trade_dic(self) -> dict:
        raise NotImplementedError()

    @property
    @abstractmethod
    def datetime_last_update_position_dic(self) -> dict:
        raise NotImplementedError()

    @property
    @abstractmethod
    def datetime_last_send_order_dic(self) -> dict:
        raise NotImplementedError()


class BacktestTraderAgent(TraderAgent):
    """
    供调用模拟交易接口使用
    """

    def __init__(self, stg_run_id, run_mode_params: dict):
        super().__init__(stg_run_id, run_mode_params)
        # 标示 order 成交模式
        self.trade_mode = run_mode_params.setdefault('trade_mode', BacktestTradeMode.Order_2_Deal)
        # 账户初始资金
        self.init_cash = run_mode_params['init_cash']
        # 用来标示当前md，一般执行买卖交易是，对时间，价格等信息进行记录
        self.curr_md_period_type = None
        self.curr_md = None
        # 用来保存历史的 order_info trade_info pos_status_info account_info
        self.order_info_list = []
        self.trade_info_list = []
        self.pos_status_info_dic = OrderedDict()
        self.account_info_list = []

    def set_curr_md(self, period_type, md):
        self.curr_md_period_type = period_type
        self.curr_md = md

    def connect(self):
        self.engine = Config.get_db_engine(Config.DB_SCHEMA_QABAT)
        # 持仓信息 初始化持仓状态字典，key为 instrument_id
        self._pos_status_info_dic = {}
        self._order_info_dic = {}
        # 账户信息
        self._account_status_info = None

    def _save_order_info(self, instrument_id, price: float, vol: int, direction: Direction, action: Action):
        order_info = OrderInfo(stg_run_id=self.stg_run_id,
                               order_date=self.curr_md['ActionDay'],
                               order_time=pd_timedelta_2_timedelta(self.curr_md['ActionTime']),
                               order_millisec=int(self.curr_md.setdefault('ActionMillisec',0)),
                               direction=int(direction),
                               action=int(action),
                               instrument_id=instrument_id,
                               order_price=float(price),
                               order_vol=int(vol)
                               )
        if False: # 暂时不用
            with Config.with_db_session(self.engine, expire_on_commit=False) as session:
                session.add(order_info)
                session.commit()
        self.order_info_list.append(order_info)
        self._order_info_dic.setdefault(instrument_id, []).append(order_info)
        # 更新成交信息
        # Order_2_Deal 模式：下单即成交
        if self.trade_mode == BacktestTradeMode.Order_2_Deal:
            self._save_trade_info(order_info)

    def _save_trade_info(self, order_info: OrderInfo):
        """
        根据订单信息保存成交结果
        :param order_info: 
        :return: 
        """
        trade_info = TradeInfo.create_by_order_info(order_info)
        self.trade_info_list.append(trade_info)
        # 更新持仓信息
        self._save_pos_status_info(trade_info)

    def _save_pos_status_info(self, trade_info: TradeInfo):
        """
        根据成交信息保存最新持仓信息
        :param trade_info: 
        :return: 
        """
        instrument_id = trade_info.instrument_id
        # direction, action, instrument_id = trade_info.direction, trade_info.action, trade_info.instrument_id
        # trade_price, trade_vol, trade_id = trade_info.trade_price, trade_info.trade_vol, trade_info.trade_id
        # trade_date, trade_time, trade_millisec = trade_info.trade_date, trade_info.trade_time, trade_info.trade_millisec
        if instrument_id in self._pos_status_info_dic:
            pos_status_info_last = self._pos_status_info_dic[instrument_id]
            pos_status_info = pos_status_info_last.update_by_trade_info(trade_info)
        else:
            pos_status_info = PosStatusInfo.create_by_trade_info(trade_info)
        # 更新
        trade_date, trade_time, trade_millisec = \
            pos_status_info.trade_date, pos_status_info.trade_time, pos_status_info.trade_millisec
        self.pos_status_info_dic[(trade_date, trade_time, trade_millisec)] = pos_status_info
        self._pos_status_info_dic[instrument_id] = pos_status_info
        # self.c_save_acount_info(pos_status_info)

    def update_account_info(self):
        """
        更加 持仓盈亏数据汇总统计当前周期账户盈利情况
        :return: 
        """
        if self.curr_md is None:
            return
        if self._account_status_info is None:
            self._account_status_info = AccountStatusInfo.create(self.stg_run_id, self.init_cash, self.curr_md)
            self.account_info_list.append(self._account_status_info)

        instrument_id = self.curr_md['InstrumentID']
        if instrument_id in self._pos_status_info_dic:
            pos_status_info_last = self._pos_status_info_dic[instrument_id]
            trade_date = pos_status_info_last.trade_date
            trade_time = pos_status_info_last.trade_time
            # 如果当前K线以及更新则不需再次更新。如果当前K线以及有交易产生，则 pos_info 将会在 _save_pos_status_info 函数中被更新，因此无需再次更新
            if trade_date == self.curr_md['ActionDay'] and trade_time == self.curr_md['ActionTime']:
                return
            # 说明上一根K线位置已经平仓，下一根K先位置将记录清除
            if pos_status_info_last.position == 0:
                del self._pos_status_info_dic[instrument_id]
            # 根据 md 数据更新 仓位信息
            pos_status_info = pos_status_info_last.update_by_md(self.curr_md)
            self._pos_status_info_dic[instrument_id] = pos_status_info

        # 统计账户信息，更新账户信息
        account_status_info = self._account_status_info.update_by_pos_status_info(self._pos_status_info_dic, self.curr_md)
        self._account_status_info = account_status_info
        self.account_info_list.append(self._account_status_info)

    def open_long(self, instrument_id, price, vol):
        self._save_order_info(instrument_id, price, vol, Direction.Long, Action.Open)

    def close_long(self, instrument_id, price, vol, offset_flag=ApiStruct.OF_Close):
        self._save_order_info(instrument_id, price, vol, Direction.Long, Action.Close)

    def open_short(self, instrument_id, price, vol):
        self._save_order_info(instrument_id, price, vol, Direction.Short, Action.Open)

    def close_short(self, instrument_id, price, vol, offset_flag=ApiStruct.OF_Close):
        self._save_order_info(instrument_id, price, vol, Direction.Short, Action.Close)

    def get_position(self, instrument_id) -> dict:
        if instrument_id in self._pos_status_info_dic:
            pos_status_info = self._pos_status_info_dic[instrument_id]
            position_date_pos_info_dic = {PositionDateType.Today: pos_status_info}
        else:
            position_date_pos_info_dic = None
        return position_date_pos_info_dic

    @property
    def datetime_last_update_position(self) -> datetime:
        return datetime.now()

    @property
    def datetime_last_rtn_trade_dic(self) -> dict:
        raise NotImplementedError()

    @property
    def datetime_last_update_position_dic(self) -> dict:
        raise NotImplementedError()

    @property
    def datetime_last_send_order_dic(self) -> dict:
        raise NotImplementedError()

    def release(self):
        try:
            with Config.with_db_session(schema_name=Config.DB_SCHEMA_QABAT) as session:
                session.add_all(self.order_info_list)
                session.add_all(self.trade_info_list)
                session.add_all(self.pos_status_info_dic.values())
                session.add_all(self.account_info_list)
                session.commit()
        except:
            self.logger.exception("release exception")

    def get_order(self, instrument_id) -> OrderInfo:
        if instrument_id in self._order_info_dic:
            return self._order_info_dic[instrument_id]
        else:
            return None


class RealTimeTraderAgent(TraderAgent):
    """
    供调用实时交易接口使用
    """

    def __init__(self, stg_run_id, run_mode_params: dict):
        super().__init__(stg_run_id, run_mode_params)
        self.trader_api = MyTraderApi(auto_update_position=True, enable_background_thread=True)

    def connect(self):
        self.trader_api.RegisterFront()
        self.trader_api.SubscribePrivateTopic(ApiStruct.TERT_QUICK)
        self.trader_api.SubscribePublicTopic(ApiStruct.TERT_QUICK)
        self.trader_api.Init()
        for _ in range(5):
            if self.trader_api.has_login:
                self.logger.info("MyTraderApi 登录确认成功")
                break
            else:
                time.sleep(1)
        else:
            self.logger.warning('trade api 登录超时，交易所时间同步可能不准，导致分钟线存在误差')

        for _ in range(10):
            if self.trader_api.is_settlement_info_confirmed:
                self.logger.info("MyTraderApi 结算信息确认成功")
                break
            else:
                time.sleep(1)
        else:
            self.logger.warning('trade api 查询结算信息确认超时，可能导致交易请求无法发送')

    def open_long(self, instrument_id, price, vol):
        self.trader_api.open_long(instrument_id, price, vol)

    def close_long(self, instrument_id, price, vol, offset_flag=ApiStruct.OF_Close):
        self.trader_api.close_long(instrument_id, price, vol, offset_flag)

    def open_short(self, instrument_id, price, vol):
        self.trader_api.open_short(instrument_id, price, vol)

    def close_short(self, instrument_id, price, vol, offset_flag=ApiStruct.OF_Close):
        self.trader_api.close_short(instrument_id, price, vol, offset_flag)

    def get_position(self, instrument_id) -> dict:
        position_date_inv_pos_dic = self.trader_api.get_position(instrument_id)
        # TODO: 临时使用 PosStatusInfo 作为返回对象，以后需要重新设计一个相应对象（不是所有属性都能够对应上）
        if position_date_inv_pos_dic is None:
            position_date_pos_info_dic = None
        else:
            position_date_pos_info_dic = PosStatusInfo.create_by_dic(position_date_inv_pos_dic)
        return position_date_pos_info_dic

    @property
    def datetime_last_update_position(self) -> datetime:
        return self.trader_api.datetime_last_update_position

    @property
    def datetime_last_rtn_trade_dic(self) -> dict:
        return self.trader_api.datetime_last_rtn_trade_dic

    @property
    def datetime_last_update_position_dic(self) -> dict:
        return self.trader_api.datetime_last_update_position_dic

    @property
    def datetime_last_send_order_dic(self) -> dict:
        return self.trader_api.datetime_last_send_order_dic

    def get_order(self, instrument_id) -> list:
        order_dic_list = self.trader_api.get_order(instrument_id)
        # TODO: 临时使用 OrderInfo 作为返回对象，以后需要重新设计一个相应对象（不是所有属性都能够对应上）
        if order_dic_list is None or len(order_dic_list) == 0:
            order_info_list = None
        else:
            order_info_list = [OrderInfo.create_by_dic(order_dic) for order_dic in order_dic_list]
        return order_info_list

    def cancel_order(self, instrument_id):
        return self.trader_api.cancel_order(instrument_id)

    def release(self):
        self.trader_api.ReqUserLogout()
        self.trader_api.Release()
