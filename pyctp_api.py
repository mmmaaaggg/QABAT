# -*- coding: utf-8 -*-
"""
Created on 2017/6/9
@author: MG
"""
import logging
import threading
import inspect
from ctp import ApiStruct, MdApi, TraderApi
import hashlib, os, sys, tempfile, time, re
from config import Config, PeriodType, PositionDateType
from backend.fh_utils import str_2_bytes, bytes_2_str
from datetime import datetime, timedelta, date
from queue import Queue, Empty
from threading import Thread
from collections import OrderedDict
from backend.orm import CommissionRateModel
from event_agent import event_agent, EventType
from md_saver import MdMin1Combiner, MdMinNCombiner, MdSaver, MdPublisher

TOO_SMALL_TO_AVAILABLE = 0.0000001
TOO_LARGE_TO_AVAILABLE = 100000000
OST_Canceled_STR = bytes_2_str(ApiStruct.OST_Canceled)

FRONT_DISCONNECTED_REASON_DIC = {
    0x1001: '网络读失败',
    0x1002: '网络写失败',
    0x2001: '接收心跳超时',
    0x2002: '发送心跳失败',
    0x2003: '收到错误报文',
}


class RequestInfo:
    """
    保存 request 相关信息到 queue中，共其他线程执行
    """

    def __init__(self, func, p_struct, request_id, request_timeout=0, max_wait_rsp_time=2):
        """
        
        :param func: 
        :param p_struct: 
        :param request_id: 
        :param request_timeout: 
        :param max_wait_rsp_time: 
        """
        self.func = func
        self.p_struct = p_struct
        self.request_id = request_id
        self.request_timeout = request_timeout
        self.create_datetime = datetime.now()
        self.handle_datetime = datetime.now()
        self.max_wait_rsp_time = max_wait_rsp_time


class ApiBase:
    def __init__(self,
                 broker_id=Config.BROKER_ID,
                 user_id=Config.USER_ID,
                 password=Config.PASSWORD):
        self.logger = logging.getLogger(self.__class__.__name__)
        # TODO:  未来将通过 self.init_request_id() 增加对 request_id的初始化动作
        self._request_id = 0
        self._mutex_request_id = threading.Lock()
        # TODO: 初始化 is_SettlementInfoConfirmed 信息，并封装成属性，同步更新数据库
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = password
        self.trading_day = ''
        self.front_id = 1
        self.session_id = 0
        # 已登录标志位
        self._has_login = False
        # 请求响应队列
        self._request_info_resp_queue = Queue()
        # 等待请求响应字典
        self._request_info_wait_resp_dic = {}
        # 等待发送请求队列
        self._request_info_will_be_send_queue = Queue()
        self._handle_request_in_queue_thread = Thread(target=self._handle_request_in_queue, daemon=True)
        self._handle_request_in_queue_thread_running = True

    @property
    def has_login(self):
        return self._has_login

    @has_login.setter
    def has_login(self, val):
        self._has_login = val
        if val and not self._handle_request_in_queue_thread.is_alive():
            self._handle_request_in_queue_thread_running = True
            self._handle_request_in_queue_thread.start()
        elif not val:
            self._handle_request_in_queue_thread_running = False

    @staticmethod
    def struc_attr_value_transfor(attr_value, validate_data_type=None):
        """
        用于将 OnRsp 接口中返回的类型的对应属性值中bytes 值转换为str ，其他类型原样返回
        目前该函数仅供 struct_2_json 函数使用
        :param attr_value:
        :param validate_data_type: 默认为 None， 验证字段是否有效，例如：date类型，验证字符串是否为日期格式（处于性能考虑，目前只验证字段长度，达到8为就代表是日期格式，对于 StartDelivDate 有时候会传递 b'1'值导致合约更新失败）
        :return: 
        """
        val_type = type(attr_value)
        if val_type == bytes:
            ret = bytes_2_str(attr_value)
        else:
            ret = attr_value
        if validate_data_type is not None:
            if validate_data_type == date:
                if len(attr_value)!=8:
                    ret = None
                else:
                    try:
                        ret = datetime.strptime(attr_value, Config.DATE_FORMAT_STR_CTP)
                    except:
                        ret = None
        return ret

    @staticmethod
    def struct_2_dic(a_struct, validate_data_type:dict=None):
        """
        用于将 OnRsp 接口中返回的类型转换为 json 字符串
        :param a_struct:
        :param validate_data_type: 默认为 None， 验证字段是否有效
        :return:
        """
        attr_dic = {k: ApiBase.struc_attr_value_transfor(getattr(a_struct, k),
                                                         None if validate_data_type is None else validate_data_type.setdefault(k, None))
                    for k, v in a_struct._fields_}
        # json_str = json.dumps(attr_dic)
        return attr_dic

    @staticmethod
    def insert_one_2_db(a_struct, collection_name=None, **kwargs):
        """
        用于将 OnRsp 接口中返回的类型转换为 json 字符串，插入mongo数据库制定 collectoin 中
        :param a_struct: 
        :param collection_name: 
        :return: 
        """
        if collection_name is None:
            collection_name = a_struct.__class__.__name__
        dic = ApiBase.struct_2_dic(a_struct)
        dic.update(kwargs)
        # with_mongo_collection(lambda col: col.insert_one(dic), collection_name)
        Config.do_for_mongo_collection(lambda col: col.insert_one(dic), collection_name)
        return dic

    def get_tmp_path(self):
        """
        获取api启动是临时文件存放目录
        :return: 
        """
        folder_name = b''.join((b'ctp.futures', self.broker_id, self.user_id))
        dir_name = hashlib.md5(folder_name).hexdigest()
        dir_path = os.path.join(tempfile.gettempdir(), dir_name, self.__class__.__name__) + os.sep
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        return os.fsencode(dir_path) if sys.version_info[0] >= 3 else dir_path

    def is_rsp_success(self, rsp_info, stack_num=1):
        """
        检查 rsp_info 状态，已上层函数名记录相应日志
        :param rsp_info: 
        :param stack_num: 
        :return: 
        """
        is_success = rsp_info is None or rsp_info.ErrorID == 0
        if not is_success:
            stack = inspect.stack()
            parent_function_name = stack[stack_num].function
            self.logger.error('%s 失败：%s', parent_function_name, bytes_2_str(rsp_info.ErrorMsg))
        return is_success

    def resp_common(self, rsp_info, request_id, is_last, stack_num=1):
        """
        查询 RspInfo状态及bLsLast状态
        :param rsp_info: 
        :param is_last: 
        :param stack_num: 被调用函数堆栈层数
        :return: 1 成功结束；0 成功但未结束；-1 未成功
        """
        # self.logger.debug("resp: %s" % str(rsp_info))
        is_success = self.is_rsp_success(rsp_info, stack_num=2)
        if not is_success:
            self._get_response(request_id)
            return -1
        elif is_last and is_success:
            self._get_response(request_id)
            return 1
        else:
            # try:
            # stack = inspect.stack()
            # parent_function_name = stack[stack_num].function
            # self.logger.debug("%s 等待数据接收完全...", parent_function_name)
            # except:
            #     self.logger.warning("get stack error")
            return 0

    def init_request_id(self):
        """
        增加对 request_id的初始化动作
        :return: 
        """
        raise NotImplementedError()

    def inc_request_id(self):
        """
        获取自增的 request_id （线程安全）
        :return: 
        """
        self._mutex_request_id.acquire()
        self._request_id += 1
        self._mutex_request_id.release()
        return self._request_id

    def log_req_if_error(self, req_ret, func_name=None, stack_num=1):
        """
        如果req 返回小于0 则记录错误信息
        -1，表示网络连接失败；
        -2，表示未处理请求超过许可数；
        -3，表示每秒发送请求数超过许可数。
        :param req_ret: 
        :param stack_num: 被调用函数堆栈层数
        :return: 
        """
        if req_ret is None:
            msg = "req 返回为 None"
            if func_name is None:
                stack = inspect.stack()
                func_name = stack[stack_num].function
            self.logger.error('%s 返回错误：%s', func_name, msg)
        elif req_ret < 0:
            if req_ret == -1:
                msg = '网络连接失败'
            elif req_ret == -2:
                msg = '未处理请求超过许可数'
            elif req_ret == -3:
                msg = '每秒发送请求数超过许可数'
            else:
                msg = '其他原因'
            if func_name is None:
                stack = inspect.stack()
                func_name = stack[stack_num].function
            self.logger.error('%s 返回错误：%s', func_name, msg)

    def _send_request_2_queue(self, func, p_struct, add_request_id=False, add_order_ref=False, request_timeout=2, max_wait_rsp_time=2):
        """
        公共Request方法，传入方面，及相关struct数据
        函数自动生成新的request_id及进行log记录
        :param func: 
        :param p_struct: 
        :param add_request_id: 
        :param request_timeout: 默认1秒钟超时。0 不做超时检查
        :param max_wait_rsp_time: 默认2秒钟超时。0 不做超时检查
        :return: 
        """
        request_id = self.inc_request_id()
        if add_request_id:
            p_struct.RequestID = request_id
        if add_order_ref:
            order_ref = self.inc_order_ref()
            p_struct.OrderRef = order_ref
        req_info = RequestInfo(func, p_struct, request_id, request_timeout, max_wait_rsp_time)
        # self.logger.debug("发送请求到 _request_info_will_be_send_queue")
        self._request_info_will_be_send_queue.put(req_info)
        # self.logger.debug("发送请求到 _request_info_will_be_send_queue 完成")
        return 0

    def _handle_request_in_queue(self):
        """
        用于从后台请求队列中获取请求，并执行发送
        :return: 
        """
        self.logger.info("%s 请求队列 后台发送线程 启动", self.__class__.__name__)
        max_wait_time = 30
        datetime_last_req = datetime.now()
        # 设定最大请求发送频率 0.5 秒钟一笔请求
        max_seconds_4_freq_request = 1
        while self._handle_request_in_queue_thread_running:
            # CTP存在请求过于密集拒绝响应的情况，因此，设定一个请求等待机制
            # 交易所未返回请求响应前等待最大 max_wait_time 秒
            # 超过响应时间，则认为该请求已经废弃，继续执行后面的请求
            # self.logger.debug("后台请求队列处理线程 Looping 1")
            try:
                if len(self._request_info_wait_resp_dic) > 0:
                    request_id = self._request_info_resp_queue.get(timeout=1)
                    if request_id in self._request_info_wait_resp_dic:
                        req_info = self._request_info_wait_resp_dic.pop(request_id)
                        datetime_last_req = req_info.handle_datetime
                    self._request_info_resp_queue.task_done()
            except Empty:
                for request_id in list(self._request_info_wait_resp_dic.keys()):
                    req_info = self._request_info_wait_resp_dic[request_id]
                    handle_datetime = req_info.handle_datetime
                    if (datetime.now() - handle_datetime).seconds > req_info.max_wait_rsp_time:
                        self.logger.warning('请求超时 %s[%d] %s -> %s', req_info.func.__name__, request_id,
                                            handle_datetime, req_info.p_struct)
                        del self._request_info_wait_resp_dic[request_id]
                    else:
                        self.logger.debug("等待请求响应 %s[%d] %s -> %s", req_info.func.__name__, request_id,
                                          handle_datetime, req_info.p_struct)
                        pass
                continue
            except:
                self.logger.exception("从 _request_info_will_be_send_queue 获取信息出现异常")

            # self.logger.debug("后台请求队列处理线程 Looping 2")
            try:
                req_info = self._request_info_will_be_send_queue.get(timeout=5)
                # self.logger.debug("后台请求队列处理线程 Looping 2.1")
                func = req_info.func
                func_name = func.__name__
                request_id = req_info.request_id
                request_timeout = req_info.request_timeout
                total_seconds = (datetime.now() - datetime_last_req).total_seconds()
                if total_seconds < max_seconds_4_freq_request:
                    time.sleep(max_seconds_4_freq_request - total_seconds)

                p_struct = req_info.p_struct
                if (datetime.now() - req_info.create_datetime).total_seconds() > request_timeout > 0:
                    # 超时检查主要是为了防止出现队列堵塞结束后，请求集中爆发的情况
                    logging.warning("请求过期[%d]，创建时间：%s 超时时间：%.1f：%s %s ",
                                    request_id, req_info.create_datetime, request_timeout, func_name, p_struct)
                    continue
                self.logger.debug('发送请求[%d] -> %s %s', request_id, func_name, p_struct)
                for n_time in range(1, 4):
                    req_ret = func(p_struct, request_id)
                    self.log_req_if_error(req_ret, func_name=func_name)
                    datetime_now = datetime.now()
                    datetime_last_req = datetime_now
                    if req_ret == 0:
                        req_info.handle_datetime = datetime_now
                        self._request_info_wait_resp_dic[request_id] = req_info
                        break
                    elif req_ret == -3:
                        # -3，表示每秒发送请求数超过许可数
                        # 等待一段时间后，再次发送
                        time.sleep(0.5 * n_time)
                        continue
                    else:
                        break

            except Empty:
                pass
            except:
                self.logger.exception("执行请求失败")
            finally:
                time.sleep(0.1)
                # self.logger.debug("后台请求队列处理线程 Looping 3")
        self.logger.info("%s 后台请求队列处理线程 结束", self.__class__.__name__)

    def _get_response(self, request_id):
        """
        每个request发送后，获得response时，执行此函数，用于处理相关请求队列
        :param request_id: 
        :return: 
        """
        self._request_info_resp_queue.put(request_id)


class StrategyOrder:
    """
    用户记录用户从 ReqOrderInsert, OnRspOrderInsert, 只到 OnRtnTrade 的全部数据信息
    """

    def __init__(self, strategy_id, front_id, session_id, order_ref, input_order):
        """
        用户记录用户从 ReqOrderInsert 请求中的 strategy_id, input_order
        :param strategy_id: 
        :param input_order: 
        """
        self.__strategy_id = strategy_id
        self.front_id = front_id
        self.session_id = session_id
        self.order_ref = order_ref
        self.__input_order = None
        self.trade_list = []
        self.input_order = input_order

    @property
    def strategy_id(self):
        return self.__strategy_id

    @property
    def input_order(self):
        return self.__input_order

    @input_order.setter
    def input_order(self, input_order):
        self.__input_order = input_order
        # with with_mongo_client() as client:
        #     db = client[Config.MONGO_DB_NAME]
        #     collection = db[Config.MONGO_COLLECTION_INPUT_ORDER]
        #     collection.insert_one(input_order)

    def __str__(self):
        return "%s(front_id=%d, session_id=%d, order_ref=%r)" % (
            self.__class__.__name__, self.front_id, self.session_id, self.order_ref)

    __repr__ = __str__


class MyMdApi(MdApi, ApiBase):
    """
    MdApi的本地封装接口
    """

    def __init__(self,
                 instrument_id_list,
                 broker_id=Config.BROKER_ID,
                 user_id=Config.USER_ID,
                 password=Config.PASSWORD):
        ApiBase.__init__(self, broker_id=broker_id,
                         user_id=user_id,
                         password=password)
        self.instrument_id_list = instrument_id_list
        self.front_id = 0
        self.Create()
        self.md_saver_dic = {}
        self.md_min1_combiner_dic = {}
        self.md_minn_combiner_dic = {}
        # 已订阅的合约列表
        self.sub_instrument_list = []
        # self.pub_sub = None  # 似乎已经没用了
        # self._md_handler = {}

    # def register_md_handler(self, name, md_handler):
    #     self._md_handler[name] = md_handler

    # def _handle_depth_md(self, depth_md_dic):
    #     error_name_list = []
    #     for name, handler in self._md_handler.items():
    #         try:
    #             handler(depth_md_dic)
    #             # self.logger.debug('%s data handle finished', name)
    #         except:
    #             self.logger.exception('%s run with error will be del on _md_handler', name)
    #             error_name_list.append(name)
    #     for name in error_name_list:
    #         del self._md_handler[name]
    #         self.logger.warning('从 _md_handler 中移除 %s', name)

    # def insert_depth_md_2_queue(self, depth_md_dic):
    #     """
    #     将Tick 数据推送到 md_tick_saver 的队列，供批量保存使用
    #     将Tick 数据推送到 md_minute_saver 的队列，供生成分钟K线使用
    #     :param depth_md_dic:
    #     :return:
    #     """
    #     # 将Tick 数据推送到 md_tick_saver 的队列，供批量保存使用
    #     self.md_tick_saver.queue_md.put(depth_md_dic)
    #     # 将Tick 数据推送到 md_minute_saver 的队列，供生成分钟K线使用
    #     instrument_id = depth_md_dic['InstrumentID']
    #     md_minute_saver = self.md_minute_saver_dic.setdefault(instrument_id, None)
    #     if md_minute_saver is not None:
    #         md_minute_saver.queue_md.put(depth_md_dic)
    #     else:
    #         self.logger.warning('%s is not on list for minute saver', instrument_id)

    def Create(self):  # , pszFlowPath='', bIsUsingUdp=False, bIsMulticast=False
        dir_path = self.get_tmp_path()
        self.logger.info('cache %s', dir_path)
        return super().Create(dir_path)

    def RegisterFront(self, front=Config.FRONT_MD_ADDRESS):
        self.logger.info('%s', front)
        if isinstance(front, bytes):
            return super().RegisterFront(front)
        for pszFrontAddress in front:
            super().RegisterFront(pszFrontAddress)

    def OnFrontConnected(self):
        self.logger.info('-> ReqUserLogin')
        self.ReqUserLogin()

    def OnFrontDisconnected(self, nReason):
        """
        当客户端与交易后台通信连接断开时，该方法被调用。当发生这个情况后，API会自动重新连接，客户端可不做处理。
        :param nReason: 
        :return: 
        """
        self.logger.warning('API将自动重新连接，客户端可不做处理 reason=%s %s',
                            nReason, FRONT_DISCONNECTED_REASON_DIC.setdefault(nReason, '未知原因'))
        super().OnFrontDisconnected(self, nReason)

    def OnHeartBeatWarning(self, nTimeLapse):
        self.logger.debug('nTimeLapse=%s', nTimeLapse)

    def ReqUserLogin(self):  # , pReqUserLogin, nRequestID
        pReqUserLogin = ApiStruct.ReqUserLogin(BrokerID=self.broker_id, UserID=self.user_id, Password=self.password)
        request_id = self.inc_request_id()
        self.logger.debug('MyMdApi:%d', request_id)
        req_ret = super().ReqUserLogin(pReqUserLogin, request_id)
        self.log_req_if_error(req_ret, stack_num=2)
        return req_ret

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        self.logger.info('%s %s %d %s', pRspUserLogin, pRspInfo, nRequestID, bIsLast)
        if not self.is_rsp_success(pRspInfo):
            return
        self.trading_day = self.GetTradingDay()
        self.logger.info('当前交易日： %s', self.trading_day)
        # 设置 sessionid, frontid
        self.session_id = pRspUserLogin.SessionID
        self.front_id = pRspUserLogin.FrontID
        self.has_login = True

        # 统一注册事件及相应的处理句柄 # 该功能已经被各个对象对应的 register 替代
        # self._register_event()

        # 订阅 md 数据
        self.SubscribeMarketData(self.instrument_id_list)
        self.has_login = True

    # def _register_event(self):
    #     """
    #     统一注册事件及相应的处理句柄
    #     :return:
    #     """
    #     # 注册 md handler
    #     # self.register_md_handler('queue md',
    #     #                          lambda depth_md_dic: self.insert_depth_md_2_queue(depth_md_dic)
    #     #                          )
    #     event_agent.register_handler(EventType.Tick_MD_Event,
    #                                  self.insert_depth_md_2_queue,
    #                                  'queue md')
    #     # tick 数据的publish 可以集成到 md_saver_tick 中，不过出于效率考虑，将其在这里直接发布
    #     # self.register_md_handler('publish md',
    #     #                          lambda depth_md_dic: MdPublisher.publish_md(
    #     #                              Config.REDIS_CHANNEL[PeriodType.Tick],
    #     #                              depth_md_dic)
    #     #                          )
    #     event_agent.register_handler(EventType.Tick_MD_Event,
    #                                  lambda depth_md_dic: MdPublisher.publish_md(
    #                                      Config.REDIS_CHANNEL[PeriodType.Tick],
    #                                      depth_md_dic),
    #                                  'publish md')

    def _start_combiner_and_saver_thread(self):
        """
        统一启动 md sver 线程
        :return: 
        """
        self.logger.info('启动 MdMin1Combiner、MdMinNCombiner')
        instrument_id_list = []
        period_shift_min_set_dic = {PeriodType.Min5: {0},
                                    PeriodType.Min15: {0}
                                    }
        for instrument_id in self.sub_instrument_list:
            # 启动1分钟 combiner
            combiner = MdMin1Combiner(instrument_id)
            combiner.start()
            self.md_min1_combiner_dic[instrument_id] = combiner
            # 启动N分钟 combiner
            combiner = MdMinNCombiner(instrument_id, period_shift_min_set_dic)
            combiner.start()
            self.md_minn_combiner_dic[instrument_id] = combiner
            instrument_id_list.append(instrument_id)
            # self.logger.info('启动 md_minute_saver[%s]', instrument_id)

        self.logger.info('%d 个 MdMin1Combiner、MdMinNCombiner 被启动：\n%s', len(instrument_id_list), instrument_id_list)

        self.logger.info('启动 md saver')
        for event_type in EventType:
            saver = MdSaver.factory(event_type)
            saver.start()
            self.md_saver_dic[event_type] = saver
            publisher = MdPublisher(event_type)

        self.logger.info('启动 md saver')

    def ReqUserLogout(self):
        """
        登出请求
        :return: 
        """
        pUserLogout = ApiStruct.UserLogout(
            BrokerID=Config.BROKER_ID,
            UserID=Config.USER_ID)
        return self._send_request_2_queue(super().ReqUserLogout, pUserLogout)

    def OnRspUserLogout(self, pUserLogout, pRspInfo, nRequestID, bIsLast):
        self.logger.info('(%d)%s', nRequestID, pRspInfo)
        self.md_tick_saver.keep_running = False
        self.logger.info('停止 md_tick_saver')
        for instrument_id, saver in self.md_min1_combiner_dic.items():
            saver.keep_running = False
            self.logger.info('停止 md_minute_saver[%s]', instrument_id)
        self.has_login = False

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        if pSpecificInstrument is None:
            self.logger.warning('(%d)：结果为 None', nRequestID)
            return
        instrument_id = bytes_2_str(pSpecificInstrument.InstrumentID)
        self.sub_instrument_list.append(instrument_id)
        # self.logger.debug('(%d)：%s', nRequestID, pSpecificInstrument)
        if status > 0:
            self.logger.debug('%d个合约订阅行情完成\n%s', len(self.sub_instrument_list), self.sub_instrument_list)
            # 启动 md sver 线程
            self._start_combiner_and_saver_thread()

    def OnRspUnSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        if pSpecificInstrument is None:
            self.logger.info('(%d)：结果为 None', nRequestID)
            return
        self.logger.info('(%d)：%s', nRequestID, pSpecificInstrument)
        if status > 0:
            self.logger.debug('取消订阅行情完成')

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        self.logger.info('(%d)%s', nRequestID, pRspInfo)

    def OnRtnDepthMarketData(self, pDepthMarketData):
        # self.logger.debug(pDepthMarketData)
        # 对无效价格进行一些过滤，否则价格中经常会出现 'AskPrice2': 1.7976931348623157e+308 这样的数字
        upper_limit_price = pDepthMarketData.UpperLimitPrice
        lower_limit_price = pDepthMarketData.LowerLimitPrice
        # Bid
        if pDepthMarketData.BidPrice1 < lower_limit_price or upper_limit_price < pDepthMarketData.BidPrice1:
            # self.logger.debug('set BidPrice1 %f --> 0 ', pDepthMarketData.BidPrice1)
            pDepthMarketData.BidPrice1 = 0
        if pDepthMarketData.BidPrice2 < lower_limit_price or upper_limit_price < pDepthMarketData.BidPrice2:
            # self.logger.debug('set BidPrice2 %f --> 0 ', pDepthMarketData.BidPrice2)
            pDepthMarketData.BidPrice2 = 0
        if pDepthMarketData.BidPrice3 < lower_limit_price or upper_limit_price < pDepthMarketData.BidPrice3:
            # self.logger.debug('set BidPrice3 %f --> 0 ', pDepthMarketData.BidPrice3)
            pDepthMarketData.BidPrice3 = 0
        if pDepthMarketData.BidPrice4 < lower_limit_price or upper_limit_price < pDepthMarketData.BidPrice4:
            # self.logger.debug('set BidPrice4 %f --> 0 ', pDepthMarketData.BidPrice4)
            pDepthMarketData.BidPrice4 = 0
        if pDepthMarketData.BidPrice5 < lower_limit_price or upper_limit_price < pDepthMarketData.BidPrice5:
            # self.logger.debug('set BidPrice5 %f --> 0 ', pDepthMarketData.BidPrice5)
            pDepthMarketData.BidPrice5 = 0
        # Ask
        if pDepthMarketData.AskPrice1 < lower_limit_price or upper_limit_price < pDepthMarketData.AskPrice1:
            # self.logger.debug('set AskPrice1 %f --> 0 ', pDepthMarketData.AskPrice1)
            pDepthMarketData.AskPrice1 = 0
        if pDepthMarketData.AskPrice2 < lower_limit_price or upper_limit_price < pDepthMarketData.AskPrice2:
            # self.logger.debug('set AskPrice2 %f --> 0 ', pDepthMarketData.AskPrice2)
            pDepthMarketData.AskPrice2 = 0
        if pDepthMarketData.AskPrice3 < lower_limit_price or upper_limit_price < pDepthMarketData.AskPrice3:
            # self.logger.debug('set AskPrice3 %f --> 0 ', pDepthMarketData.AskPrice3)
            pDepthMarketData.AskPrice3 = 0
        if pDepthMarketData.AskPrice4 < lower_limit_price or upper_limit_price < pDepthMarketData.AskPrice4:
            # self.logger.debug('set AskPrice4 %f --> 0 ', pDepthMarketData.AskPrice4)
            pDepthMarketData.AskPrice4 = 0
        if pDepthMarketData.AskPrice5 < lower_limit_price or upper_limit_price < pDepthMarketData.AskPrice5:
            # self.logger.debug('set AskPrice4 %f --> 0 ', pDepthMarketData.AskPrice5)
            pDepthMarketData.AskPrice5 = 0
        # Others
        if pDepthMarketData.AveragePrice < lower_limit_price or upper_limit_price < pDepthMarketData.AveragePrice:
            # self.logger.debug('set AveragePrice %f --> 0 ', pDepthMarketData.AveragePrice)
            pDepthMarketData.AveragePrice = 0
        if pDepthMarketData.SettlementPrice < lower_limit_price or upper_limit_price < pDepthMarketData.SettlementPrice:
            # self.logger.debug('set SettlementPrice %f --> 0 ', pDepthMarketData.SettlementPrice)
            pDepthMarketData.SettlementPrice = 0
        if pDepthMarketData.ClosePrice < lower_limit_price or upper_limit_price < pDepthMarketData.ClosePrice:
            # self.logger.debug('set ClosePrice %f --> 0 ', pDepthMarketData.ClosePrice)
            pDepthMarketData.ClosePrice = 0
        if pDepthMarketData.OpenPrice < lower_limit_price or upper_limit_price < pDepthMarketData.OpenPrice:
            # self.logger.debug('set OpenPrice %f --> 0 ', pDepthMarketData.OpenPrice)
            pDepthMarketData.OpenPrice = 0
        if pDepthMarketData.HighestPrice < lower_limit_price or upper_limit_price < pDepthMarketData.HighestPrice:
            # self.logger.debug('set HighestPrice %f --> 0 ', pDepthMarketData.HighestPrice)
            pDepthMarketData.HighestPrice = 0
        if pDepthMarketData.LowestPrice < lower_limit_price or upper_limit_price < pDepthMarketData.LowestPrice:
            # self.logger.debug('set LowestPrice %f --> 0 ', pDepthMarketData.LowestPrice)
            pDepthMarketData.LowestPrice = 0
        # 对异常数据进行归 0 处理
        if pDepthMarketData.PreClosePrice < TOO_SMALL_TO_AVAILABLE or TOO_LARGE_TO_AVAILABLE < pDepthMarketData.PreClosePrice:
            pDepthMarketData.PreClosePrice = 0
        if pDepthMarketData.Turnover < TOO_SMALL_TO_AVAILABLE or TOO_LARGE_TO_AVAILABLE < pDepthMarketData.Turnover:
            pDepthMarketData.Turnover = 0
        if pDepthMarketData.PreSettlementPrice < TOO_SMALL_TO_AVAILABLE or TOO_LARGE_TO_AVAILABLE < pDepthMarketData.PreSettlementPrice:
            pDepthMarketData.PreSettlementPrice = 0
        if pDepthMarketData.OpenInterest < TOO_SMALL_TO_AVAILABLE or TOO_LARGE_TO_AVAILABLE < pDepthMarketData.OpenInterest:
            pDepthMarketData.OpenInterest = 0
        if pDepthMarketData.LastPrice < TOO_SMALL_TO_AVAILABLE or TOO_LARGE_TO_AVAILABLE < pDepthMarketData.LastPrice:
            pDepthMarketData.LastPrice = 0
        if pDepthMarketData.PreOpenInterest < TOO_SMALL_TO_AVAILABLE or TOO_LARGE_TO_AVAILABLE < pDepthMarketData.PreOpenInterest:
            pDepthMarketData.PreOpenInterest = 0

        pDepthMarketData.PreDelta = 0
        pDepthMarketData.CurrDelta = 0

        depth_md_dic = ApiBase.struct_2_dic(pDepthMarketData)
        instrument_id = bytes_2_str(pDepthMarketData.InstrumentID)
        trading_day_str = bytes_2_str(pDepthMarketData.TradingDay)
        action_day_str = bytes_2_str(pDepthMarketData.ActionDay)
        # datetime.strptime(trading_day_str, '%Y%m%d').date()
        depth_md_dic['TradingDay'] = '-'.join([trading_day_str[:4], trading_day_str[4:6], trading_day_str[6:]])
        # datetime.strptime(action_day_str, '%Y%m%d').date()
        # 现在默认 ActionDay为当前服务器日期
        exch_datetime = Config.exch_datetime
        depth_md_dic['ActionDay'] = exch_datetime.strftime(Config.DATE_FORMAT_STR)
        depth_md_dic['ActionTime'] = exch_datetime.strftime(Config.TIME_FORMAT_STR)
        depth_md_dic['ActionMillisec'] = exch_datetime.microsecond
        depth_md_dic['UpdateTime_Sec_ms'] = float(
            pDepthMarketData.UpdateTime[6:]) + pDepthMarketData.UpdateMillisec / 1000
        # self.logger.debug(depth_md_dic)
        # depth_md_dic 需要被 json ，因此，无法直接处理datetime类型
        # action_datetime = action_day_str + ' ' + depth_md_dic['UpdateTime'] + '.' + str(pDepthMarketData.UpdateMillisec)
        # depth_md_dic['ActionDateTime'] = action_datetime  # datetime.strptime(action_datetime, '%Y%m%d %H:%M:%S.%f')

        # 通过注册器模式将 md 处理方法进行解耦、封装
        # self._handle_depth_md(depth_md_dic)
        event_agent.send_event(EventType.Tick_MD_Event, depth_md_dic, key=instrument_id)
        # 数据推入 md_saver 的 queue
        # self.md_saver.queue_md.put(depth_md_dic)
        # publish md data
        # r = Config.get_redis()
        # md_str = json.dumps(depth_md_dic)
        # r.publish(Config.REDIS_CHANNEL_HEAD_MD, md_str)

    def Release(self):
        """
        删除接口对象本身 释放资源
        :return: 
        """
        try:
            Config.release()
        finally:
            super().Release()


class MyTraderApi(TraderApi, ApiBase):
    """
        TraderApi的本地封装接口
    """

    def __init__(self, broker_id=Config.BROKER_ID, user_id=Config.USER_ID,
                 investor_id=Config.INVESTOR_ID, password=Config.PASSWORD,
                 **kwargs
                 ):
        ApiBase.__init__(self, broker_id=broker_id,
                         user_id=user_id,
                         password=password)
        self.investor_id = investor_id
        # 结算单信息，内容部分
        self.settlement_info_content = []
        self._order_ref_sn = 0  # 最大报单引用, char[13]
        self._mutex_order_ref = threading.Lock()
        # 供报单请求回报 OnRspOrderInsert 使用
        self.order_ref_strategy_order_dic = {}
        # 结算单是否已确认
        self._is_settlement_info_confirmed = False
        # 设置 strategy_id
        # TODO: strategy_id 需要通过读取配置信息初始化
        self.strategy_id = 0

        # 持仓信息
        # TODO: _investor_position_dic 用于保存 ReqQryInvestorPosition 返回的持仓信息，该信息将会在登录初始化，发生交易回报等事件后自动更新
        # _investor_position_dic 用于保存 ReqQryInvestorPosition 返回的持仓信息，该信息将会在登录初始化，发生交易回报等事件后自动更新
        self._instrument_investor_position_dic = {}
        # 持仓信息（临时记录）
        self._instrument_investor_position_tmp_dic = {}
        # 订单信息
        self._instrument_order_dic = {}
        # 订单信息（临时记录）
        self._instrument_order_tmp_dic = {}
        # 合约信息
        self._instrument_info_dic = {}

        # 是否已经更新合约信息
        self.has_update_instruemnt = False
        # 登录成功后自动更新合约信息
        self.auto_update_instrument_list = kwargs.setdefault('auto_update_instrument_list', False)
        # 交易回报后自动更新仓位信息
        self.update_position_after_rtn_trade = True
        # 交易回报后自动更新仓位信息
        self.update_order_after_rtn_trade = True

        # 定时更新仓位信息
        self.timedelta_update_position = True
        # 最近一次 更新仓位信息的时间
        self.datetime_last_update_position = None
        self.datetime_last_update_position_dic = {}
        # 最近一次 发送 order 请求的时间
        self.datetime_last_send_order_dic = {}
        # 最近一次 ret_trade 更新时间
        self.datetime_last_rtn_trade_dic = {}

        # 设置后台线程
        self.enable_background_thread = kwargs.setdefault('enable_background_thread', False)
        self.background_thread_working = False
        self.background_thread = Thread(target=self._time_interval_job, daemon=True)
        self.second_of_interval = 10
        self.auto_update_position = kwargs.setdefault('auto_update_position', False)

        self.Create()

    def Create(self):  # , pszFlowPath=''
        """
        创建TraderApi
        :return: 
        """
        dir_path = self.get_tmp_path()
        self.logger.info('cache %s', dir_path)
        return super().Create(dir_path)

    def RegisterFront(self, front=Config.FRONT_TRADER_ADDRESS):
        """
        注册前置机网络地址
        :param front: 
        :return: 
        """
        self.logger.info('%s', front)
        if isinstance(front, bytes):
            return super().RegisterFront(front)
        for pszFrontAddress in front:
            super().RegisterFront(pszFrontAddress)

    # 交易初始化

    def _time_interval_job(self):
        """
        综合处理各种后台更新动作
        :return: 
        """
        self.logger.info("%s 后台线程 启动", self.__class__.__name__)
        has_send_req_qry_instrument = False
        has_send_req_qry_investor_position = False
        timedelta_interval = timedelta(seconds=self.second_of_interval)
        while self.background_thread_working:
            try:
                if (not has_send_req_qry_instrument) and self.auto_update_instrument_list:
                    self.logger.info("集中请求全部合约信息")
                    self.ReqQryInstrument()
                    has_send_req_qry_instrument = True
                if self.auto_update_position and ((not self.auto_update_instrument_list) or self.has_update_instruemnt):
                    if self.datetime_last_update_position is None or self.datetime_last_update_position + timedelta_interval < datetime.now():
                        self.logger.debug("定时更新持仓信息")
                        self.ReqQryInvestorPosition()
                        has_send_req_qry_investor_position = True
            except:
                self.logger.exception('后台线程任务失败')
            finally:
                time.sleep(self.second_of_interval)
        self.logger.info("%s 后台线程 结束", self.__class__.__name__)

    @property
    def is_settlement_info_confirmed(self):
        return self._is_settlement_info_confirmed

    @is_settlement_info_confirmed.setter
    def is_settlement_info_confirmed(self, val):
        """
        当结算信息被首次确认，调用 ReqQryInstrument 查询全部可交易合约的信息并更新配置信息
        :param val: 
        :return: 
        """
        if self._is_settlement_info_confirmed != val:
            self._is_settlement_info_confirmed = val

    def OnFrontConnected(self):
        '''
            当客户端与交易后台建立起通信连接时（还未登录前），该方法被调用。
        '''
        super().OnFrontConnected()
        self.logger.info('-> ReqUserLogin')
        self.ReqUserLogin()

    def OnFrontDisconnected(self, nReason):
        """
        当客户端与交易后台通信连接断开时，该方法被调用。当发生这个情况后，API会自动重新连接，客户端可不做处理。
        :param nReason: 
        :return: 
        """
        self.logger.warning('API将自动重新连接，客户端可不做处理 reason=%s %s',
                            nReason, FRONT_DISCONNECTED_REASON_DIC.setdefault(nReason, '未知原因'))
        super().OnFrontDisconnected(nReason)

    def ReqUserLogin(self):
        """
        用户登录请求
        :return: 
        """
        pReqUserLogin = ApiStruct.ReqUserLogin(
            BrokerID=Config.BROKER_ID,
            UserID=Config.USER_ID,
            Password=Config.PASSWORD)
        request_id = self.inc_request_id()
        self.logger.debug('TraderApi:%d', request_id)
        req_ret = super().ReqUserLogin(pReqUserLogin, request_id)
        self.log_req_if_error(req_ret, stack_num=2)
        return req_ret

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """
        登录请求响应
        :param pRspUserLogin: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        super().OnRspUserLogin(pRspUserLogin, pRspInfo, nRequestID, bIsLast)
        # 获取当前时间，用于后面与交易所时间同步使用
        datetime_now = datetime.now()
        self.logger.info('%s %s %d %s', pRspUserLogin, pRspInfo, nRequestID, bIsLast)
        if not self.is_rsp_success(pRspInfo):
            return
        # 由于 get_inc_order_ref 需要， trading_day 需要固定长度为 8
        self.trading_day = bytes_2_str(self.GetTradingDay())
        if len(self.trading_day) != 8:
            raise ValueError('当前交易日期 trading_day %s 必须长度为8' % self.trading_day)
        self.logger.info('当前交易日期: %s', self.trading_day)
        # 设置 sessionid, frontid
        self.session_id = pRspUserLogin.SessionID
        self.front_id = pRspUserLogin.FrontID
        # 设置上一次登录时的最大报单编号
        MaxOrderRef = pRspUserLogin.MaxOrderRef
        if MaxOrderRef is None or len(MaxOrderRef) == 0:
            self.logger.warning('MaxOrderRef 无效，将被重置为 1', )
            self._order_ref_sn = 1
        else:
            self._order_ref_sn = int(MaxOrderRef)
        self.logger.debug('上一次登录 MaxOrderRef = %d', self._order_ref_sn)

        # 设置各个交易所与本地的时间差
        date_str = datetime_now.strftime(Config.DATE_FORMAT_STR)
        timedelta_default = None
        # 上期所时间
        is_shfe_timedelta_error = False
        if len(pRspUserLogin.SHFETime) == 8:
            try:
                Config.SHFETimeDelta = datetime.strptime(date_str + ' ' + bytes_2_str(pRspUserLogin.SHFETime),
                                                         Config.DATETIME_FORMAT_STR) - datetime_now
                self.logger.info(
                    '比上期所 SHFE（%s）慢 %f 秒', pRspUserLogin.SHFETime, Config.SHFETimeDelta.total_seconds())
                if timedelta_default is None:
                    timedelta_default = Config.SHFETimeDelta
            except:
                is_shfe_timedelta_error = True
                self.logger.error('上期所 SHFE 时间解析错误，使用默认时差')
                Config.SHFETimeDelta = timedelta()
        # 大商所时间
        is_dce_timedelta_error = False
        if len(pRspUserLogin.DCETime) == 8:
            try:
                Config.DCETimeDelta = datetime.strptime(date_str + ' ' + bytes_2_str(pRspUserLogin.DCETime),
                                                        Config.DATETIME_FORMAT_STR) - datetime_now
                self.logger.info(
                    '比大商所 DCE（%s）慢 %f 秒', pRspUserLogin.DCETime, Config.DCETimeDelta.total_seconds())
                if timedelta_default is None:
                    timedelta_default = Config.DCETimeDelta
            except:
                is_dce_timedelta_error = True
                self.logger.error('大商所 DCE 时间解析错误，使用默认时差')
                Config.DCETimeDelta = timedelta()
        # 郑商所时间
        is_czce_timedelta_error = False
        if len(pRspUserLogin.CZCETime) == 8:
            try:
                Config.CZCETimeDelta = datetime.strptime(date_str + ' ' + bytes_2_str(pRspUserLogin.CZCETime),
                                                         Config.DATETIME_FORMAT_STR) - datetime_now
                self.logger.info(
                    '比郑商所 CZCE（%s）慢 %f 秒', pRspUserLogin.CZCETime, Config.CZCETimeDelta.total_seconds())
                if timedelta_default is None:
                    timedelta_default = Config.CZCETimeDelta
            except:
                is_czce_timedelta_error = True
                self.logger.error('郑商所 CZCE 时间解析错误，使用默认时差')
                Config.CZCETimeDelta = timedelta()
        # 中金所时间
        is_ffex_timedelta_error = False
        if len(pRspUserLogin.FFEXTime) == 8:
            try:
                Config.FFEXTimeDelta = datetime.strptime(date_str + ' ' + bytes_2_str(pRspUserLogin.FFEXTime),
                                                         Config.DATETIME_FORMAT_STR) - datetime_now
                self.logger.info(
                    '比中金所 FFEX（%s）慢 %f 秒', pRspUserLogin.FFEXTime, Config.FFEXTimeDelta.total_seconds())
                if timedelta_default is None:
                    timedelta_default = Config.FFEXTimeDelta
            except:
                is_ffex_timedelta_error = True
                self.logger.error('中金所 FFEX 时间解析错误，使用默认时差')
                Config.FFEXTimeDelta = timedelta()
        # 能源中心时间
        is_ine_timedelta_error = False
        if len(pRspUserLogin.INETime) == 8:
            try:
                Config.INETimeDelta = datetime.strptime(date_str + ' ' + bytes_2_str(pRspUserLogin.INETime),
                                                        Config.DATETIME_FORMAT_STR) - datetime_now
                self.logger.info(
                    '比能源中心 INE（%s）慢 %f 秒', pRspUserLogin.INETime, Config.INETimeDelta.total_seconds())
                if timedelta_default is None:
                    timedelta_default = Config.INETimeDelta
            except:
                is_ine_timedelta_error = True
                self.logger.error('能源中心 INE 时间解析错误，使用默认时差')
                Config.INETimeDelta = timedelta()
        # 处理异常时差
        if timedelta_default is not None:
            if is_shfe_timedelta_error:
                Config.SHFETimeDelta = timedelta_default
            if is_dce_timedelta_error:
                Config.DCETimeDelta = timedelta_default
            if is_czce_timedelta_error:
                Config.CZCETimeDelta = timedelta_default
            if is_ffex_timedelta_error:
                Config.FFEXTimeDelta = timedelta_default
            if is_ine_timedelta_error:
                Config.INETimeDelta = timedelta_default

        # 查询结算单日期
        self.ReqQrySettlementInfoConfirm()
        # req_ret = self.ReqQrySettlementInfo()
        # self.ReqSettlementInfoConfirm()

        # 查询合约
        # self.ReqQryInstrument(str_2_bytes('rb17'))
        # 查询合约保证金率
        # self.ReqQryInstrumentMarginRate(str_2_bytes('rb1712'))
        # 查询资金账户
        # self.ReqQryTradingAccount()
        self.has_login = True
        if self.enable_background_thread and not self.background_thread.is_alive():
            self.background_thread_working = True
            self.background_thread.start()

    def ReqUserLogout(self):
        """
        登出请求
        :return: 
        """
        pUserLogout = ApiStruct.UserLogout(
            BrokerID=Config.BROKER_ID,
            UserID=Config.USER_ID)
        return self._send_request_2_queue(super().ReqUserLogout, pUserLogout)

    def OnRspUserLogout(self, pUserLogout, pRspInfo, nRequestID, bIsLast):
        '''
        登出请求响应
        :param pUserLogout: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        '''
        TraderApi.OnRspUserLogout(self, pUserLogout, pRspInfo, nRequestID, bIsLast)
        if not self.is_rsp_success(pRspInfo):
            return
        self.logger.info('%s %s %d %s', pUserLogout, pRspInfo, nRequestID, bIsLast)
        self.has_login = False
        self.background_thread_working = False

    def ReqQrySettlementInfoConfirm(self):
        """
        查询结算单确认的日期
        :return: 
        """
        self.logger.info("请求查询结算单确认的日期")
        pQrySettlementInfoConfirm = ApiStruct.QrySettlementInfoConfirm(
            BrokerID=self.broker_id, InvestorID=self.investor_id)
        return self._send_request_2_queue(super().ReqQrySettlementInfoConfirm, pQrySettlementInfoConfirm, request_timeout=0)

    def OnRspQrySettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        """
        查询结算单确认的日期响应
        :param pSettlementInfoConfirm: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        self.logger.info('查询结算单确认的日期响应(%d)：%s', nRequestID, pSettlementInfoConfirm)

        is_need_confirmed = True
        confirm_date = ""
        if pSettlementInfoConfirm is None or type(pSettlementInfoConfirm) == ApiStruct.QrySettlementInfoConfirm:
            pass
        else:
            confirm_date = bytes_2_str(pSettlementInfoConfirm.ConfirmDate)
            is_need_confirmed = int(confirm_date) < int(self.trading_day)
        # 进行结算单确认
        if is_need_confirmed:
            self.logger.info('最新结算单未确认，需查询后再确认,最后确认时间=%s，当前日期:%s',
                             confirm_date, self.trading_day)
            # 此处连续请求可能导致错误 -3 每秒发送请求数超过许可数
            time.sleep(1)
            req = self.ReqQrySettlementInfo()
            # -1，表示网络连接失败；
            # -2，表示未处理请求超过许可数；
            # -3，表示每秒发送请求数超过许可数。
            if req < 0:
                self.logger.error('-> ReqQrySettlementInfo(%d) 请求失败', nRequestID)
        else:
            self.logger.info('最新结算单已确认，不需再次确认,最后确认时间=%s，当前日期:%s',
                             confirm_date, self.trading_day)
            self.is_settlement_info_confirmed = True

    def ReqQrySettlementInfo(self):
        """
        请求查询投资者结算结果
        :return: 
        """
        self.logger.info("请求查询投资者结算结果")
        pQrySettlementInfo = ApiStruct.QrySettlementInfo(BrokerID=self.broker_id, InvestorID=self.investor_id)
        self.settlement_info_content = []
        return self._send_request_2_queue(super().ReqQrySettlementInfo, pQrySettlementInfo, request_timeout=0)

    def OnRspQrySettlementInfo(self, pSettlementInfo, pRspInfo, nRequestID, bIsLast):
        """
        请求查询投资者结算信息响应
        :param pSettlementInfo: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        settlement_id = pSettlementInfo.SettlementID
        trading_day_settlement = bytes_2_str(pSettlementInfo.TradingDay)
        # content = str_2_bytes(pSettlementInfo.Content)
        # self.logger.info('结算单[%d]内容：%s', SettlementID, content)
        self.settlement_info_content.append(pSettlementInfo.Content)
        if status > 0:
            content_all = bytes_2_str(b''.join(self.settlement_info_content))
            self.logger.info('%s 结算单内容：\n%s', trading_day_settlement, content_all)
            if trading_day_settlement is None or trading_day_settlement == "":
                self.logger.warning('结算单日期为空，替换为当前交易日%s', self.trading_day)
                trading_day_settlement = self.trading_day
            file_name = 'Settlement_%d_%s.txt' % (settlement_id, trading_day_settlement)
            try:
                with open(file_name, mode='x') as file_tmp:
                    file_tmp.write(content_all)
            except:
                self.logger.exception('结算单文件 %s 保存失败', file_name)
            self.logger.debug('查询结算单完成')
            self.settlement_info_content = content_all
            # 执行结算单确认
            self.ReqSettlementInfoConfirm()

    def ReqSettlementInfoConfirm(self):
        """
        投资者结算结果确认
        :return: 
        """
        self.logger.info("投资者结算结果确认")
        pSettlementInfoConfirm = ApiStruct.SettlementInfoConfirm(BrokerID=self.broker_id, InvestorID=self.investor_id)
        return self._send_request_2_queue(super().ReqSettlementInfoConfirm, pSettlementInfoConfirm, request_timeout=0)

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        """
        投资者结算结果确认响应
        :param pSettlementInfoConfirm: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        ConfirmDate = bytes_2_str(pSettlementInfoConfirm.ConfirmDate)
        self.logger.info(
            '结算单确认时间: %s %s', ConfirmDate, bytes_2_str(pSettlementInfoConfirm.ConfirmTime))
        if ConfirmDate == self.trading_day:
            self.logger.info('最新结算确认日期 %s 确认成功', ConfirmDate)
            self.is_settlement_info_confirmed = True

    def Release(self):
        """
        删除接口对象本身 释放资源
        :return: 
        """
        try:
            Config.release()
            if self.background_thread.is_alive():
                self.background_thread_working = False
                self.background_thread.join(self.second_of_interval)
        finally:
            super().Release()

    # 交易准备、交易查询
    def inc_order_ref(self):
        """
        自动生成自增报单编号，长度 char[13]
        :return: 
        """
        self._mutex_order_ref.acquire()
        self._order_ref_sn += 1
        order_ref = str_2_bytes(str(self._order_ref_sn))
        self._mutex_order_ref.release()
        return order_ref

    def inc_order_action_ref(self):
        """
        自动生成自增报单编号，长度 int
        :return: 
        """
        self._mutex_order_ref.acquire()
        self._order_ref_sn += 1
        order_ref_int = self._order_ref_sn
        self._mutex_order_ref.release()
        return order_ref_int

    def ReqQryInstrumentMarginRate(self, instrument_id=b''):
        """
        请求查询合约保证金率
        :return: 
        """
        pQryInstrumentMarginRate = ApiStruct.QryInstrumentMarginRate(self.broker_id, self.investor_id, instrument_id)
        return self._send_request_2_queue(super().ReqQryInstrumentMarginRate, pQryInstrumentMarginRate, request_timeout=0)

    def OnRspQryInstrumentMarginRate(self, pInstrumentMarginRate, pRspInfo, nRequestID, bIsLast):
        """
        请求查询合约保证金率响应，保证金率回报。返回的必然是绝对值
        :param pInstrumentMarginRate: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        # self.logger.info('合约保证金率：%s', pInstrumentMarginRate)
        if status > 0:
            self.logger.debug('查询合约保证金率完成')

    def ReqQryInstrument(self, instrument_id=b'', exchange_id=b'', exchange_inst_id=b'', product_id=b''):
        """
        请求查询合约
        :param instrument_id: 
        :param exchange_id: 
        :param exchange_inst_id: 
        :param product_id: 
        :return: 
        """
        pQryInstrument = ApiStruct.QryInstrument(InstrumentID=instrument_id,
                                                 ExchangeID=exchange_id,
                                                 ExchangeInstID=exchange_inst_id,
                                                 ProductID=product_id)
        self.logger.debug("查询合约 %s", pQryInstrument)
        return self._send_request_2_queue(super().ReqQryInstrument, pQryInstrument, request_timeout=0, max_wait_rsp_time=30)

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        """
        请求查询合约响应
        :param pInstrument: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        # self.logger.debug('(%d)pInstrument=%s', nRequestID, pInstrument)
        if pInstrument.UnderlyingMultiple > 100000:
            pInstrument.UnderlyingMultiple = 0
        if pInstrument.StrikePrice > 100000:
            pInstrument.StrikePrice = 0
        if pInstrument.LongMarginRatio > 100000:
            pInstrument.LongMarginRatio = 0
        if pInstrument.ShortMarginRatio > 100000:
            pInstrument.ShortMarginRatio = 0
        instrument_dic = ApiBase.struct_2_dic(pInstrument,
                                              {
                                                  "StartDelivDate": date,
                                                  "EndDelivDate": date,
                                                  'ExpireDate': date,
                                               })
        # 清理垃圾数据
        for name, val in instrument_dic.items():
            if val == "":
                instrument_dic[name] = None
        # self.logger.debug("%s: %s", instrument_dic['InstrumentID'], instrument_dic)
        self._instrument_info_dic[instrument_dic['InstrumentID']] = instrument_dic
        if status > 0:
            self.logger.debug('查询合约完成。 %d条合约信息加载完成', len(Config.instrument_info_dic))
            try:
                Config.update_instrument(self._instrument_info_dic)
            except:
                self.logger.exception("update_instrument_info")
            finally:
                self.has_update_instruemnt = True
                # self.ReqQryInstrumentCommissionRate(pInstrument.InstrumentID)
                # self._req_qry_instrument_commission_rate_all()

    def req_qry_instrument_commission_rate_all(self):
        """
        更新全部合约的手续费信息
        :return: 
        """
        self.logger.info("开始更新合约手续费数据，当前数据库中包含合约 %d 条", len(Config.instrument_info_dic))
        re_pattern_instrument_header = re.compile(r'[A-Za-z]+(?=\d+$)')
        query_dic = OrderedDict()
        for n, instrument_id in enumerate(Config.instrument_info_dic.keys()):
            m = re_pattern_instrument_header.match(instrument_id)
            if m is None:
                # self.logger.warning("%d) %s 没有找到有效的合约头部字母", n, instrument_id)
                continue
            instrument_header = m.group()
            if instrument_header not in query_dic:
                query_dic[instrument_header] = instrument_id
        self.logger.info('%d 条合约手续费信息将被请求更新\n%s', len(query_dic), query_dic)
        # new_added_instrument_id_list = []
        for instrument_header, instrument_id in query_dic.items():
            if instrument_header in Config.instrument_commission_rate_dic:
                continue
            time.sleep(1)
            self.ReqQryInstrumentCommissionRate(str_2_bytes(instrument_id))
            # 设置延时等待，直到获得相关合约费用信息
            for n in range(8):
                second = 0.2 * (2 ** n)
                time.sleep(second)
                self.logger.debug('wait %.1f', second)
                if instrument_header in Config.instrument_commission_rate_dic:
                    # new_added_instrument_id_list.append(instrument_header)
                    break
                    # 服务器更新相关费用数据
                    # try:
                    #     Config.update_instrument_commission_rate(new_added_instrument_id_list)
                    # except:
                    #     self.logger.exception("update_instrument_commission_rate")

                    # 单合约调试使用
                    # instrument_id = 'SPD TA711&TA806'
                    # self.ReqQryInstrumentCommissionRate(str_2_bytes(instrument_id))

    def ReqQryInstrumentCommissionRate(self, instrument_id=b''):
        """
        请求查询合约手续费率
        :param instrument_id: 
        :return: 
        """
        pQryInstrumentCommissionRate = ApiStruct.QryInstrumentCommissionRate(BrokerID=Config.BROKER_ID,
                                                                             InvestorID=Config.INVESTOR_ID,
                                                                             InstrumentID=instrument_id,
                                                                             )
        return self._send_request_2_queue(super().ReqQryInstrumentCommissionRate, pQryInstrumentCommissionRate,
                                          request_timeout=0)

    def OnRspQryInstrumentCommissionRate(self, pInstrumentCommissionRate, pRspInfo, nRequestID, bIsLast):
        """
        请求查询合约手续费率响应
        :param pInstrumentCommissionRate: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        self.logger.debug('(%d)pInstrumentCommissionRate=%s', nRequestID, pInstrumentCommissionRate)
        if pInstrumentCommissionRate is not None:
            instrument_commission_rate_dic = ApiBase.struct_2_dic(pInstrumentCommissionRate)
            # 清理垃圾数据
            for name, val in instrument_commission_rate_dic.items():
                if val == "":
                    instrument_commission_rate_dic[name] = None
            Config.instrument_commission_rate_dic[
                instrument_commission_rate_dic['InstrumentID']] = instrument_commission_rate_dic
            # 更新合约手续费信息到数据库
            try:
                CommissionRateModel.merge_by_commission_rate_dic(instrument_commission_rate_dic)
            except:
                self.logger.exception('更新合约手续费信息失败')
        if status > 0:
            self.logger.debug('查询合约手续费率完成。 已累计查询 %d 条合约手续费率信息', len(Config.instrument_commission_rate_dic))

    def ReqQryTradingAccount(self):
        """
        请求查询资金账户
        :return: 
        """
        pQryTradingAccount = ApiStruct.QryTradingAccount(self.broker_id, self.investor_id)
        return self._send_request_2_queue(super().ReqQryTradingAccount, pQryTradingAccount, request_timeout=0)

    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        """
        请求查询资金账户响应
        :param pTradingAccount: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        self.logger.info('资金账户:%s', pTradingAccount)
        if status > 0:
            self.logger.info('查询资金账户完成')

    def ReqQryInvestorPosition(self, instrument_id=b"", refresh=False):
        """
        请求查询投资者持仓
        :param instrument_id: 
        :return: 
        """
        pQryInvestorPosition = ApiStruct.QryInvestorPosition(self.broker_id, self.investor_id, instrument_id)
        if instrument_id != b"":
            instrument_id_str = bytes_2_str(instrument_id)
            if instrument_id_str in self._instrument_investor_position_tmp_dic:
                self._instrument_investor_position_tmp_dic = self._instrument_investor_position_dic.copy()
                del self._instrument_investor_position_tmp_dic[instrument_id_str]
            else:
                self._instrument_investor_position_tmp_dic = self._instrument_investor_position_dic
        else:
            self._instrument_investor_position_tmp_dic = {}
        return self._send_request_2_queue(super().ReqQryInvestorPosition, pQryInvestorPosition, request_timeout=0)

    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        """
        请求查询投资者持仓响应
        :param pInvestorPosition: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        self.logger.info('持仓:%s', pInvestorPosition)
        if pInvestorPosition is None:
            return
        instrument_id_str = bytes_2_str(pInvestorPosition.InstrumentID)
        data_dic = ApiBase.struct_2_dic(pInvestorPosition)
        # 增加仓位最近刷新时间
        data_dic["RefreshDateTime"] = datetime.now()
        position_date = PositionDateType.create_by_position_date(pInvestorPosition.PositionDate)
        self._instrument_investor_position_tmp_dic.setdefault(instrument_id_str, {})[position_date] = data_dic
        self.datetime_last_update_position_dic[instrument_id_str] = datetime.now()
        if status > 0:
            self._instrument_investor_position_dic = self._instrument_investor_position_tmp_dic
            self.logger.info('查询持仓完成')
            self.datetime_last_update_position = datetime.now()

    def ReqQryInvestorPositionDetail(self, instrument_id=b""):
        """
        请求查询投资者持仓明细
        :param instrument_id: 
        :return: 
        """
        p_struct = ApiStruct.QryInvestorPositionDetail(self.broker_id, self.investor_id, instrument_id)
        return self._send_request_2_queue(super().ReqQryInvestorPositionDetail, p_struct)

    def OnRspQryInvestorPositionDetail(self, pInvestorPositionDetail, pRspInfo, nRequestID, bIsLast):
        """请求查询投资者持仓明细响应"""
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        self.logger.info('持仓明细:%s', pInvestorPositionDetail)
        if status > 0:
            self.logger.info('查询持仓明细完成')

    def OnRspError(self, info, RequestId, IsLast):
        """
        错误应答
        :param info: 
        :param RequestId: 
        :param IsLast: 
        :return: 
        """
        if IsLast:
            self.logger.info('错误应答(%d):%s', RequestId, info)
            self.logger.info('接收错误应答完成')
        else:
            self.logger.info('错误应答(%d):%s', RequestId, info)

    def ReqQryOrder(self, instrument_id=b""):
        self._instrument_order_tmp_dic = {}
        p_struct = ApiStruct.QryOrder(self.broker_id, self.investor_id, InstrumentID=instrument_id)
        if instrument_id != b"":
            instrument_id_str = bytes_2_str(instrument_id)
            if instrument_id_str in self._instrument_order_tmp_dic:
                self._instrument_order_tmp_dic = self._instrument_order_dic.copy()
                del self._instrument_order_tmp_dic[instrument_id_str]
            else:
                self._instrument_order_tmp_dic = self._instrument_order_dic
        else:
            self._instrument_order_tmp_dic = {}
        return self._send_request_2_queue(super().ReqQryOrder, p_struct, request_timeout=0)

    def OnRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast):
        """
        请求查询报单响应
        :param pOrder: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        self.logger.info('报单：%s', pOrder)
        data_dic = ApiBase.struct_2_dic(pOrder)
        instrument_id_str = bytes_2_str(pOrder.InstrumentID)
        self._instrument_order_tmp_dic.setdefault(instrument_id_str, {})[pOrder.SequenceNo] = data_dic
        if status > 0:
            # 为了避免出现加载查询结果过程中，其他现场查询订单引发的信息错误，利用 self._order_tmp_dic 进行临时信息存储
            self._instrument_order_dic = self._instrument_order_tmp_dic
            self.logger.info('查询报单完成。 %s', {key: len(val) for key, val in self._instrument_order_dic.items()})

    def OnRspQryTrade(self, pTrade, pRspInfo, nRequestID, bIsLast):
        """
        请求查询成交响应
        :param pTrade: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        self.logger.info('成交:%s', pTrade)
        if status > 0:
            self.logger.info('查询成交完成')

    # 供TradeAgent交易接口使用
    def get_position(self, instrument_id) -> dict:
        """ 供 TradeAgent get_position 使用"""
        if instrument_id in self._instrument_investor_position_dic:
            position_date_inv_pos_dic = self._instrument_investor_position_dic[instrument_id]
        else:
            position_date_inv_pos_dic = None
        return position_date_inv_pos_dic

    def get_order(self, instrument_id) -> dict:
        """ 供 TradeAgent get_order 使用"""
        if instrument_id in self._instrument_order_dic:
            orders_dic = self._instrument_order_dic[instrument_id]
        else:
            orders_dic = None
        return orders_dic

    def cancel_order(self, instrument_id):
        """供 TradeAgent cancel_order 使用"""
        if instrument_id in self._instrument_order_dic:
            orders_dic = self._instrument_order_dic[instrument_id]
            for seq_no, order_dic in orders_dic.items():
                # if order_dic['VolumeTotal'] > 0:
                self.ReqOrderAction(order_dic)

    # 交易操作
    def insert_struct_2_db(self, p_struct):
        """
        将 struct 插入 mongodb，返回key 以及 dic对象
        key：(front_id 、session_id 、order_ref_int)
        OrderRef 是 CTP 后台提供给客户端标识一笔报单的字段，客户端可以通过关键
        字组（FrontID 、SessionID 、OrderRef）唯一确定一笔报单，客户端在报单发出时未填
        写 OrderRef 字段，CTP 后台会自动为该报单的 OrderRef 字段赋值并返回给客户端。
        :param p_struct: 
        :return: 
        """
        order_ref_int = int(bytes_2_str(p_struct.OrderRef))
        key = (self.front_id, self.session_id, order_ref_int)
        # dic = ApiBase.insert_one_2_db(p_struct, collection_name=None, **key)
        dic = {}
        return key, dic

    def _request_action_order_insert(self, p_struct):
        """
        公共Request方法，传入方面，及相关struct数据
        函数自动生成新的request_id及进行log记录
        2017-11-26 统一改为异步方式执行
        :param func: 
        :param p_struct: 
        :return: 
        """
        if not self.is_settlement_info_confirmed:
            self.logger.warning('当前交易日期%s，最新结算信息尚未确认，发出交易请求可能失败', self.trading_day)
        func = super().ReqOrderInsert
        # request_id = self.inc_request_id()
        # order_ref = self.inc_order_ref()
        # p_struct.OrderRef = order_ref
        # p_struct.RequestID = request_id
        return self._send_request_2_queue(func, p_struct, add_request_id=True, add_order_ref=True, request_timeout=2)
        # self.logger.debug('-> %s(%d)：\n%s', func.__name__, request_id, p_struct)
        # req_ret = func(p_struct, request_id)
        # self.log_req_if_error(req_ret, stack_num=2)
        # if req_ret >= 0:
        #     order_ref_int = int(order_ref)
        #     key = (self.front_id, self.session_id, order_ref_int)
        #     stg_order = StrategyOrder(
        #         self.strategy_id, self.front_id, self.session_id, order_ref, p_struct)
        #     self.order_ref_strategy_order_dic[key] = stg_order
        #     self.logger.debug('%s(%d) order_ref_strategy_order_dic[%s]=\n%r',
        #                       func.__name__, request_id, key, stg_order)
        # return req_ret, order_ref

    def open_long(self, instrument_id, price, vol):
        """
        开多
        :param instrument_id: 
        :param price: 
        :param vol: 
        :return: 
        """
        p_struct = ApiStruct.InputOrder(BrokerID=self.broker_id,
                                        InvestorID=self.investor_id,
                                        InstrumentID=str_2_bytes(instrument_id),
                                        UserID=self.user_id,
                                        OrderPriceType=ApiStruct.OPT_LimitPrice,
                                        Direction=ApiStruct.D_Buy,
                                        CombOffsetFlag=ApiStruct.OF_Open,
                                        CombHedgeFlag=ApiStruct.HF_Speculation,
                                        LimitPrice=price,
                                        VolumeTotalOriginal=vol,
                                        TimeCondition=ApiStruct.TC_GFD
                                        )
        return self._request_action_order_insert(p_struct)

    def close_long(self, instrument_id, price, vol, offset_flag=ApiStruct.OF_Close):
        """
        平多
        上期所的持仓分今仓（当日开仓）和昨仓（历史持仓），平仓时需要指定是平今仓还是昨仓。
        上述字段中， 若对上期所的持仓直接使用THOST_FTDC_OF_Close ， 则效果同使用
        THOST_FTDC_OF_CloseYesterday 。若对其他交易所的持仓使用
        :param instrument_id: 
        :param price: 
        :param vol: 
        :return: 
        """
        p_struct = ApiStruct.InputOrder(BrokerID=self.broker_id,
                                        InvestorID=self.investor_id,
                                        InstrumentID=str_2_bytes(instrument_id),
                                        UserID=self.user_id,
                                        OrderPriceType=ApiStruct.OPT_LimitPrice,
                                        Direction=ApiStruct.D_Sell,
                                        CombOffsetFlag=offset_flag,
                                        CombHedgeFlag=ApiStruct.HF_Speculation,
                                        LimitPrice=price,
                                        VolumeTotalOriginal=vol,
                                        TimeCondition=ApiStruct.TC_GFD
                                        )
        return self._request_action_order_insert(p_struct)

    def open_short(self, instrument_id, price, vol):
        """
        开空
        :param instrument_id: 
        :param price: 
        :param vol: 
        :return: 
        """
        p_struct = ApiStruct.InputOrder(BrokerID=self.broker_id,
                                        InvestorID=self.investor_id,
                                        InstrumentID=str_2_bytes(instrument_id),
                                        UserID=self.user_id,
                                        OrderPriceType=ApiStruct.OPT_LimitPrice,
                                        Direction=ApiStruct.D_Sell,
                                        CombOffsetFlag=ApiStruct.OF_Open,
                                        CombHedgeFlag=ApiStruct.HF_Speculation,
                                        LimitPrice=price,
                                        VolumeTotalOriginal=vol,
                                        TimeCondition=ApiStruct.TC_GFD
                                        )
        return self._request_action_order_insert(p_struct)

    def close_short(self, instrument_id, price, vol, offset_flag=ApiStruct.OF_Close):
        """
        开空
        上期所的持仓分今仓（当日开仓）和昨仓（历史持仓），平仓时需要指定是平今仓还是昨仓。
        上述字段中， 若对上期所的持仓直接使用THOST_FTDC_OF_Close ， 则效果同使用
        THOST_FTDC_OF_CloseYesterday 。若对其他交易所的持仓使用
        :param instrument_id: 
        :param price: 
        :param vol:
        :param offset_flag: 平仓标示 OF_Close, OF_ForceClose, OF_CloseToday, OF_CloseYesterday
        :return: 
        """
        p_struct = ApiStruct.InputOrder(BrokerID=self.broker_id,
                                        InvestorID=self.investor_id,
                                        InstrumentID=str_2_bytes(instrument_id),
                                        UserID=self.user_id,
                                        OrderPriceType=ApiStruct.OPT_LimitPrice,
                                        Direction=ApiStruct.D_Buy,
                                        CombOffsetFlag=offset_flag,
                                        CombHedgeFlag=ApiStruct.HF_Speculation,
                                        LimitPrice=price,
                                        VolumeTotalOriginal=vol,
                                        TimeCondition=ApiStruct.TC_GFD
                                        )
        return self._request_action_order_insert(p_struct)

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        """
        报单录入请求
        :param pInputOrder: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        key, _ = self.insert_struct_2_db(pInputOrder)
        self.logger.info('报单回报%s：\n%s', key, pInputOrder)
        if status > 0:
            self.order_ref_strategy_order_dic[key].input_order = pInputOrder
            self.logger.info('提交报单完成')
            instrument_id_str = bytes_2_str(pInputOrder.InstrumentID)
            self.datetime_last_send_order_dic[instrument_id_str] = datetime.now()
        else:
            if key in self.order_ref_strategy_order_dic:
                del self.order_ref_strategy_order_dic[key]
            self.logger.error('提交报单失败')

    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        """
        交易所报单录入错误回报
        :param pInputOrder: 
        :param pRspInfo: 
        :return: 
        """
        if not self.is_rsp_success(pRspInfo):
            return
        key, _ = self.insert_struct_2_db(pInputOrder)
        self.logger.error('报单错误回报%s：\n%s', key, pInputOrder)
        self.ReqQryOrder(pInputOrder.InstrumentID)

    def OnRtnOrder(self, pOrder):
        """
        报单通知
        报盘将通过交易核心检査的报单发送到交易所前置, 交易所会再次校验该报单. 如果交易所认为该报单不合
        法,交易所会将该报单撤销,将错误値息返回給报盘.并返回更新后的该报单的状态,当客户端接收到该错
        误信息后, 就会调用 OnErrRtnOrderlnsert 函数, 而更新后的报单状态会通过调用函数 OnRtnOrder发送到
        户端.  如果交易所认为该报单合法,则只、返回该报単状态（此时的状态应为:  “尚未触发”）。
        :param pOrder: 
        :return: 
        """
        self.logger.info('报单通知：%s', pOrder)
        self._get_response(pOrder.RequestID)
        # 报单状态
        order_status = pOrder.OrderStatus
        order_status_str = ""
        if order_status == ApiStruct.OST_AllTraded:
            order_status_str = '全部成交'
        elif order_status == ApiStruct.OST_PartTradedQueueing:
            order_status_str = '部分成交还在队列中'
        elif order_status == ApiStruct.OST_PartTradedNotQueueing:
            order_status_str = '部分成交不在队列中'
        elif order_status == ApiStruct.OST_NoTradeQueueing:
            order_status_str = '未成交还在队列中'
        elif order_status == ApiStruct.OST_NoTradeNotQueueing:
            order_status_str = '未成交不在队列中'
        elif order_status == ApiStruct.OST_Canceled:
            order_status_str = '撤单'
        elif order_status == ApiStruct.OST_Unknown:
            order_status_str = '未知'
        elif order_status == ApiStruct.OST_NotTouched:
            order_status_str = '尚未触发'
        elif order_status == ApiStruct.OST_Touched:
            order_status_str = '已触发'

        # 报单提交状态
        order_submit_status = pOrder.OrderSubmitStatus
        order_submit_status_str = ""
        if order_submit_status == ApiStruct.OSS_InsertRejected:
            order_submit_status_str = '已经提交'
        elif order_submit_status == ApiStruct.OSS_CancelSubmitted:
            order_submit_status_str = '撤单已经提交'
        elif order_submit_status == ApiStruct.OSS_ModifySubmitted:
            order_submit_status_str = '修改已经提交'
        elif order_submit_status == ApiStruct.OSS_Accepted:
            order_submit_status_str = '已经接受'
        elif order_submit_status == ApiStruct.OSS_InsertRejected:
            order_submit_status_str = '报单已经被拒绝'
        elif order_submit_status == ApiStruct.OSS_CancelRejected:
            order_submit_status_str = '撤单已经被拒绝'
        elif order_submit_status == ApiStruct.OSS_ModifyRejected:
            order_submit_status_str = '改单已经被拒绝'

        # 记录状态
        key, _ = self.insert_struct_2_db(pOrder)
        if order_status in (ApiStruct.OST_Canceled, ApiStruct.OST_Unknown) or \
                        order_submit_status in (
                        ApiStruct.OSS_InsertRejected, ApiStruct.OSS_CancelRejected, ApiStruct.OSS_ModifyRejected):
            self.logger.warning('报单提交状态：%s 报单状态：%s %s',
                                order_submit_status_str, order_status_str, bytes_2_str(pOrder.StatusMsg))
        else:
            self.logger.debug('报单提交状态：%s 报单状态：%s', order_submit_status_str, order_status_str)

        self.ReqQryOrder(pOrder.InstrumentID)

    def OnRtnTrade(self, pTrade):
        """
        成交通知
        如果该报単由交易所进行了最合成交.交易所再次返国该报単的状态(已成交) ,并通过此函数返回该笔成交。
        报单成交之后, 一个报单回报(0nRtnorder)和一个成交回报(0nRtnTrade)会被发送到客户瑞,报单回报
        中报单的状态为“已成交”.但是仍然建议客户端将成交回报作为报单成交的标志,因为CTP的交易核心在
        收到 OnRtnTrade之后才会更新该报単的状态. 如.果客户端通过报单回报来判断报単成交与否并立即平仓,有
        极小的概率会出现在平仓指令到达cTP交易核心时该报单的状态仍未更新,就会导致无法平仓.
        :param pTrade: 
        :return: 
        """
        key, _ = self.insert_struct_2_db(pTrade)
        self.logger.info('成交通知 %s：%s', key, pTrade)
        order_ref = key[2]
        if key in self.order_ref_strategy_order_dic:
            self.order_ref_strategy_order_dic[key].trade_list = pTrade
        else:
            self.logger.debug('order_ref_strategy_order_dic.keys:%s', self.order_ref_strategy_order_dic.keys())
            self.logger.warning('order_ref: %s 在当前策略报单列表中未找到\n%s', order_ref, pTrade)
        instrument_id_str = bytes_2_str(pTrade.InstrumentID)
        self.datetime_last_rtn_trade_dic[instrument_id_str] = datetime.now()
        # 更新股票持仓信息
        if self.update_position_after_rtn_trade:
            self.ReqQryInvestorPosition(pTrade.InstrumentID)
        if self.update_order_after_rtn_trade:
            self.ReqQryOrder(pTrade.InstrumentID)

    def ReqOrderAction(self, order_dic):
        """
        2017-11-26 改为使用 order_dic 作为输入参数
        根据《API特别说明》文档介绍需要增加 5 个关键参数，因此，需要从 order 信息中进行提取
        报单操作请求：撤单指令
        FrontID 、SessionID、OrderRef、ExchangID、OrderSysID
        :param order_dic: 
        :return: 
        """
        if order_dic['OrderStatus'] == OST_Canceled_STR or order_dic['VolumeTotalOriginal'] == order_dic['VolumeTraded']:
            # TODO: 1 返回值没有特别意义，只是用于区分正常返回值，以后再进行规范化处理
            return 1
        front_id = int(order_dic['FrontID'])
        session_id = int(order_dic['SessionID'])
        order_ref = str_2_bytes(order_dic['OrderRef'])
        exchang_id = str_2_bytes(order_dic['ExchangeID'])
        order_sys_id = str_2_bytes(order_dic['OrderSysID'])
        self.logger.debug('请求撤销报单：\n%s', order_dic)
        order_action_ref = self.inc_order_action_ref()
        p_struct = ApiStruct.InputOrderAction(self.broker_id, self.investor_id, order_action_ref,
                                              OrderRef=order_ref, FrontID=front_id, SessionID=session_id,
                                              ExchangeID=exchang_id, OrderSysID=order_sys_id)
        return self._send_request_2_queue(super().ReqOrderAction, p_struct, add_request_id=True, request_timeout=0)

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        """
        报单操作请求响应
        :param pInputOrderAction: 
        :param pRspInfo: 
        :param nRequestID: 
        :param bIsLast: 
        :return: 
        """
        status = self.resp_common(pRspInfo, nRequestID, bIsLast)
        if status < 0:
            return
        key, _ = self.insert_struct_2_db(pInputOrderAction)
        self.logger.info('撤单回报%s：\n%s', key, pInputOrderAction)
        self.ReqQryOrder(pInputOrderAction.InstrumentID)

    def OnErrRtnOrderAction(self, pOrderAction, pRspInfo):
        """
        交易所撤单操作错误回报
        正常情况不应该出现
        :param pOrderAction: 
        :param pRspInfo: 
        :return: 
        """
        if not self.is_rsp_success(pRspInfo):
            return
        key, _ = self.insert_struct_2_db(pOrderAction)
        self.logger.info('撤单错误回报%s：\n%s', key, pOrderAction)


def test_md_api():
    """
    对mdapi 进行创建、登录、订阅等操作
    :return: 
    """
    # 行情接口的初始化
    # 1． 创建继承自 SPI，并创建出实例，以及 API 实例
    # 2． 向 API 实例注册 SPI 实例。

    # md_api = MyMdApi(Config.subscribe_instrument_list)  # [b'rb1710', b'rb1712']
    md_api = MyMdApi([b'au1712', b'rb1712', b'pb1801'])  # [b'rb1710', b'rb1712']
    # 3． 向 API 实例注册前置地址。交易接口需要注册交易前置地址，行情接口需要注册行情前置地址。
    md_api.RegisterFront()
    md_api.Init()
    time.sleep(5)
    try:
        max_count = 20
        # for n in range(max_count):
        while md_api.has_login:
            time.sleep(1)
            # if n == 1:
            # md_api.UnSubscribeMarketData([b'rb1712'])
    except KeyboardInterrupt:
        pass
    finally:
        md_api.ReqUserLogout()
        md_api.Release()


def run_trader_api():
    """
    初始化的步骤和代码基本上与行情接口（3.2 节）的初始化一致。
    不同之处
    1.  创建 API 实例时不能指定数据的传输协议。即第二行中的函数 CreateFtdcTraderApi 中只接受一个参数（即流文件目录）。
    2. 需要订阅公有流和私有流。
    公有流：交易所向所有连接着的客户端发布的信息。比如说：合约场上交易状态。
    私有流：交易所向特定客户端发送的信息。如报单回报，成交回报。
    订阅模式 
      Restart：接收所有交易所当日曾发送过的以及之后可能会发送的所有该类消息。
      Resume：接收客户端上次断开连接后交易所曾发送过的以及之后可能会发送的所有该类消息。
      Quick：接收客户端登录之后交易所可能会发送的所有该类消息。
    3. 注册的前置地址是交易前置机的地址。
    :return: 
    """
    trader_api = MyTraderApi()
    trader_api.RegisterFront()
    trader_api.SubscribePrivateTopic(ApiStruct.TERT_QUICK)
    trader_api.SubscribePublicTopic(ApiStruct.TERT_QUICK)
    trader_api.Init()

    try:
        max_count = 60
        # while 1:
        wait_count = 0
        is_settlement_info_confirmed = False
        order_dic = None
        for n in range(max_count):
            if trader_api.has_login:
                wait_count += 1
                # if not trader_api.is_settlement_info_confirmed and wait_count % 5 == 0 and wait_count < 10:
                #     trader_api.ReqQrySettlementInfo()
                # elif not trader_api.is_settlement_info_confirmed and wait_count % 5 == 0 and 10 < wait_count < 20:
                #     trader_api.ReqSettlementInfoConfirm()
                # elif trader_api.is_settlement_info_confirmed and not is_settlement_info_confirmed:
                #     is_settlement_info_confirmed = trader_api.is_settlement_info_confirmed
                #     trader_api.logger.info("结算信息已经确认")
                if not trader_api.is_settlement_info_confirmed and n == 3:
                    trader_api.is_settlement_info_confirmed = True

            time.sleep(1)
            if trader_api.is_settlement_info_confirmed:
                if n == 5:
                    logging.info("开仓")
                    trader_api.open_long('rb1805', 3910, 1)
                elif n == 6:
                    orders_dic = trader_api.get_order('rb1805')
                    if orders_dic is None:
                        continue
                    logging.info("%s", order_dic)
                elif n == 8:
                    if order_dic is not None:
                        trader_api.ReqOrderAction(order_dic)
    except KeyboardInterrupt:
        pass
    finally:
        trader_api.ReqUserLogout()
        trader_api.Release()


def test_md_trade_api():
    """
    test_md_api test_trader_api 的合体
    :return: 
    """
    # 为了更加准确的生成分钟线数据
    # 需要首先初始化 Config中的交易所时间同步数据，因此，trade_api需要被首先启动
    # 初始化 trade_api
    trader_api = MyTraderApi()
    trader_api.RegisterFront()
    trader_api.SubscribePrivateTopic(ApiStruct.TERT_QUICK)
    trader_api.SubscribePublicTopic(ApiStruct.TERT_QUICK)
    trader_api.Init()
    for _ in range(5):
        if trader_api.has_login:
            break
        else:
            time.sleep(1)
    else:
        logging.warning('trade api 登录超时，交易所时间同步可能不准，导致分钟线存在误差')

    # 初始化 md_api
    # md_api = MyMdApi(Config.subscribe_instrument_list)  # [b'rb1710', b'rb1712']
    md_api = MyMdApi([b'au1712', b'rb1712', b'pb1801'])  # [b'rb1710', b'rb1712']
    # 3． 向 API 实例注册前置地址。交易接口需要注册交易前置地址，行情接口需要注册行情前置地址。
    md_api.RegisterFront()
    md_api.Init()

    try:
        max_count = 20
        # while md_api.has_login:
        # for n in range(max_count):
        #     time.sleep(1)
        #     if n == 1:
        #         req_ret, order_ref = trader_api.open_long(b'rb1710', 3001, 1)
        #     elif n == 3:
        #         trader_api.ReqOrderAction(order_ref)
    except KeyboardInterrupt:
        pass
    finally:
        md_api.ReqUserLogout()
        md_api.Release()

        trader_api.ReqUserLogout()
        trader_api.Release()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    # test_md_api()
    run_trader_api()
    # test_md_trade_api()
