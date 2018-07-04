#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging
from config import Config, BROKER_URL, CELERY_RESULT_BACKEND
import psutil
import os
import md_broadcast
from celery import Celery, platforms
logger = logging.getLogger()
# got pyctp folder run following command:
# celery -A tasks worker -B -l info
# celery -A tasks worker -l info

app = Celery('tasks' , broker=BROKER_URL, backend=CELERY_RESULT_BACKEND)
platforms.C_FORCE_ROOT = True
app.config_from_object('config')


@app.task
def save_md():
    """
    对mdapi 进行创建、登录、订阅等操作
    :return: 
    """
    # check is process running
    if check_process_alive():
        logger.info('process is running')
        return -1
    # register pid for current process
    register_pid()
    logger.info('Start to receive md')
    md_broadcast.broadcast_md()
    logger.info('Finish receive_md')
    return 0


def register_pid():
    r = Config.get_redis(db=Config.CELERY_INFO_DIC['REDIS_DB_OTHER'])
    key = Config.CELERY_INFO_DIC['TASK_PID_KEY']
    pid = os.getpid()
    r.set(key, pid)


def check_process_alive():
    r = Config.get_redis(db=Config.CELERY_INFO_DIC['REDIS_DB_OTHER'])
    key = Config.CELERY_INFO_DIC['TASK_PID_KEY']
    pid = r.get(key)
    is_alive = False
    if pid is None:
        logger.warning('No pid')
        return is_alive
    r.set(key, pid)
    try:
        pinfo = psutil.Process(int(pid))
        is_alive = pinfo.is_running()
        logger.info('%s=%r is alive: %s', key, pid, is_alive)
    except psutil.NoSuchProcess:
        logger.info('%s=%r is alive: %s (not exist)', key, pid, is_alive)
    finally:
        return is_alive

if __name__ == '__main__':
    # beat
    save_md.delay()
