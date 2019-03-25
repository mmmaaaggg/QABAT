#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-3-21 上午11:16
@File    : create_file_order.py
@contact : mmmaaaggg@163.com
@desc    : 创建 file_order 文件，供测试使用
"""
import pandas as pd
import os


def create_file(long_inst_id='', long_position=0, short_inst_id='', short_position=0):
    labels = ['Long_InstrumentID', 'Long_Position', 'Short_InstrumentID', 'Short_Position']
    order_df = pd.DataFrame([[long_inst_id, long_position, short_inst_id, short_position]], columns=labels)
    file_path = os.path.abspath(os.path.join(os.pardir, 'file_order', 'order.csv'))
    order_df.to_csv(file_path, index=False)


if __name__ == "__main__":
    create_file(long_inst_id='rb1905', long_position=2)
