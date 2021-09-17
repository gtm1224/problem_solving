"""
作者：郭天鸣
该文本对i.interview_data.csv文件进行预处理，提取关键信息，
并将关键data保存到test_data.csv文件
之后我们所有计算都针对test_data.csv文件
"""
# ===导入所需要的库
import pandas as pd
import time
import os
import datetime
from datetime import timedelta

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 1000)  # 最多显示数据的行数
# ===获取当前working directory，并读取测试用数据
cwd = os.getcwd()
data_dir = cwd.replace("\\", "/")
original_dir = data_dir
data_dir += '/data/i.interview_data.csv'
test_data = pd.read_csv(data_dir, skiprows=0)
# print(test_data.head(100))
# pd=test_data['date\ttime\tsymbol\tcontract\ttime_hour\topen\thigh\tlow\tclose\tvolume\topeninterest']
# ===对数据进行分割，将数据保存到test_data.csv
test_data['whole'] = test_data
test_data[
    ['date', 'time', 'symbol', 'contract', 'time_hour', 'open', 'high', 'low', 'close', 'volume', 'openinterest']] = \
test_data['whole'].str.split('\t', expand=True)
test_data['time'] = pd.to_datetime(test_data['time'])
test_data['date'] = pd.to_datetime(test_data['date'])
test_data = test_data[
    ['date', 'time', 'symbol', 'contract', 'time_hour', 'open', 'high', 'low', 'close', 'volume', 'openinterest']]
print(test_data)
test_data.to_csv(original_dir + '/data/test_data.csv')
