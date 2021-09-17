"""
作者：郭天鸣
该文本对参数进行测试，非题目答案，请略过。
"""
from To_The_Moon.quant_test_tianming_guo.encapsulation_functions import *
import pandas as pd
from datetime import timedelta
from multiprocessing.pool import Pool
from datetime import datetime

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数
# ===手工设定策略参数
initial_cash = 100000  # 初始资金，默认为100000元
face_value = 100  # 每张铁矿石合约面值100吨
c_rate = 0.0001  # 手续费，commission fee
slippage = 0.5  # 滑点 0.5 tick
leverage_rate = 1  # 杠杆倍数可自己调整
min_margin_ratio = 15 / 100  # 最低交易保证金率，根据券商会有所不同,在持仓过程中可能因最低价而导致爆仓
# ===读入数据
cwd = os.getcwd()
data_dir = cwd.replace("\\", "/")
original_dir = data_dir
data_dir += '/data/test_data.csv'
df = pd.read_csv(data_dir)
# 获取策略组合对
para_list = EMA_para_list()


# print(len(para_list))
# exit()
para=[30,120,240,0.05,0.008]
# ===单次循环
_df = df.copy()
# 计算交易信号与持仓
_df = signals(_df, para)
# 计算资金曲线
_df = equity(_df, initial_cash=100000, face_value=100, c_rate=0.0001, slippage=0.5, leverage_rate=1,
             min_margin_ratio=15 / 100)
rtn = pd.DataFrame()
rtn.loc[0, 'para'] = str(para)
print(para)
print(rtn)
r = _df.iloc[-1]['equity_curve']
rtn.loc[0, 'equity_curve'] = r
print(para, '策略最终收益：', r)



