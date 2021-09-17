"""
作者：郭天鸣
该文本利用生成的singal和pos对资金曲线进行计算，同时根据题目要求计算需要参数。
"""
import pandas as pd
import os
from datetime import datetime, timedelta
import datetime
import numpy as np
from matplotlib import pyplot as plt

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 20000)  # 最多显示数据的行数

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ===读取数据:
cwd = os.getcwd()
data_dir = cwd.replace("\\", "/")
original_dir = data_dir
data_dir += '/data/test_data_pos.csv'
df = pd.read_csv(data_dir)
# print(df.head(5000))
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ===找出开仓、平仓的k线
condition1 = df['pos'] != 0  # 当前周期不为空仓
condition2 = df['pos'] != df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
open_pos_condition = condition1 & condition2
# 注意:根据示例提示,开仓点为当前k线的收盘价，平仓点为信号产生时当前k线的收盘价,
# 注意:我们用close_pos_condition找出的为平仓的前一根k线
condition1 = df['pos'] != 0  # 当前周期不为空仓
condition2 = df['pos'] != df['pos'].shift(-1)  # 当前周期和下个周期持仓方向不一样。
close_pos_condition = condition1 & condition2

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ===对每次交易进行分组:
# 标记进场时间time_enter
df.loc[open_pos_condition, 'time_enter'] = df['time']
df['time_enter'].fillna(method='ffill', inplace=True)
df.loc[df['pos'] == 0, 'time_enter'] = pd.NaT
# print(df.head(5000))

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ===开始计算资金曲线:
# 基本参数设定
initial_cash = 100000  # 初始资金，默认为100000元
face_value = 100  # 每张铁矿石合约面值100吨
c_rate = 0.0001  # 手续费，commission fee
slippage = 0.5  # 滑点 0.5 tick
leverage_rate = 1  # 杠杆倍数可自己调整
min_margin_ratio = 15 / 100  # 最低交易保证金率，根据券商会有所不同,在持仓过程中可能因最低价而导致爆仓

# ===在开仓时
# 在open_pos_condition的K线，以收盘价计算买入合约的数量。（当资金量大的时候，可以用本根K线的前5分钟均价）
df.loc[open_pos_condition, 'contract_num'] = initial_cash * leverage_rate / (face_value * df['close'])
df['contract_num'] = np.floor(df['contract_num'])  # 对合约张数向下取整
# 对相应进场价格px_enter进行标记，根据题目给出的sample，进场价格为开仓k线的收盘价
# 需要注意的是进场价格px_enter与开仓买入价格open_pos_price不同，买入价格需要在计算时，考虑滑点
df.loc[open_pos_condition, 'px_enter'] = df['close']
df.loc[open_pos_condition, 'open_pos_price'] = df['close'] + slippage * df['pos']
df['margin'] = initial_cash - df['open_pos_price'] * face_value * df['contract_num'] * c_rate  # 即保证金
# print(df.head(5000))

# ===持仓：开仓之后每根K线结束时
# 买入之后margin，contract_num,px_enter,open_pos_price, 不再发生变动
df['margin'].fillna(method='ffill', inplace=True)
df['contract_num'].fillna(method='ffill', inplace=True)
df['px_enter'].fillna(method='ffill', inplace=True)
df['open_pos_price'].fillna(method='ffill', inplace=True)
df.loc[df['pos'] == 0, ['margin', 'contract_num', 'px_enter', 'open_pos_price']] = None
# print(df.head(5000))

# ===在平仓时
# 标记出场价格px_exit，出场时间time_exit，计算平仓价格，出场价格px_exit是出场信号产生的价格，平仓价格close_pos_price需要考虑滑点
df.loc[close_pos_condition, 'px_exit'] = df['close'].shift(-1)
df.loc[close_pos_condition, 'time_exit'] = df['time'].shift(-1)
df['px_exit'].fillna(method='bfill', inplace=True)
df['time_exit'].fillna(method='bfill', inplace=True)
df.loc[close_pos_condition, 'close_pos_price'] = df['px_exit'] - slippage * df['pos']
# print(df[['time','px_enter','open_pos_price','time_enter','px_exit','close_pos_price','time_exit','pos','signal','close']].tail(5000))
# 平仓手续费
df.loc[close_pos_condition, 'close_pos_fee'] = df['close_pos_price'] * face_value * df['contract_num'] * c_rate
# ===计算利润
# 开仓至今持仓盈亏
df['profit'] = face_value * df['contract_num'] * (df['close'] - df['open_pos_price']) * df['pos']
# 最后一行有平仓，平仓卖出时
df.loc[close_pos_condition, 'profit'] = face_value * df['contract_num'] * (
            df['close_pos_price'] - df['open_pos_price']) * df['pos']
# 根据题意标记pnl,pnl为每次交易的收益，也就是只算平仓收益，不看持仓。
df.loc[close_pos_condition, 'pnl'] = face_value * df['contract_num'] * (df['close_pos_price'] - df['open_pos_price']) * \
                                     df['pos']
df['pnl'].fillna(method='bfill', inplace=True)
# 账户净值
df['net_value'] = df['margin'] + df['profit']
# print(df[['time','px_enter','open_pos_price','time_enter','px_exit','close_pos_price','time_exit','pos','signal','close','contract_num','margin','pnl']].head(1000))

# ===计算爆仓
# 至今持仓盈亏最小值
df.loc[df['pos'] == 1, 'price_min'] = df['low']
df.loc[df['pos'] == -1, 'price_min'] = df['high']
df['profit_min'] = face_value * df['contract_num'] * (df['price_min'] - df['open_pos_price']) * df['pos']
# 账户净值最小值
df['net_value_min'] = df['margin'] + df['profit_min']
# 计算最低保证金率
df['margin_ratio'] = df['net_value_min'] / (face_value * df['contract_num'] * df['price_min'])
# 计算是否爆仓
df.loc[df['margin_ratio'] <= (min_margin_ratio + c_rate), '是否爆仓'] = 1
# 此处爆仓计算使用价格的极值，会比较保险。


# ===平仓时扣除手续费
df.loc[close_pos_condition, 'net_value'] -= df['close_pos_fee']

# ===对爆仓进行处理
df['是否爆仓'] = df.groupby('time_enter')['是否爆仓'].fillna(method='ffill')
df.loc[df['是否爆仓'] == 1, 'net_value'] = 0
# print(df[['time','open_pos_price','time_enter','close_pos_price','time_exit','pos','signal','close','contract_num','margin','pnl','net_value']].head(1000))
# =====计算资金曲线(复利)
df['equity_change'] = df['net_value'].pct_change()
df.loc[open_pos_condition, 'equity_change'] = df.loc[open_pos_condition, 'net_value'] / initial_cash - 1  # 开仓日的收益率
df['equity_change'].fillna(value=0, inplace=True)
df['equity_curve'] = (df['equity_change'] + 1).cumprod()


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ===定义summary函数来计算summary所需的参数
def summary(df, year):
    df['time_enter'] = pd.to_datetime(df['time_enter'])
    # 建立一个Dataframe用来储存结果
    result = pd.DataFrame(columns=['year', 'max_margin', 'pnl', 'dd', 'trades', 'avg_pnl', 'return'])
    # net_value_year为该年所有交易及持仓账户的净值
    net_value_year = 'net_value' + '_' + str(year)
    # equit_change_year为该年的净值百分比变化
    equity_change_year = 'equity_change' + '_' + str(year)
    # equity_curve_year为该年的资金曲线百分比（复利）
    equity_curve_year = 'equity_curve' + '_' + str(year)
    df.loc[df['time_enter'].dt.year == year, net_value_year] = df['net_value']
    # 计算净值的pct_change()
    df[equity_change_year] = df[net_value_year].pct_change()
    # 修正开仓时净值与初始资金的比值的pct_change()
    df.loc[open_pos_condition, equity_change_year] = df.loc[open_pos_condition, net_value_year] / initial_cash - 1
    df[equity_change_year].fillna(value=0, inplace=True)
    # 计算该年的资金曲线
    df[equity_curve_year] = (df[equity_change_year] + 1).cumprod()
    result['year'] = [year]
    result['return'] = [df.iloc[-1][equity_curve_year] - 1]
    # 计算最大保证金，取资金曲线中最大的值乘以初始投入资金
    result['max_margin'] = [df[equity_curve_year].max() * initial_cash]
    # 用收益率来计算该年的收益
    result['pnl'] = [initial_cash * result['return'][0]]
    # 计算历史最高值到当日的跌幅，找到最大跌幅，为最大回撤
    df['dd'] = (df[equity_curve_year] / df[equity_curve_year].expanding().max() - 1).min()
    # 由跌幅计算最大回撤的钱
    result['dd'] = [df['dd'].min() * initial_cash]
    temp = df[df['time_enter'].dt.year == year]
    temp = temp[temp['signal'].notnull() & temp['signal'] != 0]
    result['trades'] = [temp['signal'].count()]
    result['avg_pnl'] = [result['pnl'][0] / result['trades'][0]]
    return result


# ===计算题目所需的summary
# 以下可以用for loop替代
summary_result = pd.concat(
    [summary(df, 2014), summary(df, 2015), summary(df, 2016), summary(df, 2017), summary(df, 2018),
     summary(df, 2019), summary(df, 2020)])
# summary_result=pd.DataFrame()
# for year in [2014,2015,2016,2017,2018,2019,2020]:
#     summary_result=pd.concat([summary_result,summary(df,year)])

summary_result.reset_index(inplace=True, drop=True)
print(summary_result)

# ===每年收益率，用于检测summary函数的结果
# df.set_index('time_enter', inplace=True)
# annual_return = df[['equity_change']].resample(rule='Y').apply(lambda x: (1 + x).prod() - 1)
# print(annual_return)

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ===根据题目要求对dataframe进行整理,然后保存
df = df[df['signal'].notnull() & df['signal'] != 0]
df.loc[df['signal'] == 1, 'type'] = 'long'
df.loc[df['signal'] == -1, 'type'] = 'short'
df.reset_index(inplace=True, drop=True)
df = df[['date', 'symbol', 'type', 'time_enter', 'px_enter', 'time_exit', 'px_exit', 'pnl']]
print(df)
df.to_csv(original_dir + '/data/problem1_result.csv')
summary_result.to_csv(original_dir + '/data/problem1_result.csv', mode='a', index=True)
