"""
作者：郭天鸣
该文本对test_data.csv进行处理，计算开仓信号，标记开仓时间，价格等，并模拟持仓，
最后，处理过的包含开仓信号和持仓等信息的文件将被保存到test_data_pos.csv。
"""
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ===导入所需要的库
import pandas as pd
import os
from datetime import datetime, timedelta
import datetime

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 1000000)  # 最多显示数据的行数

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ===读取数据:
cwd = os.getcwd()
data_dir = cwd.replace("\\", "/")
original_dir = data_dir
data_dir += '/data/test_data.csv'
df = pd.read_csv(data_dir)

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ===再对数据进行去重和按时间排序，保证数据的准确性。
df.sort_values(by=['time'], inplace=True)
df.drop_duplicates(subset=['time'], inplace=True)
df.reset_index(inplace=True, drop=True)
# print(df)

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ===定义参数
parameter = [5, 20, 3, 0.025, 0.008]
short_span = parameter[0]
long_span = parameter[1]
commission = 0.0001
slippage = 0.5
holding_period = timedelta(hours=parameter[2])
target = parameter[3]
stop = parameter[4]


# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ===计算长周期的EMA和短周期EMA
# 定义函数计算DEMA
def calculate_DEMA(data_frame, period, column_name):
    EMA = data_frame[column_name].ewm(span=period, adjust=False).mean()
    DEMA = 2 * EMA - EMA.ewm(span=period, adjust=False).mean()
    return DEMA


df['EMA_short'] = calculate_DEMA(df, short_span, 'close')
df['EMA_long'] = calculate_DEMA(df, long_span, 'close')
# ===找出做多信号
condition1 = df['EMA_short'] > df['EMA_long']  # 短期均线 > 长期均线
condition2 = df['EMA_short'].shift(1) <= df['EMA_long'].shift(1)  # 上一周期的短期均线 <= 长期均线
df.loc[condition1 & condition2, 'signal'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

# ===找出做空信号
condition1 = df['EMA_short'] < df['EMA_long']  # 短期均线 < 长期均线
condition2 = df['EMA_short'].shift(1) >= df['EMA_long'].shift(1)  # 上一周期的短期均线 >= 长期均线
df.loc[condition1 & condition2, 'signal'] = -1  # 将产生空仓信号当天的signal设置为-1，-1代表开空

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ===找出平仓信号
# 当signal不为空时，标记开仓时间time_enter和进场价格px_enter
# 注意：进场价格并不是开仓价格，开仓价格需呀加上手续费和滑点
# 我们在之后计算收益时会用到。
df.loc[df['signal'].notnull(), 'px_enter'] = df['close']
df.loc[df['signal'].notnull(), 'time_enter'] = df['time']

# 根据进场价格计算出相应的止盈和止损价格
df['stop_price'] = df['px_enter'] * (-df['signal'] * stop + 1)
df['target_price'] = df['px_enter'] * (df['signal'] * target + 1)

# 把signal进行复制并对其补全，同时对target_price和stop_price中NaN的值进行补全方便比较
df['signal_copy'] = df['signal'].copy()
df['signal_copy'].fillna(method='ffill', inplace=True)
df['signal_copy'].fillna(value=0, inplace=True)
df['target_price'].fillna(method='ffill', inplace=True)
df['stop_price'].fillna(method='ffill', inplace=True)

# 通过条件比较得出按止盈或者止损价格平仓的行，平仓标记为0，0代表空仓，信号由flatten_due_to_price表示
condition1 = df['close'] <= df['stop_price']
condition2 = df['close'] >= df['target_price']
condition3 = df['signal_copy'] == 1
df.loc[condition3 & (condition1 | condition2), 'flatten_due_to_price'] = 0
condition1 = df['close'] >= df['stop_price']
condition2 = df['close'] <= df['target_price']
condition3 = df['signal_copy'] == -1
df.loc[condition3 & (condition1 | condition2), 'flatten_due_to_price'] = 0
# print(df[['signal','flatten_due_to_price']])
# 除去多余的因价格而产生的平仓信号并记录因价格平仓的时间点
temp = df[['flatten_due_to_price']]
temp = temp[temp['flatten_due_to_price'] != temp['flatten_due_to_price'].shift(1)]
# print(temp)
df['flatten_due_to_price'] = temp
condition1 = df['signal'].isnull()
condition2 = df['flatten_due_to_price'] == 0
df.loc[condition1 & condition2, 'signal'] = 0
# df.loc[df['flatten_due_to_price'].notnull(),'flatten_due_to_price_time']=pd.to_datetime(df['time'])
# print(temp)
# print(df['signal'])
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ===持仓时间条件平仓
# 考虑到平仓时间在每日15:00或者23:30的特殊情况，我们从下根k线开始，取间隔时间累加的方法来凑出大于等于holding_period(第一题为3小时)
# 具体我们新增一列'time_adder'将进场时间所在的行设为0,以0作为分割，我们每次重新累加其中的时间间隔，最后再找出累加的时间大于等于3小时即可
# 0                         0
# 0 days 00:45  取时间间隔累加 0 days 00:45
# 0 days 00:30  --------->  0 days 01:15
# 0 days 02:30              0 days 03:45
# ...                       ...
# 0                         0
df.loc[df['time_enter'].notnull(), 'time_adder'] = 0
df['time_diff'] = pd.to_datetime(df['time']).copy() - pd.to_datetime(df['time'].shift(1)).copy()
df.loc[df['time_diff'] > timedelta(hours=6), 'time_diff'] = timedelta(hours=0)

i = 0
adder = timedelta(hours=0)
while i < len(df['time_diff']):
    if df.loc[i, 'time_adder'] != 0:
        df.loc[i, 'time_adder'] = df.loc[i, 'time_diff'] + adder
        adder = df.loc[i, 'time_adder']
    else:
        # 当遇到0时我们将adder赋值为0小时，重新开始累加
        adder = timedelta(hours=0)
    i += 1

condition1 = pd.to_timedelta(df['time_adder']) >= holding_period
condition2 = df['signal'].isnull()
df.loc[condition1 & condition2, 'signal'] = 0

# 去除signal中的多余的平仓信号，保留每次开仓后最先出现的平仓信号
temp = df[df['signal'].notnull()][['signal']]
temp = temp[temp['signal'] != temp['signal'].shift(1)]
df['signal'] = temp['signal']
# print(df['signal'])
df['pos'] = df['signal'].copy()
df['pos'].fillna(method='ffill', inplace=True)
df['pos'].fillna(value=0, inplace=True)
df = df[['date', 'time', 'symbol', 'open', 'high', 'low', 'close', 'signal', 'pos']]
print(df)
df.to_csv(original_dir + '/data/test_data_pos.csv')
