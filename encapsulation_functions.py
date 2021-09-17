"""
作者：郭天鸣
该文本对第一题中用到的generate_signals文本以及
evaluate_pos文本封装为函数，方便我们多线程测试不同参数。
"""
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import datetime
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 100000)  # 最多显示数据的行数


# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# 定义函数计算DEMA
def calculate_DEMA(data_frame, period, column_name):
    EMA = data_frame[column_name].ewm(span=period, adjust=False).mean()
    DEMA = 2 * EMA - EMA.ewm(span=period, adjust=False).mean()
    return DEMA

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# 该函数产生交易信号和持仓
def signals(df,parameter=[5,20,3,0.025,0.008]):
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    # ===再对数据进行去重和按时间排序，保证数据的准确性。
    df.sort_values(by=['time'], inplace=True)
    df.drop_duplicates(subset=['time'], inplace=True)
    df.reset_index(inplace=True, drop=True)

    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    # ===定义参数
    short_span = parameter[0]
    long_span = parameter[1]
    holding_period = timedelta(hours=parameter[2])
    target = parameter[3]
    stop = parameter[4]

    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    # ===计算长周期的EMA和短周期EMA
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
    # print(df)
    return df
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# 该函数计算资金曲线
def equity(df, initial_cash=100000, face_value=100, c_rate=0.0001, slippage=0.5, leverage_rate=1,
           min_margin_ratio=15 / 100   ):
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
    df.loc[close_pos_condition, 'pnl'] = face_value * df['contract_num'] * (
                df['close_pos_price'] - df['open_pos_price']) * \
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
    df.loc[open_pos_condition, 'equity_change'] = df.loc[open_pos_condition, 'net_value'] / initial_cash - 1  # 首次开仓的收益率
    df['equity_change'].fillna(value=0, inplace=True)
    df['equity_curve'] = (df['equity_change'] + 1).cumprod()
    return df

def EMA_para_list(List_MA=[[5,20],[10,30],[20,60]], List_holding_periods=[2, 3, 5],List_stops=[0.008, 0.01],List_targets=[0.025, 0.03]):
    para_list = []
    for MA in List_MA:
        for period in List_holding_periods:
            for stop in List_stops:
                for target in List_targets:
                    para=[]
                    para=MA+[period,target,stop]
                    para_list.append(para)
    return para_list