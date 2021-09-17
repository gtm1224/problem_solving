"""
作者：郭天鸣
该文本运用了并行的方法来对不同参数进行测试
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
# ===单次循环
def calculate_by_one_loop(para):
    _df = df.copy()
    # 计算交易信号与持仓
    print(para)
    _df = signals(_df, para)
    # 计算资金曲线
    _df = equity(_df, initial_cash=100000, face_value=100, c_rate=0.0001, slippage=0.5, leverage_rate=1,
                 min_margin_ratio=15 / 100)
    rtn = pd.DataFrame()
    rtn.loc[0, 'para'] = str(para)
    # print(para)
    # print(rtn)
    r = _df.iloc[-1]['equity_curve']
    rtn.loc[0, 'equity_curve'] = r
    print(para, '策略最终收益：', r)
    # print(rtn)
    # ----------------------------------
    return rtn


if __name__ == '__main__':
    start_time = datetime.now()  # 标记开始时间
    with Pool(processes=8) as pool:  # 本人电脑是8核所以开8核
        # 使用并行批量获得data frame的一个列表
        df_list = pool.map(calculate_by_one_loop, para_list)
        print('读入完成, 开始合并', datetime.now() - start_time)
        # 合并为一个大的DataFrame
        para_curve_df = pd.concat(df_list, ignore_index=True)

    # ===输出
    add_time = pd.DataFrame(columns=['final_time'])
    final_time = add_time['final_time'] = [datetime.now() - start_time]
    print(para_curve_df)
    para_curve_df.to_csv(original_dir + '/data/problem2_result.csv')
    add_time.to_csv(original_dir + '/data/problem2_result.csv', mode='a')

"""
读入完成, 开始合并 0:04:38.408414
                         para  equity_curve
0    [5, 20, 2, 0.025, 0.008]      0.041439
1     [5, 20, 2, 0.03, 0.008]      0.038322
2     [5, 20, 2, 0.025, 0.01]      0.041023
3      [5, 20, 2, 0.03, 0.01]      0.037936
4    [5, 20, 3, 0.025, 0.008]      0.036884
5     [5, 20, 3, 0.03, 0.008]      0.033316
6     [5, 20, 3, 0.025, 0.01]      0.037288
7      [5, 20, 3, 0.03, 0.01]      0.033681
8    [5, 20, 5, 0.025, 0.008]      0.041027
9     [5, 20, 5, 0.03, 0.008]      0.038528
10    [5, 20, 5, 0.025, 0.01]      0.039935
11     [5, 20, 5, 0.03, 0.01]      0.037502
12  [10, 30, 2, 0.025, 0.008]      0.169191
13   [10, 30, 2, 0.03, 0.008]      0.169340
14   [10, 30, 2, 0.025, 0.01]      0.157339
15    [10, 30, 2, 0.03, 0.01]      0.157478
16  [10, 30, 3, 0.025, 0.008]      0.142629
17   [10, 30, 3, 0.03, 0.008]      0.142188
18   [10, 30, 3, 0.025, 0.01]      0.139829
19    [10, 30, 3, 0.03, 0.01]      0.139397
20  [10, 30, 5, 0.025, 0.008]      0.221394
21   [10, 30, 5, 0.03, 0.008]      0.225046
22   [10, 30, 5, 0.025, 0.01]      0.219949
23    [10, 30, 5, 0.03, 0.01]      0.223576
24  [20, 60, 2, 0.025, 0.008]      0.311602
25   [20, 60, 2, 0.03, 0.008]      0.309988
26   [20, 60, 2, 0.025, 0.01]      0.332990
27    [20, 60, 2, 0.03, 0.01]      0.331266
28  [20, 60, 3, 0.025, 0.008]      0.323420
29   [20, 60, 3, 0.03, 0.008]      0.329745
30   [20, 60, 3, 0.025, 0.01]      0.355655
31    [20, 60, 3, 0.03, 0.01]      0.362611
32  [20, 60, 5, 0.025, 0.008]      0.345527
33   [20, 60, 5, 0.03, 0.008]      0.349490
34   [20, 60, 5, 0.025, 0.01]      0.388453
35    [20, 60, 5, 0.03, 0.01]      0.392909

Process finished with exit code 0

"""
