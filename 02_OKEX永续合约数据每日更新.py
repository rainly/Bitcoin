# 交易所：OKEX
# 时间：每日0910更新昨日数据
# 类别：币本位保证金永续合约、USDT保证金永续合约
# 币种：所有
# 周期：5m，15m

import pandas as pd
import ccxt
import time
import os
import datetime
from dingmsg import *

pd.set_option('expand_frame_repr', False)  # 完整显示列名称
pd.set_option('display.unicode.east_asian_width', True)  # 中文显示对齐
pd.set_option('display.max_rows', 500)  # 显示最大行数
pd.set_option('precision', 4)  # 浮点数的精度，只影响显示精度，不改变记录精度

okex = ccxt.okex()  # 创建交易所

# 构造日期格式
yesterday_date = datetime.date.today() - datetime.timedelta(days=1)
start_time = str(yesterday_date) + ' 00:00:00'
end_time = str(datetime.date.today()) + ' 00:00:00'
start = okex.iso8601(okex.parse8601(start_time))
end = okex.iso8601(okex.parse8601(end_time))

# 获取永续合约信息
futures_list = okex.swapGetInstruments()  # 获取交易合约信息
market = pd.DataFrame(futures_list)  # 转换为DF
symbol_list = list(market['instrument_id'])  # 抽取instrument_id列转为列表


# ===构造参数列表
params_list = []
time_interval = ['300', '900']  # 单位为秒
for s in symbol_list:
    for t in time_interval:
        params = {
            'instrument_id': s,  # 币对
            'granularity': t,  # 周期
            'start': start,  # 开始时间
            'end': end  # 结束时间
        }  # 参数格式各个交易所不同
        params_list.append(params)

error_list = []  # 创建错误列表


for params in params_list:
    path = r'C:\Users\Administrator\PycharmProjects\AK_DIGICCY\data\history_candle_data'
    df_list = []
    print(params)
    try:
        while True:
            # 获取数据
            df = okex.swapGetInstrumentsInstrumentIdCandles(params=params)
            # 整理数据
            df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
            # 合并数据
            df_list.append(df)
            # 新的start
            t = pd.to_datetime(params['end'])
            params['start'] = df.iloc[0][0]
            # 判断是否挑出循环
            if t <= pd.to_datetime(params['start']) or df.shape[0] <= 1:
                break
            time.sleep(0.5)  # 暂停n秒，防止抓取过于频繁导致报错

        df = pd.concat(df_list, ignore_index=True)  # 合并数据
        df.rename(columns={0: 'candle_begin_time', 1: 'open', 2: 'high',
                           3: 'low', 4: 'close', 5: 'volume', 6: 'currency_volume'}, inplace=True)  # 重命名
        df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)  # 去重
        df.sort_values('candle_begin_time', inplace=True)  # 排序
        df.reset_index(drop=True, inplace=True)  # 重置

        # ===保存数据到文件
        # 创建交易所文件夹
        path = os.path.join(path, 'okex')
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 创建spot文件夹
        path = os.path.join(path, 'swap')
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 创建日期文件夹
        path = os.path.join(path, str(pd.to_datetime(df.iloc[0][0]).date()))
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 拼接文件目录
        file_name = params['instrument_id'] + '_' + params['granularity'] + '.csv'
        path = os.path.join(path, file_name)
        # 保存数据
        df.to_csv(path, index=False)
        print(path)
    except Exception as e:
        print(e)
        error_list.append(params['instrument_id'] + '_' + params['granularity'])
print(error_list)

content = 'AK哥！\nOKEX永续合约数据更新完毕！'
send_dingding_msg_QC(content)