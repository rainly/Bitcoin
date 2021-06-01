# 交易所：币安
# 时间：每日0930更新昨日数据
# 类别：现货
# 币对：所有USDT标价
# 周期：5m，30m，1h

import pandas as pd
import ccxt
import time
import os
import datetime
from dingmsg import *
pd.set_option('expand_frame_repr', False)

binance = ccxt.binance()  # 创建交易所

# 构造日期格式
yesterday_date = datetime.date.today() - datetime.timedelta(days=1)
start_time = str(yesterday_date) + ' 00:00:00'
end_time = str(datetime.date.today()) + ' 00:00:00'
start = binance.parse8601(start_time)  # 币安的时间格式
end = binance.parse8601(end_time)

# 获取交割合约信息
spot_list = binance.load_markets()  # 获取交易合约信息
market = pd.DataFrame(spot_list).T  # 转换为DF
symbol_list = list(market['id'])  # 抽取symbol列转为列表
symbol_list = list(filter(lambda x: x.endswith('USDT'), symbol_list))  # 只保留USDT标价的货币


# ===构造参数列表
params_list = []
time_interval = ['5m', '30m', '1h']  # K线周期
for s in symbol_list:
    for t in time_interval:
        params = {
            'symbol': s,  # 币对
            'interval': t,  # 周期
            'startTime': start,  # 开始时间
            'endTime': end  # 结束时间
        }  # 参数格式各个交易所不同
        params_list.append(params)


error_list = []  # 创建错误列表


for params in params_list:
    path = r'C:\Users\Administrator\PycharmProjects\AK_DIGICCY\data\history_candle_data_XBX'
    df_list = []
    print(params)
    try:
        while True:
            # 获取数据
            df = binance.publicGetKlines(params=params)
            # 整理数据
            df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
            df['candle_begin_time'] = pd.to_datetime(df[0], unit='ms')  # 整理时间
            # 合并数据
            df_list.append(df)
            # 新的start
            t = pd.to_datetime(df.iloc[-1][0], unit='ms')
            params['startTime'] = binance.parse8601(str(t))
            # 判断是否挑出循环
            if t >= pd.to_datetime(params['endTime'], unit='ms') or df.shape[0] <= 1:
                break
            time.sleep(1)  # 暂停n秒，防止抓取过于频繁导致报错

        df = pd.concat(df_list, ignore_index=True)  # 合并数据
        df.rename(columns={1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume',
                           7: 'quote_volume', 8: 'trade_num', 9: 'taker_buy_base_asset_volume',
                           10: 'taker_buy_quote_asset_volume'}, inplace=True)  # 重命名
        df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)  # 去重
        df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
                 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']]  # 保留有用的列
        df.sort_values('candle_begin_time', inplace=True)  # 排序
        df.reset_index(drop=True, inplace=True)  # 重置


        # ===保存数据到文件
        # 创建交易所文件夹
        path = os.path.join(path, 'binance')
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 创建spot文件夹
        path = os.path.join(path, 'spot')
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 创建日期文件夹
        path = os.path.join(path, str(pd.to_datetime(df.iloc[0][0]).date()))
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 拼接文件目录
        file_name = params['symbol'][:-4] + '-' + params['symbol'][-4:] + '_' + params['interval'] + '.csv'
        path = os.path.join(path, file_name)
        # 保存数据
        pd.DataFrame(columns=['数据由AK整理']).to_csv(path, index=False, encoding='gbk')
        df.to_csv(path, index=False, mode='a', encoding='gbk')
        print(path)
    except Exception as e:
        print(e)
        error_list.append(params['symbol'] + '_' + params['interval'])
print(error_list)

content = 'AK哥！\n币安现货数据更新完毕！'
send_dingding_msg_QC(content)