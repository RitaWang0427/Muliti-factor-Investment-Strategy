import os
import pandas as pd
from utils import dir_data_raw_account,dir_data_output
import numpy as np

dirname = dir_data_raw_account +'financial_indicator'
files = os.listdir(dirname)
files = sorted(files)
print(files)
merged = pd.read_csv(dir_data_output+'merged.csv')


# 合并财务和行情因子
last_date_per_month = pd.read_csv(dir_data_output + "last_trading_date_monthly.csv")
last_date_per_month = sorted(list(last_date_per_month.last_trading_date_per_month))
last_date_per_month = [int(str(x)[:4]+str(x)[5:7]+str(x)[8:10]) for x in last_date_per_month]

merged_account = pd.DataFrame()

def find_closest_value(timepoint, time_series):
    # takes a pd.Timestamp() instance and a pd.Series with dates in it
    # calcs the delta between `timepoint` and each date in `time_series`
    # returns the closest date and optionally the number of days in its time delta
    time_series = time_series.sort_values(ascending=True).reset_index(drop=True)
    deltas = timepoint - time_series
    deltas = [1000000000000000000 if x < 0 else x for x in deltas]
    idx_closest_date = np.argmin(deltas)
    res = {"trade_date": time_series.ix[idx_closest_date]}
    return pd.Series(res)

def find_closest_value1(timepoint, time_series):
    # takes a pd.Timestamp() instance and a pd.Series with dates in it
    # calcs the delta between `timepoint` and each date in `time_series`
    # returns the closest date and optionally the number of days in its time delta
    time_series = time_series.sort_values(ascending=True).reset_index(drop=True)
    deltas = time_series-timepoint
    deltas = [1000000000000000000 if x < 0 else x for x in deltas]
    idx_closest_date = np.argmin(deltas)
    res = {"trade_date": time_series.ix[idx_closest_date]}
    return pd.Series(res)


for names in files:
    account = pd.read_excel(dirname+'/'+names)
    del account['end_date']
    account = account.sort_values(['ann_date'],ascending=True).reset_index(drop=True)
    del account['Unnamed: 0']
    account = account.iloc[1:]
    trade_date = pd.DataFrame(data=np.unique(merged.loc[merged['ts_code']==account['ts_code'][1]].trade_date),columns=['trade_date']).sort_values(by=['trade_date'])
    account = account.drop(account[(account.ann_date > trade_date.trade_date.iloc[-1])].index)
    ann_date = trade_date.trade_date.apply(find_closest_value,args=[account.ann_date])
    ann_date = np.unique(ann_date)
    account['bool'] = account.ann_date.apply(lambda x: x in ann_date)
    account = account[account['bool']]
    del account['bool']
    ann_date = pd.DataFrame(ann_date,columns=['ann_date'])
    account['trade_date'] = account.ann_date.apply(find_closest_value1, args=[trade_date.trade_date])
    print(account[['ts_code','trade_date','ann_date']])
    merged_account = merged_account.append(account)

merged_account = merged_account.sort_values(['ts_code'],ascending=True).groupby(['ts_code'],sort=False).\
    apply(lambda x:x.sort_values(['trade_date'],ascending=True)).reset_index(drop=True)
merge1 = pd.merge(merged,merged_account,on=['ts_code','trade_date'],how='outer').ffill()
merge1 = merge1.fillna(0)
example = merge1[merge1.ts_code == '000630.SZ']
print(example[['ts_code','trade_date','ann_date']])
merge1.to_csv(dir_data_output+'after_merge.csv')


#反转因子
dirname = dir_data_output+'after_merge.csv'
merge = pd.read_csv(dirname)

dirname1 = dir_data_output+'trading_data_daily'
files1 = os.listdir(dirname1)
files1.remove('.DS_Store')
files1 = sorted(files1)
june = files1[0]
print(files1)

store = pd.DataFrame()
for names in files1:
    data = pd.read_csv(dirname1+'/'+names)
    data = data[['ts_code','trade_date','open','close']]
    store = store.append(data)
merged = pd.merge(merge,store,on=['ts_code','trade_date'],how = 'inner')
del merged['Unnamed: 0']
june = pd.read_csv(dirname1+'/'+june)
june = june[['ts_code','trade_date','open','close']]
change = merged[['ts_code','trade_date','open','close']]
change = change.append(june)
change = change.groupby(['ts_code']).apply(lambda x : x.sort_values(['trade_date'],ascending=True)).reset_index(drop=True)
print(change.head())
change['sa_close_ratio_one_month'] = change['close']/(change['close'].shift(1)-1)
change['sa_close_ratio_two_month'] = change['close']/(change['close'].shift(2)-1)
print(change['sa_close_ratio_two_month'] )
change['sa_close_ratio_three_month'] = change['close']/(change['close'].shift(3)-1)
change['sa_close_ratio_six_month'] = change['close']/(change['close'].shift(6)-1)
change['sa_close_ratio_one_year'] = change['close']/(change['close'].shift(12)-1)
change = change.drop(change[(change.trade_date <= 20150630)].index).reset_index(drop=True)
merged['sa_close_ratio_one_month'] = change['sa_close_ratio_one_month']
merged['sa_close_ratio_two_month'] = change['sa_close_ratio_two_month']
merged['sa_close_ratio_three_month'] = change['sa_close_ratio_three_month']
merged['sa_close_ratio_six_month'] = change['sa_close_ratio_six_month']
merged['sa_close_ratio_one_year'] = change['sa_close_ratio_one_year']
merged= merged.fillna(0)
del merged['close']
del merged['open']
merged.to_csv(dir_data_output+'after_merge.csv')
