import os
import pandas as pd
from utils import dir_data_output


#get monthly_return..csv
dirname = dir_data_output+'trading_data_daily'
files = os.listdir(dirname)
files.remove('.DS_Store')
files = sorted(files)
files.pop(0)
k = 0

for name in files:
    month = name[-11:-4]
    stock = pd.read_csv(dirname+'/'+name)
    stock = pd.DataFrame({'trade_date':(stock.groupby(stock.ts_code).apply(lambda x: x.trade_date.sort_values().iloc[-1]))}).reset_index()
    stock.to_csv(dir_data_output+'monthly return/monthly_return_%s.csv'%(month)))

#计算月收益率
dirname = dir_data_output+'/monthly return'
files = os.listdir(dirname)
files = sorted(files)
files.pop(0)

k = 0
for name in files:
    month = name[-11:-4]
    stock = pd.read_csv(dirname+'/'+name)
    pervious= pd.read_csv(dirname +'/'+files[k-1])
    previous = previous[['ts_code','close']]
    previous =previous.rename(columns={'close':'close_x'})
    merge = pd.merge(pervious,stock,on='ts_code',how='inner')
    merge['monthly_return'] = (merge['close'] - merge['close_x'])/merge['close_x']
    del merge['close_x']
    merge.to_csv(dir_data_output+'monthly_return_%s.csv'%(month))
    k = k + 1

#计算日收益率标准差
dirname = dir_data_output+'trading_data_daily'
files = os.listdir(dirname)
files.remove('.DS_Store')
files = sorted(files)
k = 0
for name in files:
    month = name[-11:-4]
    if k<3:
        k = k+1
        continue
    data = pd.read_csv(dirname+'/'+name)
    data['daily_return'] = (data.close - data.open)/data.open
    merge1 = data
    for i in range(2):
        previous = pd.read_csv(dirname+'/'+files[k-i-1])
        previous['daily_return'] = (previous.close - previous.open)/previous.open
        merge1 = merge1.append(previous)
    #print(merge1)
    k = k + 1
    ratio = pd.DataFrame(merge1.groupby('ts_code').daily_return.std())
    #print(data[['ts_code','trade_date','daily_return']])
    ratio = ratio.rename(columns={'daily_return':'daily_return_std_three_month'})
    store = pd.read_csv(dir_data_output+'monthly return/monthly_return_%s.csv'%(month))
    #del store['max_min_ratio_one_year']
    #ratio = pd.DataFrame((data.groupby('ts_code').close -  data.groupby('ts_code').open)/data.groupby('ts_code').open).reset_index()
    #print(ratio1)
    #ratio = ratio.rename(columns={0:'max_min_ratio_one_month'})
    merge = pd.merge(store,ratio, on='ts_code',how = 'inner')
    del merge['Unnamed: 0']
    #print(merge[['daily_return_std_one_month','daily_return_std_two_month']])
    merge.to_csv(dir_data_output+'monthly return/monthly_return_%s.csv'%(month))


#计算市场月收益率
import tushare as ts
ts.set_token('9203526c35de212c7bb198947d2961fb20cf93beafcf547f3e50b24f')
pro = ts.pro_api()
last_trade_date = pd.read_csv(dir_data_output+'/last_trading_date_monthly.csv')
index_monthly = pd.DataFrame(columns = ['ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount'])
df = pro.index_daily(ts_code='000300.SH', start_date='20150630', end_date='20150630')
index_monthly = index_monthly.append(df)
for date in last_trade_date.last_trading_date_per_month:
    date = str(date).replace('-','')
    df = pro.index_daily(ts_code='000300.SH', start_date=date, end_date=date)
    index_monthly = index_monthly.append(df)
index_monthly.to_csv(dir_data_output+'index_monthly.csv')


#计算月beta
k = 0
index = pd.read_csv(dirname+'/HS300_index.csv')
listi = list(index.monthly_return)
for name in files:
    if name == 'HS300_index.csv':
        continue
    month = name[-11:-4]
    stock = pd.read_csv(dirname+'/'+name)
    stock = stock.rename(columns={'monthly return': 'monthly_return'})
    stock['month_beta'] = (stock['monthly_return'] - 0.02)/(listi[k]-0.02)
    #stock['bool_last'] = [trade_date == last_date.ts_code for ts_code, trade_date in groupedS]
    #print(stock)
    #stock['bool_last'] = stock.groupby('ts_code').apply(lambda x: x.trade_date == last_date)
    k = k + 1
    stock.to_csv(dirname+'/'+name)


#计算市场年收益率
index_year = pd.read_csv(dir_data_output+'index_yearly.csv')
trade_date = sorted(list(index_year.trade_date))
trade_date = [20150630] + trade_date
last_date_per_month = pd.read_csv(dir_data_output + "last_trading_date_monthly.csv")
last_date_per_month = sorted(list(last_date_per_month.last_trading_date_per_month))
last_date_per_month = ['2015-06-30']+last_date_per_month
last_date_per_month = [int(str(x)[:4]+str(x)[5:7]+str(x)[8:10]) for x in last_date_per_month]
listi = list(index_year.yearly_return)
month1 = [str(x)[:4]+'-'+str(x)[4:6] for x in trade_date]
k = 1
j = 0
for name in files:
    month = name[-11:-4]
    if month != month1[k]:
        j = j+1
        continue
    data = pd.read_csv(dirname+'/'+name)
    data = data.loc[data['trade_date']==trade_date[k]]
    previous = pd.read_csv(dirname+'/'+files[j-12])
    previous = previous.loc[previous['trade_date']==last_date_per_month[j-12]]
    data=data[['ts_code','trade_date','close']]
    previous = previous[['ts_code','open']]
    merge = pd.merge(previous,data,on='ts_code',how='inner')
    merge['yearly_return'] = (merge.close - merge.open)/merge.open
    merge = merge[['ts_code','trade_date','yearly_return']]
    k = k+1
    j=j+1
index_year.to_csv(dir_data_output+'index_yearly.csv')


#计算年beta
dirname = dir_data_output+'trading_data_daily'
files = os.listdir(dirname)
files.remove('.DS_Store')
files = sorted(files)

index_year = pd.read_csv(dir_data_output+'index_yearly.csv')
trade_date = sorted(list(index_year.trade_date))
trade_date = [20150630] + trade_date
last_date_per_month = pd.read_csv(dir_data_output + "last_trading_date_monthly.csv")
last_date_per_month = sorted(list(last_date_per_month.last_trading_date_per_month))
last_date_per_month = ['2015-06-30']+last_date_per_month
last_date_per_month = [int(str(x)[:4]+str(x)[5:7]+str(x)[8:10]) for x in last_date_per_month]
listi = list(index_year.yearly_return)
month1 = [str(x)[:4]+'-'+str(x)[4:6] for x in trade_date]
k = 1
j = 0
for name in files:
    month = name[-11:-4]
    if month != month1[k]:
        j = j+1
        continue
    data = pd.read_csv(dirname+'/'+name)
    data = data.loc[data['trade_date']==trade_date[k]]
    previous = pd.read_csv(dirname+'/'+files[j-12])
    previous = previous.loc[previous['trade_date']==last_date_per_month[j-12]]
    data=data[['ts_code','trade_date','close']]
    previous = previous[['ts_code','open']]
    merge = pd.merge(previous,data,on='ts_code',how='inner')
    merge['yearly_return'] = (merge.close - merge.open)/merge.open
    merge = merge[['ts_code','yearly_return']]
    merge['year_beta'] = (merge['yearly_return'] - 0.02)/(listi[k-1]-0.02)
    store = pd.read_csv(dir_data_output + 'monthly return/monthly_return_%s.csv' % (month))
    merge = pd.merge(store, merge, on='ts_code', how='inner')
    del merge['Unnamed: 0']
    merge.to_csv(dir_data_output + 'monthly return/monthly_return_%s.csv' % (month))
    k = k+1
    j=j+1

#合并各个月份的行情因子至一个文档
import os
import pandas as pd
from utils import dir_data_output

dirname = dir_data_output+'monthly return'
os.listdir(dirname)
files = os.listdir(dirname)
files = sorted(files)
files.pop(0)
files.pop(0)

merged = pd.DataFrame(columns = ['ts_code','trade_date','monthly_return','month_beta','max_min_ratio_one_month','max_min_ratio_two_month',
                                 'max_min_ratio_three_month','max_min_ratio_six_month','max_min_ratio_one_year','daily_return_std_one_month',
                                 'daily_return_std_two_month','daily_return_std_three_month','yearly_return','year_beta'])
for name in files:
    data = pd.read_csv(dirname+'/'+name)
    merged = merged.append(data)
merged = merged.sort_values(['ts_code'],ascending=True).groupby(['ts_code'],sort=False).\
    apply(lambda x:x.sort_values(['trade_date'],ascending=True)).reset_index(drop=True)
del merged['Unnamed: 0']
merged = merged[['ts_code','trade_date','monthly_return','month_beta','max_min_ratio_one_month','max_min_ratio_two_month',
                                 'max_min_ratio_three_month','max_min_ratio_six_month','max_min_ratio_one_year','daily_return_std_one_month',
                               'daily_return_std_two_month','daily_return_std_three_month','yearly_return','year_beta']]
merged = merged.fillna(0)
merged.to_csv(dir_data_output+'merged.csv')
