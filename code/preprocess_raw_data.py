from jqdatasdk import *
import pandas as pd
import numpy as np
from utils import dir_data_output, dir_data_raw_market
auth('18562693679','693679')

trading_date = get_trade_days(start_date='20150701',end_date='20200701')
trading_date = sorted(trading_date)
trading_date_month = [str(date)[:7] for date in trading_date]
trading_date = pd.DataFrame({"trading_date": trading_date,'month': trading_date_month})
last_trading_date_per_month = trading_date.trading_date.groupby(trading_date.month).apply(
    lambda x: x.sort_values().iloc[-1])
last_trading_date_per_month.index = np.arange(len(last_trading_date_per_month))
last_trading_date_per_month.drop(last_trading_date_per_month.tail(1).index,inplace = True)
pd.DataFrame({"last_trading_date_per_month": last_trading_date_per_month}).to_csv(
    dir_data_output + "last_trading_date_monthly.csv", index=False)

last_date_per_month = pd.read_csv(dir_data_output + "last_trading_date_monthly.csv")
HS300 = pd.DataFrame()
for i in range(len(last_date_per_month.last_trading_date_per_month)):
    datee = str(last_date_per_month.last_trading_date_per_month[i])
    stocks = get_index_stocks('000300.XSHG',date = str(datee))
    HS300[datee[:7]] = stocks
stock_monthly = pd.DataFrame()
HS300.to_csv(dir_data_output + "HS300.csv", index=True)

k=0
for i in HS300.columns:
    stock_monthly = pd.DataFrame(columns = ['ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount'])
    for stockn in HS300[i]:
        if stockn[-4:] == 'XSHG':
            suffix = 'SH'
        else:
            suffix = 'SZ'
        try:
            stock_data = pd.read_excel(dir_data_raw_market+'daily_stock_split_adj/daily_stock_split_adj_%s.%s.xls'%(stockn[:6],suffix))
            stock_data["bool_month"] = stock_data.trade_date.apply(lambda x: str(x)[:6] == str(i)[:4]+str(i)[-2:])
            stock_data = stock_data[stock_data.bool_month]
            del stock_data['Unnamed: 0']
            del stock_data['bool_month']
            stock_monthly=stock_monthly.append(stock_data)
        except FileNotFoundError:
            print(stockn[:6]+suffix)
    stock_monthly.to_csv(dir_data_output+'trading_data_daily_%s.csv'%(i),index = False)




