import numpy as np
import pandas as pd
from utils import dir_data_output

data = pd.read_csv(dir_data_output+'after_merge.csv')

data['month'] = [str(x)[:6] for x in data.trade_date]
label = data.groupby('month').apply(lambda x:x.sort_values(['monthly_return'], ascending=False)).reset_index(drop=True)
label = label.groupby('month').head(10)
label = label[['ts_code','month']]
data = pd.merge(data,label,on=['ts_code','month'],how='left',indicator='choose')
data['choose'] = np.where(data.choose == 'both', 1, 0)
del data['Unnamed: 0']
del data['Unnamed: 0.1']
data.to_csv(dir_data_output+'label.csv')

def mad(factor):
    me = np.nanmedian(factor)
    mad = np.nanmedian(abs(factor - me))
    up = me + (3 * 1.4826 * mad)
    down = me - (3 * 1.4826 * mad)
    factor = np.where(factor > up, up, factor)
    factor = np.where(factor < down, down, factor)
    factor = pd.DataFrame(factor)
    return factor


def stand(factor):
    mean = factor.mean()
    std = factor.std()
    return (factor - mean) / std


stock = pd.read_csv(dir_data_output+ 'label.csv')
del stock['Unnamed: 0']
print(stock)
for index, row in stock.iteritems():
    if index in ('ts_code', 'trade_date','ann_date','choose','month'):
            stock[index] = row.tolist()
    else:
            stock[index] = stand(mad(row))
print(stock)
stock.to_csv(dir_data_output+'data_preprocessed/standardized/'+'label_std.csv')
