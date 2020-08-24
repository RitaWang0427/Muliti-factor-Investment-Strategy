from utils import dir_data_output
import pandas as pd
import numpy as np
import scipy.stats as ss

data = pd.read_csv(dir_data_output+'after_merge.csv')
data['month'] = [str(x)[:6] for x in data.trade_date]
data['monthly_nxt_return'] = data.sort_values(['trade_date'],ascending=True).groupby(['ts_code']).\
    apply(lambda x:x['monthly_return'].shift(1)).reset_index(drop=True)
del data['Unnamed: 0']
def anal_ICs(data, factor_name):
    ICs = []
    dates = np.unique(data['month'])
    dates = sorted(dates)
    for date in dates:
        cur_df = data[data.month == date]
        cur_bool1 = pd.notna(cur_df[factor_name])
        cur_bool2 = pd.notna(cur_df['monthly_nxt_return'])
        cur_bool = cur_bool1 & cur_bool2
        if sum(cur_bool) > 0:
            cur_df = cur_df[cur_bool]
            ICs.append(np.corrcoef(cur_df[factor_name],cur_df['monthly_nxt_return'])[0,1])
    ICs = [x for x in ICs if ~np.isnan(x)]
    ICs = pd.Series(ICs)
    IC_mean = np.mean(ICs)
    IC_abs_mean = np.mean(abs(ICs))
    IC_std = np.std(ICs)
    IC_greater_zero = (sum(ICs > 0) / len(ICs) - 0.5)
    IR = IC_mean / IC_std
    IC_t = IC_mean / IC_std * np.sqrt(len(ICs) - 1)
    return {"IC_mean": IC_mean, "IC_abs_mean": IC_abs_mean, "IC_std": IC_std, 'IR': IR,
            "IC_greater_zero": IC_greater_zero, "IC_t": IC_t}


def anal_rankICs(data_df, factor_name):
    ICs = []
    dates = np.unique(data_df['month'])
    dates = sorted(dates)
    for date in dates:
        cur_df = data_df[data_df.month == date]
        cur_bool1 = pd.notna(cur_df[factor_name])
        cur_bool2 = pd.notna(cur_df['monthly_nxt_return'])
        cur_bool = cur_bool1 & cur_bool2
        if sum(cur_bool) > 0:
            cur_df = cur_df[cur_bool]
            s1 = ss.rankdata(cur_df[factor_name])
            s2 = ss.rankdata(cur_df['monthly_nxt_return'])
            ICs.append(np.corrcoef(s1, s2)[0, 1])
    ICs = [x for x in ICs if ~np.isnan(x)]
    ICs = pd.Series(ICs)
    IC_mean = np.mean(ICs)
    IC_abs_mean = np.mean(abs(ICs))
    IC_std = np.std(ICs)
    IR = IC_mean/IC_std
    IC_greater_zero = (sum(ICs > 0) / len(ICs) - 0.5)
    IC_t = IC_mean / IC_std * np.sqrt(len(ICs))

    return {"rankIC_mean": IC_mean, "rankIC_abs_mean": IC_abs_mean, "rankIC_std": IC_std,'rankIR': IR,
            "rankIC_greater_zero": IC_greater_zero, "rankIC_t": IC_t}


IC = pd.DataFrame()
for column in data.columns:
    if column =='ts_code' or column =='trade_date' or column =='ann_date' or column == 'monthly_nxt_return'or column == 'month' or column == 'choose' :
        continue
    else:
        ICs = anal_ICs(data, column)
        store = pd.DataFrame([[column,ICs['IC_mean'],ICs['IC_abs_mean'],ICs['IC_std'],ICs['IC_greater_zero'],ICs['IC_t'], ICs['IR']]],
                              columns = ['factor','IC_mean','IC_abs_mean','IC_std','IC_greater_zero','IC_t','IR'])
        IC = IC.append(store)
IC.to_csv(dir_data_output+'ic.csv',index = True)
