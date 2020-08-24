from utils import dir_data_output
import pandas as pd
from sklearn.svm import LinearSVC
from sklearn.model_selection import KFold
data = pd.read_csv(dir_data_output+'data_preprocessed/standardized/label_std.csv')

ic = pd.read_csv(dir_data_output+'ic.csv')
ic = ic.sort_values(['IR'],ascending=False).reset_index(drop= True)
factorname = list(ic.iloc[:30].factor)
data =data.sample(frac = 1).reset_index(drop=True)
sample = data.iloc[:int(data.shape[0]*0.85)]
factor = sample[factorname]
label = sample[['choose']]
test = data.iloc[int(data.shape[0]*0.85):]
testf = test[factorname]
testl = test[['choose']]
SVM = LinearSVC()
kf= KFold(n_splits=5,random_state=None, shuffle=False)
for train_index, test_index in kf.split(sample):
    factor_train = factor.iloc[train_index]
    factor_test = factor.iloc[test_index]
    label_train = label.iloc[train_index]
    label_test = label.iloc[test_index]
    SVM.fit(factor_train,label_train)
    print(SVM.score(factor_test,label_test))

print(SVM.score(testf,testl))
