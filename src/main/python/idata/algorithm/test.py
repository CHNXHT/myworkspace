import numpy as np
import pandas as pd
import sklearn
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import roc_auc_score
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import mutual_info_classif, mutual_info_regression
import xgboost as xgb
import matplotlib.pyplot as plt
import matplotlib as mpl




def main():
    # 读取数据
    df = pd.read_csv("C:\\Users\\WF\\Desktop\\DN\\training_dataset.csv")

    # 计算每列缺失值比例
    missing_percentage = df.isnull().sum() / len(df) * 100

    # 选择缺失值比例小于80%的列
    selected_columns = missing_percentage[missing_percentage < 80].index

    # 仅保留选择的列
    data = df[selected_columns]

    # 采用互信息（Mutual Information）方法选取重要指标，进行缺失值处理,对重要指标缺失低于50%的采用均值填补
    X = data.drop('measurement_flag', axis=1)
    y = data['measurement_flag']
    mi_res = []
    for col in X.columns:
        df_col = data[[col, 'measurement_flag']].copy().dropna()
        mi = mutual_info_regression(df_col[[col]], df_col['measurement_flag'])
        mi_res.append([col, mi])
    print(max(mi_res), min(mi_res))

    used = [res for res in mi_res if res[1] >= 0.02]

    histogram(used)

    col_used = [res[0] for res in mi_res if res[1] >= 0.02]
    columns = []
    for col in col_used:
        percentage = df[col].isnull().sum() / len(df) * 100
        # 对重要指标缺失低于50% 的采用均值填补，缺失超过50%的直接删除
        if percentage < 50:
            columns.append(col)
        else:
            X.drop(col, axis=1)
    important_features = columns
    imp = sklearn.impute.SimpleImputer()
    # X_imp = imp.fit_transform(X[important_features])
    # X_imp_df = pd.DataFrame(X_imp, columns=important_features)
    X[important_features] = imp.fit_transform(X[important_features])

    # 对剩余指标的缺失值采用基于链式方程的多重插补（Multivariate Imputation by Chained Equations，MICE）方法进行填补替换
    Y = IterativeImputer(max_iter=10, random_state=0)

    # 使用fit_transform函数填补数据
    X_imputed = Y.fit_transform(X)

    # 将填补后的数据集保存到文件中
    X_imputed_df = pd.DataFrame(X_imputed, columns=X.columns)

    # 采用 Z-score 方法对特征指标进行归一化处理
    scaler = StandardScaler()
    X = scaler.fit_transform(X_imputed_df)

    # 将数据集按 90% 和 10% 的比例拆分为训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=0)

    # 利用 XGBoost 生成二分类提升决策树模型
    xgb_model = xgb.XGBClassifier()

    # 模型参数
    params = {
        'max_leaf_nodes': [2, 8, 20, 32, 64],
        'min_child_weight': [5, 10, 50, 100],
        'learning_rate': [0.1, 0.2, 0.4],
        'n_estimators': [20, 100, 500, 800],
        'random_state': [0]
    }

    # 训练模型
    grid_search = RandomizedSearchCV(xgb_model, params, n_iter=40, cv=10, scoring='roc_auc', random_state=0)
    grid_search.fit(X_train, y_train)

    # 输出最佳模型参数和评价指标值
    print('Best Parameters: ', grid_search.best_params_)
    y_pred = grid_search.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred)

    y_pred = grid_search.predict(X_test)
    report = classification_report(y_test, y_pred, output_dict=True)

    acc_mean = report['accuracy']
    prec_mean = report['macro avg']['precision']
    rec_mean = report['macro avg']['recall']
    f1_mean = report['macro avg']['f1-score']

    # 创建表格
    df = pd.DataFrame({'评价指标': ['准确率', '精确率', '召回率', 'F1 值', 'AUC'],
                       '均数': [acc_mean, prec_mean, rec_mean, f1_mean, auc]})

    # 打印表格
    print(df)
    print(report)


if __name__ == '__main__':

    print('运行主程序')
    main()
