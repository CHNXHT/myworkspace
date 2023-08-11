import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import SelectFromModel
from sklearn.feature_selection import mutual_info_classif
from sklearn.impute import IterativeImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# 读取数据集
data = pd.read_csv("your_data.csv")

# 将特征和标签分开
X = data.drop("label", axis=1)
y = data["label"]

# 用二项逻辑回归进行特征选择
clf = LogisticRegression(penalty='l1', solver='liblinear')
sfm = SelectFromModel(clf)
X_new = sfm.fit_transform(X, y)

# 采用互信息方法选取重要指标，进行缺失值处理
mi_scores = mutual_info_classif(X_new, y)

# 剩余指标的缺失值采用基于链式方程的多重插补方法进行填补替换
imp = IterativeImputer(max_iter=10, random_state=0)
X_imputed = imp.fit_transform(X)
X_imputed = pd.DataFrame(X_imputed, columns=X.columns)

# 将特征和得分存储到字典中
feature_scores = dict(zip(X.columns[sfm.get_support()], mi_scores))

# 按得分降序排序并选择前k个特征作为模型的输入
k = 10
selected_features = [feature for feature, score in sorted(feature_scores.items(), key=lambda x: x[1], reverse=True)[:k]]
X_selected = X[selected_features]

# 将数据集分成训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X_selected, y, test_size=0.3, random_state=42)

# 使用二分类决策森林建立模型
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)
y_pred = clf.predict(X_test)

# 输出模型准确率
print("Accuracy:", accuracy_score(y_test, y_pred))
