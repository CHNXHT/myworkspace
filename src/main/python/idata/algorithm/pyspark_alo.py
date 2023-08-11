from pyspark.python.pyspark.shell import spark
from pyspark.sql.functions import col
from pyspark.ml.feature import Imputer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# 读取数据
df = spark.read.format("csv").option("header", "true").load("C:\\Users\\WF\\Desktop\\DN\\training_dataset.csv")

# 计算每列缺失值比例
missing_percentage = df.select([(100*(col(c).isNull().sum()/df.count())).alias(c) for c in df.columns])

# 选择缺失值比例小于80%的列
selected_columns = [c for c in missing_percentage.columns if missing_percentage.select(c).collect()[0][0] < 80]

# 仅保留选择的列
data = df.select(selected_columns)

# 采用互信息（Mutual Information）方法选取重要指标，进行缺失值处理，对重要指标缺失低于50%的采用均值填补
X = data.drop('measurement_flag')
y = data.select('measurement_flag')
mi_res = []
for col in X.columns:
    df_col = data.select(col, 'measurement_flag').dropna()
    mi = df_col.stat.corr(col, 'measurement_flag', 'spearman')
    mi_res.append((col, mi))
print(max(mi_res, key=lambda x:x[1]), min(mi_res, key=lambda x:x[1]))

used = [res for res in mi_res if res[1] >= 0.02]

# 打印重要特征的互信息值的直方图
hist_data = spark.createDataFrame(used, ['feature', 'mi'])
hist_data.toPandas().plot(kind='bar', x='feature', y='mi')

col_used = [res[0] for res in mi_res if res[1] >= 0.02]
columns = []
for col in col_used:
    percentage = data.select(col).na.drop().count() / data.count() * 100
    # 对重要指标缺失低于50%的采用均值填补，缺失超过50%的直接删除
    if percentage < 50:
        columns.append(col)
    else:
        data = data.drop(col)
important_features = columns

# 采用SimpleImputer对缺失值进行填补
imputer = Imputer(inputCols=important_features, outputCols=[f"{col}_imputed" for col in important_features], strategy='mean')
data = imputer.fit(data).transform(data)

# 对剩余指标的缺失值采用Multivariate Imputation by Chained Equations（MICE）方法进行填补替换
imputer = Imputer(strategy='mice', maxIter=10, inputCols=[f"{col}_imputed" for col in important_features], outputCols=[f"{col}_imputed" for col in important_features])
data = imputer.fit(data).transform(data)

# 采用Z-score方法对特征指标进行归一化处理
scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
scaler_model = scaler.fit(data)
data = scaler_model.transform(data).select('scaled_features', 'measurement_flag')

#
