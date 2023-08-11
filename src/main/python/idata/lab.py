import mysql.connector
import random

# 连接 MySQL 数据库
mydb = mysql.connector.connect(user='root',
                               password='1qaz@wsx',
                               host='localhost',
                               database='medical_data')


# 创建游标对象
cursor = mydb.cursor()

# 执行 SQL 查询，读取表中的所有数据
query = "SELECT measurement_name_new,sample_name_new,unit_new,value_as_number_new  FROM ods_medical_records_lab"
cursor.execute(query)

# 获取结果集
results = cursor.fetchall()

# 关闭游标和数据库连接
cursor.close()
mydb.close()

# 将表数据按照指定列进行分组，并随机选择每个分组中的一行
selected_rows = []
groups = {}
for row in results:
    group_key = row[0]  # 使用第一列作为分组的键
    if group_key not in groups:
        groups[group_key] = []
    groups[group_key].append(row)
for group_key, group_rows in groups.items():
    selected_row = random.choice(group_rows)
    selected_rows.append(selected_row)

# 将结果写入本地文件
with open('D:\\工作\\2023\\3月\\表\\lab_random.txt', 'w') as f:
    for row in selected_rows:
        f.write(','.join(str(field) for field in row) + '\n')