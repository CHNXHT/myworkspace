import pandas as pd
import mysql.connector

# 设置MySQL数据库连接信息
cnx = mysql.connector.connect(user='root',
                              password='idata@2023',
                              host='172.16.16.32',
                              database='medical_data')

sql = """
SELECT * from dm_person_measurement
"""

# 从MySQL数据库中读取数据
df = pd.read_sql(sql, con=cnx)
print(df)
# 翻转数据
# df_flipped = df.T
# pivot_df = pd.pivot_table(df, values='sign_value', index=['person_id_new'], columns=['sign_name'])
# print(pivot_df)

# pivot_df.to_csv('D:\\工作\\2023\\3月\\表\\dialog_clean.csv', encoding='utf-8-sig')

# 关闭数据库连接
cnx.close()
