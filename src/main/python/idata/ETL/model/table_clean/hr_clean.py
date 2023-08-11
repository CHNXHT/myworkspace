import pandas as pd
import mysql.connector

# 设置MySQL数据库连接信息
cnx = mysql.connector.connect(user='root',
                              password='idata@2023',
                              host='172.16.16.32',
                              database='medical_data')

sql = """
SELECT
	person_id_new,
	sign_value_new as HEART_RATE
FROM
	(
	SELECT
		A.person_id_new,
		sign_value_new 
	FROM
		ods_medical_hr A
		JOIN ( SELECT person_id_new, MIN( TIME ) AS TIME, MIN( id ) AS id FROM `ods_medical_hr` GROUP BY person_id_new ) B ON A.person_id_new = B.person_id_new 
		AND A.TIME = B.TIME 
		AND A.id = B.id 
	) t 
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
