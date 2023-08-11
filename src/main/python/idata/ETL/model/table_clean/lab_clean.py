import pandas as pd
import mysql.connector

# 设置MySQL数据库连接信息
cnx = mysql.connector.connect(user='root',
                              password='idata@2023',
                              host='172.16.16.32',
                              database='medical_data')

sql = """
# SELECT
# 	t1.person_id_new,
# 	t1.measurement_name_new,
# 	AVG( value_as_number_new ) AS num 
# FROM
# 	`ods_medical_records_lab` t1
# 	JOIN dm_measurement t2 ON t1.measurement_name_new = t2.measurement_name_new 
# 	AND t2.index_correlation = '1' 
# GROUP BY
# 	t1.person_id_new,
# 	measurement_name_new
SELECT
	v1.person_id_new,
	v1.measurement_name_new,
	v1.num,
	v2.measurement_flag 
FROM
	(
	SELECT
		t1.person_id_new,
		t1.measurement_name_new,
		AVG( value_as_number_new ) AS num 
	FROM
		`ods_medical_records_lab` t1
		JOIN dm_measurement t2 ON t1.measurement_name_new = t2.measurement_name_new 
		AND t2.index_correlation = '1' 
	GROUP BY
		t1.person_id_new,
		measurement_name_new 
	) v1
	JOIN dm_person_measurement v2 ON v1.person_id_new = v2.person_id_new 
WHERE
	v2.measurement_flag IN (
	0,
	2)
"""

# 从MySQL数据库中读取数据
df = pd.read_sql(sql, con=cnx)

# 翻转数据
# df_flipped = df.T
pivot_df = pd.pivot_table(df, values='num', index=['person_id_new','measurement_flag'], columns=['measurement_name_new'])
# print(pivot_df)
# 将结果写入本地文件
# pivot_df.to_csv('D:\\工作\\2023\\3月\\表\\test11.csv', encoding='utf-8-sig')

# 关闭数据库连接
cnx.close()
