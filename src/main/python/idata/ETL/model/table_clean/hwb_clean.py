import pandas as pd
import mysql.connector

# 设置MySQL数据库连接信息
cnx = mysql.connector.connect(user='root',
                              password='idata@2023',
                              host='172.16.16.32',
                              database='medical_data')

sql = """
SELECT
	person_id_new,sign_name,sign_value 
FROM
	(
	SELECT
		B.* 
	FROM
		ods_medical_hwb B
		JOIN ( SELECT person_id_new, sign_name, MIN( `time` ) AS `time` FROM `ods_medical_hwb` GROUP BY person_id_new, sign_name ) AS A ON A.person_id_new = B.person_id_new 
		AND A.`time` = B.`time` 
		AND A.sign_name = B.sign_name 
	) t
ORDER BY
	person_id_new
"""

# 从MySQL数据库中读取数据
df = pd.read_sql(sql, con=cnx)
# print(df)
# 翻转数据
pivot_df = pd.pivot_table(df, values='sign_value', index=['person_id_new'], columns=['sign_name'])
# print(pivot_df)


# 关闭数据库连接
cnx.close()
