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
	patient_age,
	case when patient_sex = '男' then 1
			 when patient_sex = '女' then 0
			 end as patient_sex
FROM
	dw_medical_notetext
"""

# 从MySQL数据库中读取数据
df = pd.read_sql(sql, con=cnx)

# 关闭数据库连接
cnx.close()
