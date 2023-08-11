import mysql.connector
import pandas as pd

# 设置MySQL数据库连接信息
cnx = mysql.connector.connect(user='root',
                              password='idata@2023',
                              host='172.16.16.32',
                              database='medical_data')

# 读取数据
cursor = cnx.cursor()
query = """SELECT DISTINCT person_id_new,condition_nm,1 as res
FROM(
SELECT
	person_id_new,
	case when condition_code_hf REGEXP 'I10|I11|I12|I13|I14|I15' then 'HYPERTENTION'
	 when condition_code_hf LIKE '%I70.9%' then 'A_S'
	 when condition_code_hf LIKE '%I25.1%' then 'CHD'
	 when condition_code_hf LIKE '%I64%' then 'CEREBRAL_APOPLEXTY'
	 when condition_code_hf LIKE '%I20.0%' then 'MI'
	 when condition_code_hf LIKE '%I50.9%' then 'CHF' 
	 when condition_code_hf LIKE '%I49.9%' then 'ARRHYTHMIAS'
	 when condition_code_hf LIKE '%E%' then 'MEN'
	 ELSE ''
	END AS condition_nm
FROM
	`ods_medical_records_dialog`
	) as a
	where condition_nm != ''
"""
# cursor.execute(query)


# 从MySQL数据库中读取数据
df = pd.read_sql(query, con=cnx)
# print(df)
# 翻转数据
pivot_df = pd.pivot_table(df, values='res', index=['person_id_new'], columns=['condition_nm'], fill_value=0)
# print(pivot_df)
# pivot_df.to_csv('D:\\工作\\2023\\3月\\表\\dialog_test.csv', encoding='utf-8-sig')


