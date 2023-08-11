import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\用药\\sample_atc.rds")

lines = f.read()
lin = lines.split("\n")

# 连接到MySQL数据库
cnx = mysql.connector.connect(user='root',
                              password='1qaz@wsx',
                              host='localhost',
                              database='medical_data')

# 创建一个游标对象
cursor = cnx.cursor()

# 创建表
table_name = 'medication_atc'

#"","person_id_new","drug_name","drug_start_date","drug_end_date",
# "dose","dose_unit_new","medication_frequency_new","route_new","class"
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'drug_name VARCHAR(255) ,'
                'drug_start_date VARCHAR(255), '
                'drug_end_date VARCHAR(255), '
                'dose VARCHAR(255), '
                'dose_unit_new VARCHAR(255), '
                'medication_frequency_new VARCHAR(255), '
                'route_new VARCHAR(255), '
                'class VARCHAR(255) )'
                # 'PRIMARY KEY (person_id_new))'
                ).format(table_name)
cursor.execute(create_table)

for index in range(len(lin) - 1):
    # args = lin[index].split('",')
    data = lin[index]
    reader = csv.reader([data])
    args = next(reader)
    #分割字段
    #"","person_id_new","drug_name","drug_start_date","drug_end_date",
    # "dose","dose_unit_new","medication_frequency_new","route_new","class"
    ID = args[0]
    person_id_new = args[1]
    drug_name = args[2]
    drug_start_date = args[3]
    drug_end_date = args[4]
    dose = args[5]
    dose_unit_new = args[6]
    medication_frequency_new = args[7]
    route_new = args[8]
    CLASS = args[9]

    print(ID,person_id_new, drug_name, drug_start_date,
          drug_end_date,dose,dose_unit_new,
          medication_frequency_new,route_new,CLASS)
    if(ID != ""):
        cursor.execute("INSERT INTO medication_atc("
                       "id,"
                       "person_id_new,"
                       "drug_name,"
                       "drug_start_date,"
                       "drug_end_date,"
                       "dose,"
                       "dose_unit_new,"
                       "medication_frequency_new,"
                       "route_new,"
                       "class)"
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s,%s)",
                       (ID,person_id_new, drug_name, drug_start_date,
                        drug_end_date,dose,dose_unit_new,
                        medication_frequency_new,route_new,CLASS))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
