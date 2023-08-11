import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\指标\\sample_vital_sign.rds")

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
table_name = 'medical_vital_sign'

#"person_id_new","vital_signs","units","person_id","measure_time","vital_signs_values",
# "vital_signs_new","units_new","seq_id
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'vital_signs VARCHAR(255) ,'
                'units VARCHAR(255) ,'
                'person_id VARCHAR(255) ,'
                'measure_time VARCHAR(255) ,'
                'vital_signs_values VARCHAR(255), '
                'vital_signs_new VARCHAR(255), '
                'units_new VARCHAR(255), '
                'seq_id VARCHAR(255) )'
                # 'PRIMARY KEY (person_id_new))'
                ).format(table_name)
cursor.execute(create_table)

for index in range(len(lin) - 1):
    # args = lin[index].split('",')
    data = lin[index]
    reader = csv.reader([data])
    args = next(reader)
    #分割字段
    #"person_id_new","vital_signs","units","person_id","measure_time","vital_signs_values",
    #"vital_signs_new","units_new","seq_id
    ID = args[0]
    person_id_new = args[1]
    vital_signs = args[2]
    units = args[3]
    person_id = args[4]
    measure_time = args[5]
    vital_signs_values = args[6]
    vital_signs_new = args[7]
    units_new = args[8]
    seq_id = args[9]

    print(ID,person_id_new, vital_signs,
          units,person_id,measure_time,vital_signs_values,
          vital_signs_new,units_new,seq_id)
    if(ID != ""):
        cursor.execute("INSERT INTO medical_vital_sign("
                       "id,"
                       "person_id_new,"
                       "vital_signs,"
                       "units,"
                       "person_id,"
                       "measure_time,"
                       "vital_signs_values,"
                       "vital_signs_new,"
                       "units_new,"
                       "seq_id)"
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (ID,person_id_new, vital_signs,
                        units,person_id,measure_time,vital_signs_values,
                        vital_signs_new,units_new,seq_id))
    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
