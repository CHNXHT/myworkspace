
import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\指标\\sample_respiration.rds")

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
table_name = 'medical_respiration'

#"person_id_new","seq_id","sign_value_new","sign_name","time","source","unit_new","date"
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'seq_id VARCHAR(255) ,'
                'sign_value_new VARCHAR(255) ,'
                'sign_name VARCHAR(255) ,'
                '`time` VARCHAR(255), '
                '`source` VARCHAR(255), '
                'unit_new VARCHAR(255), '
                '`date` VARCHAR(255) )'
                # 'PRIMARY KEY (person_id_new))'
                ).format(table_name)
cursor.execute(create_table)

for index in range(len(lin) - 1):
    # args = lin[index].split('",')
    data = lin[index]
    reader = csv.reader([data])
    args = next(reader)
    #分割字段
    #"person_id_new","seq_id","sign_value_new","sign_name","time","source","unit_new","date"
    ID = args[0]
    person_id_new = args[1]
    seq_id = args[2]
    sign_value_new = args[3]
    sign_name = args[4]
    time = args[5]
    source = args[6]
    unit_new = args[7]
    date = args[8]

    print(ID,person_id_new, seq_id, sign_value_new,
          sign_name,time,source,
          unit_new,date)
    if(ID != ""):
        cursor.execute("INSERT INTO medical_respiration("
                       "id,"
                       "person_id_new,"
                       "seq_id,"
                       "sign_value_new,"
                       "sign_name,"
                       "time,"
                       "source,"
                       "unit_new,"
                       "date)"
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)",
                       (ID,person_id_new, seq_id, sign_value_new,
                        sign_name,time,source,
                        unit_new,date))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
