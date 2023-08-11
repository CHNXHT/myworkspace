import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\指标\\sample_bp.rds")

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
table_name = 'medical_bp'

"","person_id_new","sbp","dbp","seq_id","source","unit_new","time","date"
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'sbp VARCHAR(255) ,'
                'dbp VARCHAR(255) ,'
                'seq_id VARCHAR(255) ,'
                '`source` VARCHAR(255), '
                'unit_new VARCHAR(255), '
                '`time` VARCHAR(255),'
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
    #"","person_id_new","sbp","dbp","seq_id","source","unit_new","time","date"
    ID = args[0]
    person_id_new = args[1]
    sbp = args[2]
    dbp = args[3]
    seq_id = args[4]
    source = args[5]
    unit_new = args[6]
    time = args[7]
    date = args[8]

    print(ID,person_id_new, sbp, dbp,
          seq_id,source,unit_new,
          time,date)
    if(ID != ""):
        cursor.execute("INSERT INTO medical_bp("
                       "id,"
                       "person_id_new,"
                       "sbp,"
                       "dbp,"
                       "seq_id,"
                       "source,"
                       "unit_new,"
                       "time,"
                       "date)"
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)",
                       (ID,person_id_new, sbp, dbp,
                        seq_id,source,unit_new,
                        time,date))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
