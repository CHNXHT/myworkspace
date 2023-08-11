import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\sample_event.rds")

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
table_name = 'medical_records_event'
"","PERSON_ID_NEW","cond","date","VISIT_START_DATE","VISIT_END_DATE","icu_start","icu_end","class"
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'cond VARCHAR(255), '
                '`date` VARCHAR(255), '
                'visit_start_date VARCHAR(255), '
                'visit_end_date VARCHAR(255), '
                'icu_start VARCHAR(255), '
                'icu_end VARCHAR(255), '
                'class VARCHAR(255))'
                # 'PRIMARY KEY (person_id_new))'
                ).format(table_name)
cursor.execute(create_table)

for index in range(len(lin) - 1):
    # args = lin[index].split('",')
    data = lin[index]
    reader = csv.reader([data])
    args = next(reader)
    #分割字段
    ID = args[0]
    person_id_new = args[1]
    cond = args[2]
    Date = args[3]
    visit_start_date = args[4]
    visit_end_date = args[5]
    icu_start = args[6]
    icu_end = args[7]
    CLASS = args[8]
    print(ID,person_id_new, cond, Date,
          visit_start_date,visit_end_date,icu_start,
          icu_end,CLASS)
    if(ID != ""):
        cursor.execute("INSERT INTO medical_records_event("
                       "id,"
                       "person_id_new,"
                       "cond,"
                       "date,"
                       "visit_start_date,"
                       "visit_end_date,"
                       "icu_start,"
                       "icu_end,"
                       "class)  "
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)",
                       (ID,person_id_new, cond, Date,
                        visit_start_date,visit_end_date,icu_start,
                        icu_end,CLASS))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
