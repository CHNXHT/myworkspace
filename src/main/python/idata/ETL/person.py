import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\sample_person.rds")
filename = 'D:\\工作\\2023\\3月\\省立医院解析数据\\test\\test.txt'

lines = f.read()
# print(lines)
lin = lines.split("\n")
# 连接到MySQL数据库
cnx = mysql.connector.connect(user='root',
                              password='1qaz@wsx',
                              host='localhost',
                              database='medical_data')

# 创建一个游标对象
cursor = cnx.cursor()

# 创建表
table_name = 'person'
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL  '
                ' )'
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

    print(ID,person_id_new)
    if(ID != ""):
        cursor.execute("INSERT INTO person("
                       "id,person_id_new)  "
                       "VALUES (%s,%s)",
                       (ID,person_id_new))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
