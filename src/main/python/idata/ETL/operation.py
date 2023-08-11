import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\手术\\sample_oper.rds")
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
table_name = 'operation'
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'procedure_name VARCHAR(255), '
                'procedure_code VARCHAR(255), '
                'procedure_date VARCHAR(255), '
                'procedure_grade VARCHAR(255), '
                'anesthesia_method VARCHAR(255), '
                'incision_healing VARCHAR(255), '
                'procedure_code_hf VARCHAR(255), '
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
    procedure_name = args[2]
    procedure_code = args[3]
    procedure_date = args[4]
    procedure_grade = args[5]
    anesthesia_method = args[6]
    incision_healing = args[7]
    procedure_code_hf = args[8]
    CLASS = args[9]
    print(ID,person_id_new, procedure_name, procedure_code,
          procedure_date,procedure_grade,anesthesia_method,
          incision_healing,procedure_code_hf,CLASS)
    if(ID != ""):
        cursor.execute("INSERT INTO operation("
                       "id,person_id_new,procedure_name,procedure_code,"
                       "procedure_date,procedure_grade,anesthesia_method,"
                       "incision_healing,procedure_code_hf,class)  "
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (ID,person_id_new, procedure_name, procedure_code,
                        procedure_date,procedure_grade,anesthesia_method,
                        incision_healing,procedure_code_hf,CLASS))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
