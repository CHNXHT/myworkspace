import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\sample_diag(诊断).rds")


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
table_name = 'medical_records_dialog'
"","PERSON_ID_NEW","CONDITION_NAME","CONDITION_CODE","CONDITION_START_DATE","CONDITION_TYPE","DIAGNOSIS_TYPE","CONDITION_CODE_HF","class"
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'condition_name VARCHAR(255), '
                'condition_code VARCHAR(255), '
                'condition_start_date VARCHAR(255), '
                'condition_type VARCHAR(255), '
                'diagnosis_type VARCHAR(255), '
                'condition_code_hf VARCHAR(255), '
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
    condition_name = args[2]
    condition_code = args[3]
    condition_start_date = args[4]
    condition_type = args[5]
    diagnosis_type = args[6]
    condition_code_hf = args[7]
    CLASS = args[8]
    print(ID,person_id_new, condition_name, condition_code,
          condition_start_date,condition_type,diagnosis_type,
          condition_code_hf,CLASS)
    if(ID != ""):
        cursor.execute("INSERT INTO medical_records_dialog("
                       "id,"
                       "person_id_new,"
                       "condition_name,"
                       "condition_code,"
                       "condition_start_date,"
                       "condition_type,"
                       "diagnosis_type,"
                       "condition_code_hf,"
                       "class)  "
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)",
                       (ID,person_id_new, condition_name, condition_code,
                        condition_start_date,condition_type,diagnosis_type,
                        condition_code_hf,CLASS))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
