import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\sample_lab.rds")

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
table_name = 'medical_records_lab'
#"PERSON_ID_NEW","MEASUREMENT_NAME_NEW","SAMPLE_NAME_NEW","DETECT_TIME",
# "UNIT_NEW","VALUE_AS_NUMBER_NEW","UID","APPLY_TIME","VALUE_AS_CATEGORY_NEW",
# "VISIT_TYPE","DEPARTMENT_ID","class"
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'measurement_name_new VARCHAR(255), '
                'sample_name_new VARCHAR(255), '
                'detect_time VARCHAR(255), '
                'unit_new VARCHAR(255), '
                'value_as_number_new VARCHAR(255), '
                'uid VARCHAR(255), '
                'apply_time VARCHAR(255),'
                'value_as_category_new VARCHAR(255),'
                'visit_type VARCHAR(255),'
                'department_id VARCHAR(255),'
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
    measurement_name_new = args[2]
    sample_name_new = args[3]
    detect_time = args[4]
    unit_new = args[5]
    value_as_number_new = args[6]
    uid = args[7]
    apply_time = args[8]
    value_as_category_new = args[9]
    visit_type = args[10]
    department_id = args[11]
    CLASS = args[12]
    allvalues = (ID,person_id_new, measurement_name_new, sample_name_new,
          detect_time,unit_new,value_as_number_new,uid,apply_time,value_as_category_new,
          visit_type,department_id,CLASS)
    print(allvalues)
    if(ID != ""):
        cursor.execute("INSERT INTO medical_records_lab("
                       "id,"
                       "person_id_new,"
                       "measurement_name_new,"
                       "sample_name_new,"
                       "detect_time,"
                       "unit_new,"
                       "value_as_number_new,"
                       "uid,"
                       "apply_time,"
                       "value_as_category_new,"
                       "visit_type,"
                       "department_id,"
                       "class)  "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (ID,person_id_new, measurement_name_new, sample_name_new,
                         detect_time,unit_new,value_as_number_new,uid,apply_time,value_as_category_new,
                         visit_type,department_id,CLASS))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
