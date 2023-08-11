import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\sample_prescription.rds")

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
table_name = 'medical_records_prescripption'
#"","PERSON_ID_NEW","ORDER_TYPE","ORDER_CLASS","ORDER_NAME","ORDER_START_TIME",
# "ORDER_STOP_TIME","DOSAGE","DOSAGE_UNIT","FREQUENCY","ROUTE","prescription_type"
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'order_type VARCHAR(255), '
                'order_class VARCHAR(255), '
                'order_name VARCHAR(255), '
                'order_start_time VARCHAR(255), '
                'order_stop_time VARCHAR(255), '
                'dosage VARCHAR(255), '
                'dosage_unit VARCHAR(255), '
                'frequency VARCHAR(255), '
                'route VARCHAR(255), '
                'prescription_type VARCHAR(255))'
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
    order_type = args[2]
    order_class = args[3]
    order_name = args[4]
    order_start_time = args[5]
    order_stop_time = args[6]
    dosage = args[7]
    dosage_unit = args[8]
    frequency = args[9]
    route = args[10]
    prescription_type = args[11]
    print(ID,person_id_new, order_type, order_class,
          order_name,order_start_time,order_stop_time,
          dosage,dosage_unit,frequency,route,prescription_type)
    if(ID != ""):
        cursor.execute("INSERT INTO medical_records_prescripption("
                       "id,"
                       "person_id_new,"
                       "order_type,"
                       "order_class,"
                       "order_name,"
                       "order_start_time,"
                       "order_stop_time,"
                       "dosage,"
                       "dosage_unit,"
                       "frequency,"
                       "route,"
                       "prescription_type)  "
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (ID,person_id_new, order_type, order_class,
                        order_name,order_start_time,order_stop_time,
                        dosage,dosage_unit,frequency,route,prescription_type))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
