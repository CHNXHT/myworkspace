import csv

import mysql.connector

f = open("D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\sample_visit.rds")

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
table_name = 'medical_records_visit'


create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'uniq_id VARCHAR(255) ,'
                'visit_start_date VARCHAR(255), '
                'visit_end_date VARCHAR(255), '
                'out_condition VARCHAR(255), '
                'department_id_new VARCHAR(255), '
                'dept_discharge_from_new VARCHAR(255), '
                'total_costs VARCHAR(255), '
                'charlson_score VARCHAR(255), '
                'ccc_score VARCHAR(255), '
                'visit_type VARCHAR(255))'
                # 'PRIMARY KEY (person_id_new))'
                ).format(table_name)
cursor.execute(create_table)

for index in range(len(lin) - 1):
    # args = lin[index].split('",')
    data = lin[index]
    reader = csv.reader([data])
    args = next(reader)
    #分割字段
    #"","person_id_new","uniq_id","visit_start_date","visit_end_date","out_condition",
    # "department_id_new","dept_discharge_from_new","total_costs","charlson.score","ccc.score","visit_type"
    ID = args[0]
    person_id_new = args[1]
    uniq_id = args[2]
    visit_start_date = args[3]
    visit_end_date = args[4]
    out_condition = args[5]
    department_id_new = args[6]
    dept_discharge_from_new = args[7]
    total_costs = args[8]
    charlson_score = args[9]
    ccc_score = args[10]
    visit_type = args[11]
    print(ID,person_id_new, uniq_id, visit_start_date,
          visit_end_date,out_condition,department_id_new,
          dept_discharge_from_new,total_costs,charlson_score,ccc_score,visit_type)
    if(ID != ""):
        cursor.execute("INSERT INTO medical_records_visit("
                       "id,"
                       "person_id_new,"
                       "uniq_id,"
                       "visit_start_date,"
                       "visit_end_date,"
                       "out_condition,"
                       "department_id_new,"
                       "dept_discharge_from_new,"
                       "total_costs,"
                       "charlson_score,"
                       "ccc_score,"
                       "visit_type)"
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s)",
                       (ID,person_id_new, uniq_id, visit_start_date,
                         visit_end_date,out_condition,department_id_new,
                         dept_discharge_from_new,total_costs,charlson_score,ccc_score,visit_type))

    # 提交更改
    cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
f.close()
