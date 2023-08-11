import re

import chardet as chardet
import mysql.connector
# 连接到MySQL数据库
cnx = mysql.connector.connect(user='root',
                              password='1qaz@wsx',
                              host='localhost',
                              database='medical_data')

# 创建一个游标对象
cursor = cnx.cursor()

# 创建表
table_name = 'medical_note'
#"","person_id_new","note_type","person_id","note_date","note_text","department_id","note_type_new"
create_table = ('CREATE TABLE IF NOT EXISTS {} '
                '(id VARCHAR(255) , '
                'person_id_new VARCHAR(255) NOT NULL , '
                'note_type VARCHAR(255), '
                'person_id VARCHAR(255), '
                'note_date VARCHAR(255), '
                'note_text TEXT, '
                'department_id VARCHAR(255), '
                'note_type_new VARCHAR(255))'
                # 'PRIMARY KEY (person_id_new))'
                ).format(table_name)
cursor.execute(create_table)


# 打开文件并读取内容
with open('D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\notetest.rds', 'rb') as f:
    file_contents = f.read()
# 检测文件编码方式
result = chardet.detect(file_contents)
print(result)
encoding = result['encoding']
# 打开文件
with open('D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\test.txt','r', encoding=encoding) as f:
    # 读取文件内容
    file_contents = f.read()
    pattern = r'\n"'  # 正则表达式模式
    arrs = re.split(pattern,file_contents)
    print(arrs)
    for eles in arrs:
        args = eles.split('","')
        # if len(args) > 7:

        if len(args) > 0 :
            ID = re.search(r'\d+', args[0]).group()
        else:
            ID = ""
        if len(args) > 1 :
            person_id_new = args[1]
        else:
            person_id_new = ""
        if len(args) > 2 :
            note_type = args[2]
        else:
            note_type = ""
        if len(args) > 3 :
            person_id = args[3]
        else:
            person_id = ""
        if len(args) > 4:
            note_date = args[4]
        else:
            note_date = ""
        if len(args) > 5:
            note_text = args[5]
        else:
            note_text = ""
        if len(args) > 6:
            department_id = args[6]
        else:
            department_id = ""
        if len(args) > 7:
            note_type_new = args[7].replace('"','')
        else:
            note_type_new = ""

        #"","person_id_new","note_type","person_id","note_date","note_text","department_id","note_type_new"
        if(ID != ""):
            cursor.execute("INSERT INTO medical_note("
                           "id,"
                           "person_id_new,"
                           "note_type,"
                           "person_id,"
                           "note_date,"
                           "note_text,"
                           "department_id,"
                           "note_type_new)"
                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                           (ID,person_id_new, note_type, person_id,
                            note_date,note_text,department_id,note_type_new))
        print(ID)
        # print(eles.split('","'))
    print(len(arrs))

    # 提交更改
cnx.commit()

# 关闭连接和游标
cursor.close()
cnx.close()
