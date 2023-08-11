import base64
import csv
import re
import string

import mysql.connector
import pandas as pd
from collections import defaultdict
# 设置MySQL数据库连接信息
cnx = mysql.connector.connect(user='root',
                              password='idata@2023',
                              host='172.16.16.32',
                              database='medical_data')
sql="""
SELECT
	person_id_new,
	note_date,
	note_text
FROM
	`ods_medical_note` 
WHERE note_type = '入院记录'
"""
# 创建游标对象
cursor = cnx.cursor()

# 执行查询语句
query = (sql)
cursor.execute(query)

# 初始化一个 defaultdict，用于存储分类数据并进行排序
data = defaultdict(list)
for (col1, col2, col3) in cursor:
    data[col1].append((col2, col3))

# 对每个分类中的数据进行排序，并取第一个元素
min_values = {}
for col1, col23_list in data.items():
    sorted_col23_list = sorted(col23_list)
    min_values[col1] = sorted_col23_list[0]

for col1, (col2, col3) in min_values.items():

    person_id_new = col1
    note_date = col2
    # 性别
    match = re.search(r'姓名\n\n(.+)\n\n出生地', col3)
    name = match.group(1) if match else ''
    patient_name = base64.b64encode(name.encode())
    # print(patient_name)
    # 年龄
    match = re.search(r'年龄\n\n(\d+)岁', col3)
    patient_age = match.group(1) if match else ''
    # 性别
    match = re.search(r'性别\n\n(.+)\n\n职业', col3)
    patient_sex = match.group(1) if match else ''
    # 职业
    match = re.search(r'职业\n\n(.+)\n\n年龄', col3)
    patient_job = match.group(1) if match else ''
    # 民族
    match = re.search(r'民族\n\n(.+)\n\n病史陈述者', col3)
    patient_nation = match.group(1) if match else ''
    # 病史陈述者
    match = re.search(r'病史陈述者\n\n(.+)\n\n主  诉', col3)
    medical_history_presenter = match.group(1) if match else ''

    # 提取主诉信息
    match = re.search(r'主\s*诉\s*：(.+?)。', col3)
    chief_complaint = match.group(1) if match else ''

    # 提取现病史信息
    match = re.search(r'现\s*病\s*史\s*：(.+?)。', col3, re.DOTALL)
    history_present_illness = match.group(1) if match else ''
    # 提取既往史信息
    match = re.search(r'既\s*往\s*史\s*：(.+?)。', col3, re.DOTALL)
    history_past = match.group(1) if match else ''
    # 提取个人史信息
    match = re.search(r'个\s*人\s*史\s*：(.+?)。', col3, re.DOTALL)
    history_patient = match.group(1) if match else ''
    # 提取婚育史信息
    match = re.search(r'婚\s*育\s*史\s*：(.+?)。', col3, re.DOTALL)
    history_marriage = match.group(1) if match else ''
    # 提取家族史信息
    match = re.search(r'家\s*族\s*史\s*：(.+?)。', col3, re.DOTALL)
    history_family = match.group(1) if match else ''

    # 体  格  检  查（medical_examination）
    # 体温
    match = re.search(r"体温([\d\.]+)℃", col3)
    medical_examination_temperature = match.group(1) if match else ''
    # 脉搏
    match = re.search(r"脉搏(\d+)次／分", col3)
    medical_examination_pulse = match.group(1) if match else ''
    # 呼吸
    match = re.search(r"呼吸(\d+次／分)", col3)
    medical_examination_respiration = match.group(1) if match else ''
    # 血压
    match = re.search(r"血压(\d+\／\d+)mmHg", col3)
    medical_examination_blood_pressure = match.group(1) if match else ''

    print(person_id_new,patient_name,note_date,medical_examination_temperature,medical_examination_pulse,medical_examination_respiration,medical_examination_blood_pressure)

    # 匹配一般情况
    pattern = r"一般情况：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_general = match.group(1) if match else ''
    # print(general)
    # 皮肤黏膜
    pattern = r"皮肤黏膜：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_skin_mucous_membrane = match.group(1) if match else ''
    # 淋 巴 结
    pattern = r"淋 巴 结：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_lymph_nodes = match.group(1) if match else ''
    # 头颅五官
    pattern = r"头颅五官：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_head_facial_features = match.group(1) if match else ''
    # 颈    部
    pattern = r"颈    部：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_neck = match.group(1) if match else ''
    # print(neck)
    # 胸    部
    pattern = r"胸    部：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_chest = match.group(1) if match else ''
    # 肺    部
    pattern = r"肺    部：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_lungs = match.group(1) if match else ''
    # 心    脏
    pattern = r"心    脏：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_heart = match.group(1) if match else ''
    # 腹    部
    pattern = r"腹    部：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_abdomen = match.group(1) if match else ''
    # 肛门生殖器
    pattern = r"肛门生殖器：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_anogenital = match.group(1) if match else ''
    # 脊柱四肢
    pattern = r"脊柱四肢：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_spine_limbs = match.group(1) if match else ''
    # 神经系统
    pattern = r"神经系统：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_nervous_system = match.group(1) if match else ''
    # 其    他
    pattern = r"其    他：(.+?)。"
    match = re.search(pattern, col3)
    medical_examination_other = match.group(1) if match else ''
    # 提取专科检查内容
    pattern1 = r"专\s*科\s*检\s*查\s*([\s\S]*?)\s*辅\s*助\s*检\s*查"
    match = re.search(pattern1, col3)
    medical_examination_exam_content = match.group(1) if match else ''

    # 提取辅助检查内容
    pattern = r"辅\s*助\s*检\s*查\s*([\s\S]*?)诊\s*断\s*禁\s*止\s*直\s*接\s*录\s*入"
    match = re.search(pattern, col3)
    medical_examination_general_condition = match.group(1) if match else ''

    # 定义插入数据的SQL语句和数据
    sql = "INSERT INTO dw_medical_notetext (person_id_new,note_date,patient_name,patient_age,patient_sex,patient_job,patient_nation,medical_history_presenter,chief_complaint,history_present_illness,history_past,history_patient,history_marriage,history_family,medical_examination_temperature,medical_examination_pulse,medical_examination_respiration,medical_examination_blood_pressure,medical_examination_general,medical_examination_skin_mucous_membrane,medical_examination_lymph_nodes,medical_examination_head_facial_features,medical_examination_neck,medical_examination_chest,medical_examination_lungs,medical_examination_heart,medical_examination_abdomen,medical_examination_anogenital,medical_examination_spine_limbs,medical_examination_nervous_system,medical_examination_other,medical_examination_exam_content,medical_examination_general_condition) " \
          "VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s,%s, %s, %s,%s, %s,%s, %s)"
    cursor.execute(sql,(person_id_new,note_date,patient_name,patient_age,patient_sex,patient_job,patient_nation,medical_history_presenter,chief_complaint,history_present_illness,history_past,history_patient,history_marriage,history_family,medical_examination_temperature,medical_examination_pulse,medical_examination_respiration,medical_examination_blood_pressure,medical_examination_general,medical_examination_skin_mucous_membrane,medical_examination_lymph_nodes,medical_examination_head_facial_features,medical_examination_neck,medical_examination_chest,medical_examination_lungs,medical_examination_heart,medical_examination_abdomen,medical_examination_anogenital,medical_examination_spine_limbs,medical_examination_nervous_system,medical_examination_other,medical_examination_exam_content,medical_examination_general_condition))
    # 提交更改
    cnx.commit()

    # print(f"id：{person_id_new}℃，姓名：{name}，血压：{medical_examination_blood_pressure}，专科检查内容：{medical_examination_exam_content},辅助检查内容：{medical_examination_general_condition}")
    # results.append((col1, name, age))

# 将结果写入本地 CSV 文件
# with open('D:\\工作\\2023\\3月\\表\\demo3.csv', 'w', newline='', encoding='utf-8') as f:
#     writer = csv.writer(f)
#     writer.writerow(['col1', 'col2', 'col3'])
#     for col1, (col2, col3) in min_values.items():
#         writer.writerow([col1, col2, col3])

# 关闭连接
cursor.close()
cnx.close()


# sorted_rows.to_csv('D:\\工作\\2023\\3月\\表\\demo2.csv', encoding='utf-8-sig')
# 打印排序后的结果
# for row in sorted_rows:
#     print(row)
