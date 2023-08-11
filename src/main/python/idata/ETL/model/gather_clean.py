import mysql.connector
import pandas as pd

from idata.ETL.model.table_clean import dialog_clean, lab_clean, hwb_clean, hr_clean,notetext_clean, person_measure



dialog_df = dialog_clean.pivot_df
lab_df = lab_clean.pivot_df
hwb_df = hwb_clean.pivot_df
hr_df = hr_clean.df
notetext_df = notetext_clean.df
person_measure_df = person_measure.df
# 执行左连接操作
result_df1 = pd.merge(lab_df, dialog_df,  on='person_id_new', how='left')
result_df2 = pd.merge(result_df1, hwb_df,  on='person_id_new', how='left')
result_df3 = pd.merge(result_df2, hr_df,  on='person_id_new', how='left')
result_df4 = pd.merge(result_df3, notetext_df,  on='person_id_new', how='left')
result_df5 = pd.merge(result_df4, person_measure_df,  on='person_id_new', how='left')
# print(result_df)
# 将结果输出到本地CSV文件
result_df5.to_csv('D:\\工作\\2023\\3月\\表\\training_dataset.csv', encoding='utf-8-sig')

