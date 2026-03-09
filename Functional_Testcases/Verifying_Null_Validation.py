import pandas as pd
import json
import os
import logging
from datetime import datetime

from Functional_Testcases.Verifying_Source_Target_Counts import Result

dt=datetime.now.strftime("%d-%m-%Y_%H-%M-%S")
log_file=fr"C:\Users\Dell\PycharmProjects\PythonProject\PythonProject\etl_practice\Logs"
logging.basicConfig(filename=log_file,
                    level=logging.DEBUG,
                    format='%(asctime)s-%(levelname)s-%(message)s')


def Null_Check(target_db):
    Config_TestScenarios_dir=fr"C:\Users\Dell\PycharmProjects\PythonProject\PythonProject\etl_practice\Config_TestScenarios"

    all_table_result=[]

    for filename in os.listdir(Config_TestScenarios_dir):
        ts_json_path=Config_TestScenarios_dir + '/' + filename

        try:
            with open(ts_json_path) as SQL_path:
                SQL_QUERY=json.load(SQL_path)

            target_table=SQL_QUERY["tc_01_Source & Target tables"]["t_table"]

            #null value from target
            null_count=SQL_QUERY["tc_02_Source & Target count validation"]["Null_cnt"]
            target_cursor=target_db.cursor()
            target_cursor.execute(null_count)
            df_null_check=pd.DataFrame(target_cursor)

            if df_null_check.empty or df_null_check==0:
                null_count=0
                null_records=0
                Status="There are no null records in this table"
                Result="PASS"
                logging.info(f"Null Count : {null_count}")

            else:
                null_count=df_null_check.iloc[0,0]
                null_records=SQL_QUERY["tc_03_Source & Target count validation"]["null_records"]
                null_cursor=target_db.cursor()
                null_cursor.execute(null_records)
                df_null_check=pd.DataFrame(null_cursor).to_string(index=False, header=False)
                df_null_check=','.join(df_null_check.split())
                Status="There are null records in this table"
                Result="FAIL"
                logging.info(f"Null Count : {null_count}")

            df_nulls=pd.DataFrame(
                {
                    "Database":["MySql_Source","Oracle_Target"],
                    "Target_table":[target_table],
                    "Null_count":[null_count],
                    "Null_records":[null_records],
                    "Status":[Status],
                    "Result":[Result],
                }

            all_table_result.append(df_null_check)

            except Exception as e:
                logging.error(f"Error: {e}")
                continue

    df_null_result=pd.conact(all_table_result,ignore_index=True)
    return df_null_result











