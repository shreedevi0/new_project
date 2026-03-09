import pandas as pd
import json
import os
import logging
from datetime import datetime

dt=datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
log_file=fr"C:\Users\Dell\PycharmProjects\PythonProject\PythonProject\etl_practice\Logs"
logging.basicConfig(filename=log_file,level=logging.DEBUG,format='%(asctime)s %(message)s',datefmt='%m/%d/%Y %I:%M:%S')

def Duplicate_check(td):
    Config_TestScenarios_mkdir=fr"C:\Users\Dell\PycharmProjects\PythonProject\PythonProject\etl_practice\Config_TestScenarios"

    all_tables=[]

    #json path
    for filename in os.listdir(Config_TestScenarios_mkdir):
        ts_json_path=Config_TestScenarios_mkdir +'/'+'filename'

        try:
            with open(ts_json_path,'r') as sql_path:
                SQL_QUERY=json.load(sql_path)

            #reading source & target tables
            source_table=SQL_QUERY["tc_01_Source & Target tables"]["s_table"]
            target_table=SQL_QUERY["tc_01_Target tables"]["t_table"]

            #capturing duplicates
            dupl_count=SQL_QUERY[ "tc_04_Verification of duplicate records in target"]["duplicate_cnt"]
            dupl_cursor=td.cursor()
            duplicate_records=dupl_cursor.execute(dupl_count)
            df_duplicate_records=pd.DataFrame(duplicate_records)

            if df_duplicate_records.empty or df_duplicate_records==0:
                dupl_count=0
                dupl_records=None
                Status="there are no duplicate records"
                Result="PASS"

            else:
                dupl_count=df_duplicate_records
                dupl_records=SQL_QUERY[ "tc_04_Verification of duplicate records in target"]["duplicate_record"]
                dupl_cursor=td.cursor()
                df_dupl_records=dupl_cursor.execute(dupl_cursor)
                df_dupl_records=pd.DataFrame(df_dupl_records).to_string(index=False, header=False)
                df_du_rcrd=','.join(df_dupl_records.split())
                Status="there are duplicate records"
                Result="FAIL"

                df_result=pd.DataFrame(
                    {"Database":["Mysql_source"],["Oracle_target"],
                     "table":[target_table],
                     "duplicate_cnt":[dupl_count],
                    "duplicate_records":[df_duplicate_records]
                    "Status":[Status]
                    "Result":[Result]
                )

                all_tables.append(df_result)

        except exception as e:
            logging.error(e)

    df_result=pd.concat(all_tables, ignore_index=True)
    return df_result





