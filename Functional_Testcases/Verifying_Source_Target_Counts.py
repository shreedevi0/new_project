import pandas as pd
import json
import os
import logging
from datetime import datetime

dt=datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
log_file=fr"C:\Users\Dell\PycharmProjects\PythonProject\PythonProject\etl_practice\Logs"
logging.basicConfig(filename=log_file,level=logging.DEBUG,
                    format='%(asctime)s-%(levelname)s-%(message)s',
                    datefmt="%d-%m-%Y %H-%M-%S",
                    force='true')

def Source_Target_count(sd,td):
logging.info("Source db initiated")
logging.info("target db initiated")
logging.info("source & target table validation initiated")

Config_TestScenarios_dir=fr"C:\Users\Dell\PycharmProjects\PythonProject\PythonProject\etl_practice\Config_TestScenarios"

all_table_result=[]

for filename in os.listdir(Config_TestScenarios_dir):
    ts_json_path=Config_TestScenarios_dir + '/' + filename

    try:
        logging.info(f"Processing {filename}")

        with open(ts_json_path) as SQL_path:
            SQL_QUERY=json.load(SQL_path)

        #capturing source & target table
        s_file_type=SQL_QUERY["tc_01_Source & Target tables"] ["file_type"]
        s_file_path = SQL_QUERY["tc_01_Source & Target tables"]["file_path"]
        t_table=SQL_QUERY["tc_01_Target tables"] ["t_table"]

        #count validation for source files
        #read fils based on file type
        if s_file_type.lower() == "csv":
            df_source=pd.read_csv(s_file_path)

        elif s_file_type.lower() == "json":
            df_source=pd.read_json(s_file_path)

        elif s_file_type.lower() == "json":
            df_source=pd.read_json(s_file_path)

        else:
            raise Exception(f"Unsupported file type: {s_file_type}")

        df_source_count=len(df_source)

        #count validation for targe table
        t_count=SQL_QUERY["tc_02_Source & Target count validation"] ["t_count"]
        t_cursor=td.cursor()
        t_cursor.execute(t_count)
        df_target_count=pd.DataFrame(t_cursor).iloc[0,0]

        if df_source_count==df_target_count:
            STATUS="count matched between source and target"
            Result="PASS"
            logging.info(f"source table count: {df_source_count} & target table count: {df_target_count}")
        else:
            STATUS="count mismatch between source and target"
            Result="FAIL"
            logging.info(f"source table count: {df_source_count} & target table count: {df_target_count}")

        #create dataframe to store result in excel sheet
        df_result=pd.DataFrame(
            {
                "Database":['MySql_Source','Oracle_Target'],
                "Source_File":[s_file_path],
                "Target_Tables":[t_table],
                "Count":[df_source_count,df_target_count],
                "Result":[Result,None],
                "Status":[STATUS,None]
            }
        )

        all_table_result.append(df_result)

    except Exception as e:
       logging.error(e)
       continue

df_count-result=pd.concat(all_table_result, ignore_index=True)

#importing all the functions and calling them
#all results in same excel wit different excel sheets
#all table


