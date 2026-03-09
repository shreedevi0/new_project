import pandas as pd
import json
import os
import logging
from datetime import datetime

dt = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
log_file = fr"C:\Users\Dell\PycharmProjects\PythonProject\PythonProject\ETL_Automation_With_Pytest\Logs\\ETL_testing_logs_{dt}.log"
logging.basicConfig(filename=log_file,level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt="%d-%m-%Y %H-%M-%S",force=True)

def Column_mapping_Validation(target_db):

    Config_testscenarios_dir = r"C:\Users\Dell\PycharmProjects\PythonProject\PythonProject\ETL_Automation_With_Pytest\Config_Testscenarios"

    # List to collect results for all files
    all_table_result_list = []

    logging.info("* Source-to-Target Column Mapping Validation Started *")

    for filename in os.listdir(Config_testscenarios_dir):
        ts_json_file_path = Config_testscenarios_dir + '/' + filename

        try:
            logging.info(f"* Initiated processing of file:{filename} *")

            #Loading SQL queries from all JSON files into python program
            with open(ts_json_file_path, 'r') as ts_SQL_file:
                SQL_Queries = json.load(ts_SQL_file)

            #Capturing Source & Target table names from .json file
            file_type = SQL_Queries["tc_01_Source & Target Details"]["file_type"]
            file_path = SQL_Queries["tc_01_Source & Target Details"]["file_path"]
            t_table_name = SQL_Queries["tc_01_Source & Target Details"]["t_table"]
            logging.info(f"* Source file: {file_path} Target Table: {t_table_name} validations initiated *")

            # source and target queries for columns
            if file_type.lower() == "csv":
                df_source_result = pd.read_csv(file_path)
            elif file_type.lower() == "excel":
                df_source_result = pd.read_excel(file_path)
            elif file_type.lower() == "json":
                df_source_result = pd.read_json(file_path)
            else:
                raise Exception("Unsupported file type")



            t_query = SQL_Queries['tc_05_Verifying Source & Target Columns mapping']['t_table']
            target_cursor = target_db.cursor()
            target_cursor.execute(t_query)
            df_target_result = pd.DataFrame(target_cursor)

            missing_records_in_target = []
            source_pids = df_source_result.iloc[:,0].tolist()
            target_pids = df_target_result.iloc[:,0].tolist()

            for pid in source_pids:
                if pid not in target_pids:
                    missing_records_in_target.append(pid)

            mismatch_records = []

            for i in range(len(df_target_result)):

                # Prevent out-of-bounds access
                if i >= len(df_source_result):
                    break
                source_row = df_source_result.iloc[i].values
                target_row = df_target_result.iloc[i].values

                if any(source_row != target_row):
                        mismatch_records.append(source_row[0])
    #here source_row[0]) - Zero is index number of first value in source_row values (p_id)

            if mismatch_records != [] or missing_records_in_target != []:

                df_mismatch_result = pd.DataFrame(
                    {
                        "Database": ["Source table", "Target table"],
                        "Table_names": [s_table_name, t_table_name],
                        "Result": ["Source & Target tables data not matched!", None],
                        "Status": ["FAIL", None],
                        "Mismatched Source & Target Records": [mismatch_records, None],
                        "Missing Records in Target": [missing_records_in_target, None],
                    }
                    )
            else:
                df_mismatch_result = pd.DataFrame(
                            {
                                "Database": ["Source table", "Target table"],
                                "Table_names": [s_table_name, t_table_name],
                                "Result": ["Source & Target tables data matched!", None],
                                "Status": ["PASS", None],
                                "Mismatched Source & Target Records": [None, None],
                                "Missing Records in Target": [None, None],
                            }
                            )


        #Append the result for the current file to the list
            all_table_result_list.append(df_mismatch_result)

        except Exception as e:
            logging.error(f"Error in file - {filename}: {e}")
            continue

    df_mismatch_results = pd.concat(all_table_result_list, ignore_index=True)
    return df_mismatch_results