import csv
import psycopg2
import pandas as pd
import json 
import sys

def get_engine(json_file, sec):
    with open(json_file) as f:
        ENGINE = dict((p['identifier'], p['value']) for section in json.load(f) 
                                                    for p in section[sec])
        return ENGINE


def pg_load_table(file_path, table_name, dname, host, port, user, pwd):
    """
        This fuction upload csv to a target table
    """
    try:
        conn = pyscopg2.connect(dbname=dbname,host=host,port=port, user=user, password=pwd)
        print("Connecting to Database")
        cur = conn.cursor()
        f = open(file_path, "r")

        #Truncate the table first
        cur.execute("Truncate{} Cascade;".format(table_name))
        print("Truncated{}".format(table_name))

        #Load table from the file 
        cur.copy_expert("copy{} from STDIN CSV HEADER QUOTE '\"'".format(table_name),f)
        cur.execute("commit")
        print("Loaded data into{}".format(table_name))
        connn.close()
        print("DB connection closed")

    except Exception as e:
        print("Error:{}".format(str(e)))
        sys.exit(1)


if __name__ == "__main__":
    ENGINE = get_engine("config/cred.json", "credentialsEngine")

    iqr_table = ENGINE['no_outliers_iqr']
    std_table = ENGINE['no_outliers_std']
    report_table = ENGINE['reporting']
    withOutliers_table = ENGINE['with_outliers']
    dbname = ENGINE['dbname']
    host = ENGINE['host']
    port = ENGINE['port']
    user = ENGINE['Username']
    pwd = ENGINE['password']

    #csv files 
    files_csv = get_engine("config/indicators.json", "data")
    report_file = files_csv['report_data']
    iqr_file = files_csv['iqr_no_outlier_data']
    std_file = files_csv['std_no_outlier_data']
    withOutliers_file = files_csv['outlier_data']

    pg_load_table(report_file, report_table, dbname, host, port, user, pwd)
    pg_load_table(withOutliers_file, withOutliers_table, dbname, host, port, user, pwd)
    pg_load_table(std_file, std_table, dbname, host, port, user, pwd)
    pg_load_table(iqr_file, iqr_table, dbname, host, port, user, pwd)




