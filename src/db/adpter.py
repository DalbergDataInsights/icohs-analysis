import psycopg2
import psycopg2.extras
import pandas as pd
import sys
import json
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import os


param_dic = {
    "host": os.environ.get('HOST'),
    "port": os.environ.get('PORT'),
    "database": os.environ.get("DB"),
    "user": os.environ.get("USER"),
    "password": os.environ.get("PASSWORD")
}

engine = create_engine('postgresql://'+param_dic['user']+':'+param_dic['password'] +
                       '@'+param_dic['host']+':'+param_dic['port']+'/'+param_dic['database'], echo=False)


def pg_connect(params_dic=param_dic):
    """
        Connect to the PostgreSQL database server
    """
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn


def pg_read_lookup(table_name, param_dic=param_dic):
    """
        This table reads data from the lookup tables
    """
    conn = pg_connect(param_dic)
    query = "select * from {}".format(table_name)
    df = sqlio.read_sql_query(query, conn)
    df = pd.DataFrame(df)
    df_dict = dict(zip(df.iloc[:, 1], df.iloc[:, 0]))
    return df_dict


def pg_read_table_by_indicator(table_name, indicator=None, param_dic=param_dic):
    """
        This function reads all data by indicators
    """

    conn = pg_connect(param_dic)

    indicator_query = ''

    if indicator is not None:
        # "select indicatorcode from "indicator" where indicatorname = "malaria_tests";"
        indicator_code_query = f'''SELECT "indicatorcode" FROM "indicator" WHERE indicator.indicatorname LIKE '{indicator}';'''
        cursor = conn.cursor()
        cursor.execute(indicator_code_query)

        indicator_code = cursor.fetchone()[0]
        print(indicator_code)
        indicator_query = f"WHERE indicatorcode = '{indicator_code}'"

    query = f'''SELECT districtname,
                       facilityname,
                       indicatorname,
                       year,
                       month,
                       value
                FROM (SELECT *
                     FROM {table_name} {indicator_query}) as "indicators" 
                LEFT JOIN location on indicators.facilitycode = location.facilitycode
			    LEFT JOIN indicator on indicators.indicatorcode = indicator.indicatorcode;
                '''
    df = pd.read_sql(query, con=conn)

    df.columns = ['id', 'orgUnit', 'dataElement', 'year', 'month', 'value']
    return df


def pg_write_lookup(file_path, table_name, param_dic=param_dic):
    """
        This function upload csv to a target lookup table
    """
    f = open(file_path, "r")

    conn = pg_connect(param_dic)
    cur = conn.cursor()

    query = """
            COPY %s FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ','
            """
    try:
        cur.copy_expert(sql=query % table_name, file=f)
        cur.execute("commit")
    except Exception as e:
        print(e)
    cur.close()


def pg_write_table(file_path, table_name, param_dic=param_dic):
    """
        This function upload csv to a target table
    """
    f = open(file_path, "r")

    conn = pg_connect(param_dic)
    cur = conn.cursor()

    query = f"""
        COPY {table_name} (facilitycode, indicatorcode, year, month, value) FROM STDIN WITH (FORMAT CSV)
    """
    cur.copy_expert(sql=query, file=f)

    cur.execute("commit")
    cur.close()


def pg_delete_records(year, month, table_name, param_dic=param_dic):
    """
        Delete by year and month
    """
    conn = pg_connect(param_dic)
    cur = conn.cursor()

    query = f"""
        DELETE FROM {table_name} WHERE month = '{month}' and year = {year};
        """
    cur.execute(query)
    cur.execute("commit")
    cur.close()


def pg_update_write(year, month, file_path, table_name, param_dic=param_dic):
    """
        This function deletes the months data and then inserts in new data
    """
    pg_delete_records(year, month, table_name, param_dic)

    pg_write_table(file_path, table_name, param_dic)

  ######################
  #### OUTPUT FILE #####
  ######################


def pg_final_table(file_path, table_name, engine=engine, param_dic=param_dic):

    conn = pg_connect(param_dic)
    cursor = conn.cursor()

    # drop table if it exists
    cursor.execute('DROP table IF EXISTS {};'.format(table_name))
    conn.commit()

    # create table
    df = pd.read_csv(file_path)
    df.head(0).to_sql(table_name, con=engine, index=False)

    # Insert data
    f = open(file_path)
    query = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """
    cursor.copy_expert(sql=query % table_name, file=f)
    conn.commit()
    cursor.close()
