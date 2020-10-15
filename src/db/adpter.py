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

# READ


def pg_read_lookup(table_name, getdict=True, param_dic=param_dic):
    """
        This table reads data from the lookup tables
    """
    conn = pg_connect(param_dic)
    query = "select * from {}".format(table_name)
    df = sqlio.read_sql_query(query, conn)
    df = pd.DataFrame(df)

    if getdict is True:
        df = dict(zip(df.iloc[:, 1], df.iloc[:, 0]))

    return df


def pg_read_table_by_indicator(table_name, param_dic=param_dic):
    """
        This function reads all data by indicators
    """

    conn = pg_connect(param_dic)

    # TODO : change name to code  and clarify bug

    query = f'''SELECT facilitycode,
                       indicatorname,
                       year,
                       month,
                       value
                FROM (SELECT *
                    FROM {table_name}) as "tempdata" 
			        LEFT JOIN indicator on tempdata.indicatorcode = indicator.indicatorcode;
                '''
    df = pd.read_sql(query, con=conn)

    df.columns = ['orgUnit', 'dataElement', 'year', 'month', 'value']
    return df

# WRITE AND UPDDATE


def pg_write_lookup(file_path, table_name, param_dic=param_dic):

    # TODO Check for bug when asssing idicators
    """
        Check if any new indicators/facilities were added and upload new ones to a target lookup table
    """
    # Check the status of teh lookup table in the database, compare with latest user input

    conn = pg_connect(param_dic)

    df_current = pd.read_sql("SELECT * FROM %s" % table_name, conn)
    df_new = pd.read_csv(file_path)

    # TODO review that inelegant piece of code

    if table_name == 'indicator':
        col1 = 'indicatorname'
        col2 = '0'
    elif table_name == 'location':
        col1 = 'facilitycode'
        col2 = col1

    current = set(df_current[col1].unique())
    new = set(df_new[col2].unique())
    add = new.difference(current)

    # If any additions, add to the lookup

    if len(add) > 0:
        # TODO : delete this to_csv step, and revise teh query accordingly
        # TODO : Double check this will only add what I need and note try to replace what is there

        df_add = df_new[df_new[col2].isin(add)]
        file_path_add = f'{file_path[:-4]}_addition.csv'

        df_add.to_csv(file_path_add, index=False)

        cur = conn.cursor()

        with open(file_path_add, "r") as f:
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

        os.remove(file_path_add)


def pg_delete_lookup(file_path, table_name, param_dic=param_dic):
    """
        Check if any new indicators/facilities were remove and delete those from target lookup table
    """

    conn = pg_connect(param_dic)

    df_current = pd.read_sql("SELECT * FROM %s" % table_name, conn)
    df_new = pd.read_csv(file_path)

    # TODO review that inelegant piece of code

    if table_name == 'indicator':
        col1 = 'indicatorname'
        col2 = '0'
    elif table_name == 'location':
        col1 = 'facilitycode'
        col2 = col1

    current = set(df_current[col1].unique())
    new = set(df_new[col2].unique())
    remove = tuple(current.difference(new))

    if len(remove) > 0:
        cur = conn.cursor()

        query = f"""DELETE FROM {table_name} WHERE {col1} in {remove};"""

        cur.execute(query)
        cur.execute("commit")
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

    query = f"""DELETE FROM {table_name} WHERE month = '{month}' and year = {year};"""

    cur.execute(query)
    cur.execute("commit")
    cur.close()


def pg_update_write(year, month, file_path, table_name, param_dic=param_dic):
    """
        This function deletes the months data and then inserts in new data
    """
    pg_delete_records(year, month, table_name, param_dic)

    pg_write_table(file_path, table_name, param_dic)


def pg_update_pop(file_path, param_dic=param_dic):

    f = open(file_path, "r")

    conn = pg_connect(param_dic)
    cur = conn.cursor()

    delete_query = f"""DELETE FROM pop;"""
    cur.execute(delete_query)

    write_query = f"""
        COPY pop (DistrictName, year, Male, Female, Total, childbearing_age, pregnants, not_pregnant, births, u1, u5, u15, suspect_tb) FROM STDIN WITH (FORMAT CSV)
    """
    cur.copy_expert(sql=write_query, file=f)

    cur.execute("commit")
    cur.close()

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
