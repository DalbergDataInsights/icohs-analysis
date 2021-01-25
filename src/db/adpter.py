import psycopg2
import psycopg2.extras
import sqlite3
import pandas as pd
import sys
import json
import pandas.io.sql as sqlio
from sqlalchemy import types, create_engine
import os


param_dic = {
    "host": os.environ.get('HOST'),
    "port": os.environ.get('PORT'),
    "database": os.environ.get("DB"),
    "user": os.environ.get("DB_USERNAME"),
    "password": os.environ.get("DB_PASSWORD")
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


def pg_read(table_name, getdict=True, param_dic=param_dic):
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


def pg_update_indicator(dataelements, param_dic=param_dic):
    '''Checks whether any changes were made to the list of data elements , if so wipes the data clean'''

    conn = pg_connect(param_dic)
    cur = conn.cursor()

    current = pd.read_sql("SELECT * FROM indicator", conn)

    changed = set(current['indicatorname']
                  ).symmetric_difference(set(dataelements))

    if len(changed) > 0:

        try:

            queries = ["DELETE FROM main",
                       "DELETE FROM report",
                       "DELETE FROM indicator"]

            for q in queries:
                cur.execute(q)
                cur.execute("commit")

            df = pd.DataFrame(dataelements)\
                .sort_values(0, ignore_index=True)\
                .reset_index()\
                .rename(columns={'index': 'indicatorcode', 0: 'indicatorname'})

            df.to_sql('indicator', con=engine, if_exists='append', index=False)

        except Exception as e:
            print(e)

    cur.close()


def pg_update_location(file_path, param_dic=param_dic):

    print('check')


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


def pg_update_pop(file_path, cols, param_dic=param_dic):

    f = open(file_path, "r")

    conn = pg_connect(param_dic)
    cur = conn.cursor()

    drop_query = f"""DROP table pop;"""
    cur.execute(drop_query)

    col_string = " float8 NULL, ".join(cols.split(", "))

    create_query = f"""CREATE table pop (
        id serial NOT NULL,
	    district_name varchar(255) NOT NULL,
	    "year" int2 NULL,
	    male int4 NULL,
	    female int4 NULL,
	    total int4 NULL,
        {col_string} float8 NULL);"""

    cur.execute(create_query)

    write_query = f"""
        COPY pop (district_name, year, male, female, total, {cols}) FROM STDIN WITH (FORMAT CSV)
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


def output_to_test_sqlite(df, table_name, engine_url):

    df['date'] = pd.to_datetime(df['date'])

    engine = create_engine(engine_url, echo=False)

    type_dict = {key: types.Integer() for key in df.columns[4:]}
    type_dict['district_name'] = types.VARCHAR(length=200)
    type_dict['facility_id'] = types.VARCHAR(length=200)
    type_dict['facility_name'] = types.VARCHAR(length=500)
    type_dict['date'] = types.Date()

    df.to_sql(table_name, con=engine, chunksize=10000,
              dtype=type_dict, if_exists='replace', index=False)


def config_to_test_sqlite(df, engine_url):

    engine = create_engine(engine_url, echo=False)

    type_dict = {key: types.VARCHAR(length=300) for key in df.columns}

    df.to_sql('config', con=engine, chunksize=10000,
              dtype=type_dict, if_exists='replace', index=False)
