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


def pg_update_dataelements(file_path, table_name, param_dic=param_dic):

    # TODO Check for bug when asssing idicators
    """
        Check if any new indicators/facilities were added and upload new ones to a target lookup table
    """
    # Check the status of teh lookup table in the database, compare with latest user input

    conn = pg_connect(param_dic)
    cur = conn.cursor()

    df_current = pd.read_sql("SELECT * FROM %s" % table_name, conn)
    df_new = pd.read_csv(file_path)

    # TODO review that inelegant piece of code

    if table_name == 'indicator':
        col1 = 'indicatorname'
        col2 = '0'
        to_update = 'indicatorcode'
    elif table_name == 'location':
        col1 = 'facilitycode'
        col2 = col1
        to_update = 'facilitycode'

    current = set(df_current[col1].unique())
    new = set(df_new[col2].unique())

    # Check for new data elements - if any additions, add to the lookup

    add = new.difference(current)
    if len(add) > 0:

        df_add = df_new[df_new[col2].isin(add)]
        df_add = df_add[[df_add.columns[-1]]]

        try:
            i = df_current.indicatorcode.astype(int).max()+1
            for x in df_add.index:
                sql = f"INSERT INTO {table_name} VALUES ('{i}','{df_add.loc[x,df_add.columns[-1]]}');"
                cur.execute(sql)
                cur.execute("commit")
                i = i+1

        except Exception as e:
            print(e)

    # Now check for redundant data elements

    remove = current.difference(new)
    df_remove = df_current[df_current[col1].isin(remove)]
    codes_remove = tuple(df_remove[to_update].unique())

    # then drop from main table whatver missing indicators

    if len(remove) > 0:

        query = f"""DELETE FROM main WHERE {to_update} in {codes_remove};"""

        cur.execute(query)
        cur.execute("commit")

        # then dropfrom the indicator table

        query = f"""DELETE FROM {table_name} WHERE {to_update} in {codes_remove};"""

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
