import psycopg2
import psycopg2.extras
import pandas as pd
import sys
import pandas.io.sql as sqlio
from sqlalchemy import types, create_engine
import os
from src.helpers import get_unique_indics

param_dic = {
    "host": os.environ.get("HOST"),
    "port": os.environ.get("PORT"),
    "database": os.environ.get("DB"),
    "user": os.environ.get("DB_USERNAME"),
    "password": os.environ.get("DB_PASSWORD"),
}

engine = create_engine(
    "postgresql://"
    + param_dic["user"]
    + ":"
    + param_dic["password"]
    + "@"
    + param_dic["host"]
    + ":"
    + param_dic["port"]
    + "/"
    + param_dic["database"],
    echo=False,
)


def pg_connect(params_dic=param_dic):
    """
    Connect to the PostgreSQL database server.
    """
    conn = None
    try:
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**params_dic)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn


# INIT


def pg_recreate_tables():
    """Run sql file with queries for creating the repository tables."""

    with open("./src/db/create_tables.sql", "r") as f:
        stream = f.read()
        queries = stream.split(";")
    conn = pg_connect()
    curr = conn.cursor()
    for q in queries[:-1]:
        try:
            print(q)
            curr.execute(q)
        except Exception as e:
            print("Query execution failed")
            print(e)
        finally:
            conn.commit()
    curr.close()


# READ


def pg_read(table_name, param_dic=param_dic):
    """
    Read data from the lookup tables
    """
    conn = pg_connect(param_dic)
    query = "select * from {}".format(table_name)
    df = sqlio.read_sql_query(query, conn)
    df = pd.DataFrame(df)

    return df


def pg_read_table_by_indicator(table_name, param_dic=param_dic):
    """
    Read all data by indicators
    """

    conn = pg_connect(param_dic)

    query = f"""SELECT facilitycode,
                       indicatorname,
                       year,
                       month,
                       value
                FROM (SELECT *
                    FROM {table_name}) as "tempdata"
			          LEFT JOIN indicator on tempdata.indicatorcode = indicator.indicatorcode_out;
                """
    df = pd.read_sql(query, con=conn)

    df.columns = ["orgUnit", "dataElement", "year", "month", "value"]
    return df


# WRITE AND UPDATE


def pg_create_indicator(dataelements):

    rename_dict = {
        "identifier": "indicatorname",
        "Keep outliers": "indicatorcode_out",
        "SD": "indicatorcode_std",
        "ICR": "indicatorcode_iqr",
        "Report": "indicatorcode_rep",
    }

    df = pd.DataFrame.from_dict(dataelements, orient="columns").rename(
        columns=rename_dict
    )

    df = df[
        [
            "indicatorcode_out",
            "indicatorcode_std",
            "indicatorcode_iqr",
            "indicatorcode_rep",
            "indicatorname",
        ]
    ]

    # (
    #     pd.DataFrame(dataelements)
    #     .sort_values(0, ignore_index=True)
    #     .reset_index()
    #     .rename(columns={"index": "indicatorcode", 0: "indicatorname"})
    # )

    df.to_sql("indicator", con=engine, if_exists="append", index=False)


def pg_update_indicator(dataelements, param_dic=param_dic):
    """Checks whether any changes were made to the list of data elements , if so wipes the data clean"""

    conn = pg_connect(param_dic)
    cur = conn.cursor()

    try:
        current = pd.read_sql("SELECT * FROM indicator", conn)
    except Exception as e:
        print(e)
        print(
            "Cannot read indicator table - does it exist? Creating table from scratch."
        )
        current = {"indicatorname": []}

    unique_list = get_unique_indics(dataelements)

    changed = set(current["indicatorname"]).symmetric_difference(set(unique_list))

    if len(changed) > 0:

        tables = [
            "main",
            "report",
            "indicator",
        ]

        for table in tables:
            try:
                cur.execute(f"DELETE FROM {table}")
            except Exception as e:
                print(e)
                print(f"Cannot delete data from table {table}")
        cur.execute("commit")

        pg_create_indicator(dataelements)

    cur.close()


def pg_check_table(tablename):
    """Given table name {str}, return {bool} with if exists status"""
    conn = pg_connect(param_dic)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(
            tablename.replace("'", "''")
        )
    )
    if cur.fetchone()[0] == 1:
        cur.close()
        return True

    cur.close()
    return False


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
    # Check if table exists
    if not pg_check_table(table_name):
        # create if not
        df = pd.read_csv(file_path, nrows=0)
        df.to_sql(table_name, con=engine, index=False)

    # delete existing month records
    pg_delete_records(year, month, table_name, param_dic)

    # update table
    pg_write_table(file_path, table_name, param_dic)


def pg_update_location(file_path, param_dic=param_dic):
    """Check if the location file is more extensive than the location table and append new locations"""

    locations = pd.read_sql("SELECT * FROM location;", con=engine)
    locations_update = pd.read_csv(file_path)

    new_locations = pd.merge(locations, locations_update, how="outer", indicator=True)
    new_locations = new_locations[new_locations._merge == "right_only"]
    new_locations = new_locations[["facilitycode", "facilityname", "districtname"]]
    new_locations.to_sql("location", con=engine, if_exists="append", index=False)


def pg_update_pop(file_path, cols, param_dic=param_dic):

    f = open(file_path, "r")

    conn = pg_connect(param_dic)
    cur = conn.cursor()

    drop_query = f"""DROP TABLE IF EXISTS pop;"""
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

    f.close()

    cur.execute("commit")
    cur.close()


######################
#### OUTPUT FILE #####
######################


def pg_final_table(file_path, table_name, engine=engine, param_dic=param_dic):

    conn = pg_connect(param_dic)
    cursor = conn.cursor()

    # drop table if it exists
    cursor.execute("DROP table IF EXISTS {};".format(table_name))
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

    df["date"] = pd.to_datetime(df["date"])

    engine = create_engine(engine_url, echo=False)

    type_dict = {key: types.Integer() for key in df.columns[4:]}
    type_dict["district_name"] = types.VARCHAR(length=200)
    type_dict["facility_id"] = types.VARCHAR(length=200)
    type_dict["facility_name"] = types.VARCHAR(length=500)
    type_dict["date"] = types.Date()

    df.to_sql(
        table_name,
        con=engine,
        chunksize=10000,
        dtype=type_dict,
        if_exists="replace",
        index=False,
    )


def config_to_test_sqlite(df, engine_url):

    engine = create_engine(engine_url, echo=False)

    type_dict = {key: types.VARCHAR(length=300) for key in df.columns}

    df.to_sql(
        "config",
        con=engine,
        chunksize=10000,
        dtype=type_dict,
        if_exists="replace",
        index=False,
    )
