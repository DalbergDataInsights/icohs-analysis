from src.pipeline import clean, process, indic
from src.helpers import INDICATORS, make_note, get_unique_indics
import os
from datetime import datetime
import json

from dotenv import load_dotenv, find_dotenv  # NOQA: E402

load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402

from src.db import adpter as db  # NOQA: E402
from src.api.api_push import Dhis  # NOQA: E402

START_TIME = datetime.now()

with open(INDICATORS["data_config"], "r", encoding="utf-8") as f:
    VAR_CORR = json.load(f)


def run():

    make_note("Starting the pipeline", START_TIME)

    # Adding any new indicators / facilities to the lookup table

    db.pg_update_indicator(dataelements=VAR_CORR)
    db.pg_update_location(file_path=INDICATORS["name_district_map"])

    # Adding the population data

    cols = clean.clean_pop_to_temp(INDICATORS["pop"], INDICATORS["pop_perc"])

    db.pg_update_pop("data/temp/pop.csv", cols)

    # cleaning the data and writing it to the database file by file

    files = os.listdir(INDICATORS["raw_data"])

    for f in files:

        raw_path = INDICATORS["raw_data"] + f
        processed_path = INDICATORS["processed_data"] + f

        # Clean the data

        df = clean.clean(raw_path=raw_path)

        # Send it to a temporary csv

        (temp_csv_path, year, month, table) = clean.map_to_temp(
            raw_path=raw_path, map=db.pg_read("indicator"), clean_df=df
        )

        # Write the clean data to the database

        db.pg_update_write(
            year=year, month=month, file_path=temp_csv_path, table_name=table
        )

        # Move original data from the 'raw' to the 'processed' folder

        clean.move_csv_files(raw_path, processed_path)

        make_note(f"Cleaning and database insertion done for file {f}", START_TIME)

    # Processing the data (creating outliers excluded and report tables)

    process.process(
        main=db.pg_read_table_by_indicator("main"),
        report=db.pg_read_table_by_indicator("report"),
        location=db.pg_read("location"),
    )

    # Writing to the database

    db.pg_final_table(file_path=INDICATORS["rep_data"], table_name="report_output")

    db.pg_final_table(file_path=INDICATORS["out_data"], table_name="outlier_output")

    db.pg_final_table(
        file_path=INDICATORS["std_data"], table_name="std_no_outlier_output"
    )

    db.pg_final_table(
        file_path=INDICATORS["iqr_data"], table_name="iqr_no_outlier_output"
    )

    # recording measured time
    make_note("Pipeline done", START_TIME)

    # Send off to DHIS2

    api = Dhis(
        os.environ.get("API_USERNAME"),
        os.environ.get("API_PASSWORD"),
        "https://repo.hispuganda.org/repo/api",
    )

    for output in [
        "outlier_output",
        "std_no_outlier_output",
        "iqr_no_outlier_output",
        "report_output",
    ]:

        make_note(f"Reformatting data for the DHIS2 repo", START_TIME)

        df = db.pg_read(output)
        df = indic.transform_for_dhis2(
            df=df, map=db.pg_read("indicator"), outtype=output[:3]
        )
        filepath = f"data/temp/{output}_dhis.csv"
        df.to_csv(filepath, index=False)
        make_note(f"Publishing {output} to the DHIS2 repo", START_TIME)

        api.post([filepath])

    # Transformation to indicators (sealed from the rest)

    pop = db.pg_read("pop")

    for output in [
        ("outlier_output", "out"),
        ("std_no_outlier_output", "std"),
        ("iqr_no_outlier_output", "iqr"),
        ("report_output", "rep"),
    ]:
        data = db.pg_read(output[0])
        indic.transform_to_indic(data, pop, output[1])

    indic.pass_on_config()
