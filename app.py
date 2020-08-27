
from src.pipeline import clean, process
from src.helpers import INDICATORS, make_note
import os
import pandas as pd
import numpy as np
from datetime import datetime

from dotenv import load_dotenv, find_dotenv  # NOQA: E402
load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402
from src.db import adpter as db  # NOQA: E402

START_TIME = datetime.now()
VAR_CORR = pd.read_csv(INDICATORS['var_correspondence_data'])

if __name__ == '__main__':

    make_note('Starting the pipeline', START_TIME)

    # Setting  up the lookup table in teh database

    pd.DataFrame(VAR_CORR['identifier'].unique())\
        .to_csv('data/temp/indicators.csv')
    db.pg_write_lookup('data/temp/indicators.csv', 'indicator')

    db.pg_write_lookup(INDICATORS['name_district_map'], 'location')

    # TODO: Making sure the data we need is here with an assert
    # Checking that both a reporting and a main data files are there

    files = os.listdir(INDICATORS['raw_data'])

    # cleaning the data and writing it to the database file by file

    for f in files:

        raw_path = INDICATORS['raw_data']+f
        processed_path = INDICATORS['processed_data']+f

        # Clean the data

        df = clean.clean(raw_path)

        # Send it to a temporary csv

        (temp_csv_path,
         year,
         month,
         table) = clean.map_to_temp(raw_path,
                                    db.pg_read_lookup('indicator'),
                                    df)

        # Write the clean data to the database

        db.pg_update_write(year, month, temp_csv_path, table)

        # Move orginal data from the 'raw' to the 'prcessed' folder

        # TODO : commented out for testing purposes
        clean.move_csv_files(raw_path, processed_path)

        make_note(f'Cleaning and database insertion done for file {f}',
                  START_TIME)

    # Processing the data

    process.process(db.pg_read_table_by_indicator('main'),
                    db.pg_read_table_by_indicator('report'))

    # Writing to the database
    db.pg_final_table(INDICATORS['report_data'],
                      'report_output')
    db.pg_final_table(INDICATORS['outlier_data'],
                      'outlier_output')
    db.pg_final_table(INDICATORS['std_no_outlier_data'],
                      'std_no_outlier_output')
    db.pg_final_table(INDICATORS['iqr_no_outlier_data'],
                      'iqr_no_outlier_output')

    # recording measured time
    make_note('Pipeline done', START_TIME)
