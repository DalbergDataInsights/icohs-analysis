
from src.pipeline import clean, process, indic
from src.helpers import INDICATORS, make_note, get_unique_indics
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

    # make_note('Starting the pipeline', START_TIME)

    # Adding any new indiactors/facilities to the lookup table

    pd.DataFrame(get_unique_indics(VAR_CORR)).to_csv(
        INDICATORS['indicators_map'])

    db.pg_write_lookup(file_path=INDICATORS['indicators_map'],
                       table_name='indicator')

    db.pg_write_lookup(file_path=INDICATORS['name_district_map'],
                       table_name='location')

    # Adding the population data

    cols = clean.clean_pop_to_temp(INDICATORS['pop'], INDICATORS['pop_perc'])

    db.pg_update_pop('data/temp/pop.csv', cols)

    # cleaning the data and writing it to the database file by file

    files = os.listdir(INDICATORS['raw_data'])

    for f in files:

        raw_path = INDICATORS['raw_data']+f
        processed_path = INDICATORS['processed_data']+f

        # Clean the data

        df = clean.clean(raw_path=raw_path)

        # Send it to a temporary csv

        (temp_csv_path,
         year,
         month,
         table) = clean.map_to_temp(raw_path=raw_path,
                                    map=db.pg_read('indicator'),
                                    clean_df=df)

        # Write the clean data to the database

        db.pg_update_write(year=year,
                           month=month,
                           file_path=temp_csv_path,
                           table_name=table)

        # Move orginal data from the 'raw' to the 'prcessed' folder

        clean.move_csv_files(raw_path, processed_path)

        make_note(f'Cleaning and database insertion done for file {f}',
                  START_TIME)

    # Removing any redundant indicators/facilities from the lookup tables

    db.pg_delete_lookup(file_path=INDICATORS['indicators_map'],
                        table_name='indicator')

    db.pg_delete_lookup(file_path=INDICATORS['name_district_map'],
                        table_name='location')

    # Processing the data (creating outliers excluded and report tables)

    process.process(main=db.pg_read_table_by_indicator('main'),
                    report=db.pg_read_table_by_indicator('report'),
                    location=db.pg_read('location', getdict=False))

    # Writing to the database

    db.pg_final_table(file_path=INDICATORS['rep_data'],
                      table_name='report_output')

    db.pg_final_table(file_path=INDICATORS['out_data'],
                      table_name='outlier_output')

    db.pg_final_table(file_path=INDICATORS['std_data'],
                      table_name='std_no_outlier_output')

    db.pg_final_table(file_path=INDICATORS['iqr_data'],
                      table_name='iqr_no_outlier_output')

    # recording measured time
    make_note('Pipeline done', START_TIME)

    # Optional transformation to indicators (sealed from the rest on purpose)

    pop = db.pg_read('pop', getdict=False)

    outlier = db.pg_read('outlier_output', getdict=False)
    indic.transform_to_indic(outlier, pop, 'out')

    std = db.pg_read('std_no_outlier_output', getdict=False)
    indic.transform_to_indic(std, pop, 'std')

    iqr = db.pg_read('iqr_no_outlier_output', getdict=False)
    indic.transform_to_indic(iqr, pop, 'iqr')

    report = db.pg_read('report_output', getdict=False)
    indic.transform_to_indic(report, pop, 'rep')

    indic.pass_on_config()
