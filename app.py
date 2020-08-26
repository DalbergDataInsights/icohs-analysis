
from src.pipeline import clean, process
from src.helpers import INDICATORS, make_note
import os
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv, find_dotenv  # NOQA: E402
load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402
from src.db import adpter as db  # NOQA: E402

START_TIME = datetime.now()
VAR_CORR = pd.read_csv(INDICATORS['var_correspondence_data'])

if __name__ == '__main__':

    make_note('Starting the pipeline', START_TIME)

    db.pg_write_lookup(INDICATORS['name_district_map'], 'location')

    pd.DataFrame(VAR_CORR['identifier'].unique())\
        .to_csv('data/temp/indicators')
    db.pg_write_lookup('data/temp/', 'indicator')

    # cleaning the data
    files = os.listdir(INDICATORS['raw_data'])

    for f in files:
        clean.clean(INDICATORS['raw_data']+f, INDICATORS['processed_data']+f)
        make_note(f'Cleaning done for file {f}', START_TIME)

    # Processing the data

    # reporting, outlier_data, std_no_outlier_data, iqr_no_outlier_data = process.process(
        # db.pg_read_table_by_indicator())

    # recording measured time
    make_note('Pipeline done', START_TIME)
