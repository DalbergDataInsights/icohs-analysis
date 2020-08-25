
from datetime import datetime

from src.helpers import INDICATORS, make_note
from src.pipeline import clean, process
from src.db import adpter as db
import pandas as pd
from dotenv import load_dotenv, find_dotenv

START_TIME = datetime.now()

if __name__ == '__main__':

    make_note('Starting the pipeline', START_TIME)

    load_dotenv(find_dotenv())

    db.pg_write_lookup(INDICATORS['name_district_map'], 'location')

    # cleaning the data
    files = os.listdir(INDICATORS['raw_data'])

    for f in files:
        clean.clean(INDICATORS['raw_data']+f, INDICATORS['processed_data'])
        make_note(f'Cleaning done for file {f}', START_TIME)

    # Processing the data

    reporting, outlier_data, std_no_outlier_data, iqr_no_outlier_data = process.process(
        db.pg_read_table_by_indicator())

    # recording measured time
    make_note('Pipeline done', START_TIME)
