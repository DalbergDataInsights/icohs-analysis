
from datetime import datetime

from src.helpers import INDICATORS, make_note
from src.pipeline import clean, process
from src.db import adpter as db 

START_TIME = datetime.now()

if __name__ == '__main__':

    # start measuring time
    make_note('Starting the pipeline', START_TIME)

    # cleaning the data
    clean_data = clean.clean(INDICATORS['new_instance_data'],
                             INDICATORS['old_instance_data'],
                             INDICATORS['new_instance_data_report'],
                             INDICATORS['old_instance_data_report'])

    
    clean_data['districts']= clean_data['districts'].map(db.pg_read_lookup('district'))
    clean_data['dataElement']= clean_data['dataElement'].map(db.pg_read_lookup('indicator'))
    clean_data['orgUnit']= clean_data['orgUnit'].map(db.pg_read_lookup('facility'))

    make_note('Backing up csv', START_TIME)
    # order of clean csv file [district, facility, indicator, year, month , value]
    clean_data.to_csv('data/data_clean.csv', index=False,header=False)
    del clean_data
    make_note('Writing to the database', START_TIME)
    db.pg_write_table('data/data_clean.csv','repository')

    reporting, outlier_data, std_no_outlier_data, iqr_no_outlier_data = process.process(
        db.pg_read_table_by_indicator())

    # recording measured time
    make_note('Pipeline done', START_TIME)
