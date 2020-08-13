
from datetime import datetime

from src.helpers import INDICATORS, make_note
from src.pipeline import clean, process

START_TIME = datetime.now()

if __name__ == '__main__':

    # start measuring time
    make_note('Starting the pipeline', START_TIME)

    # cleaning the data
    clean_data = clean.clean(INDICATORS['new_instance_data'],
                             INDICATORS['old_instance_data'],
                             INDICATORS['new_instance_data_report'],
                             INDICATORS['old_instance_data_report'])

    reporting, outlier_data, std_no_outlier_data, iqr_no_outlier_data = process.process(
        clean_data)

    # recording measured time
    make_note('Pipeline done', START_TIME)
