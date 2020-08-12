###########################
#     Initialize file     #
###########################


# For sample :

# l.305 csv export
# l.335,337 chnage sample cols
# l.371 csv export
# l.459 valid ids
# engine sample path


import pandas as pd
import sys
import glob
import json
import os
from pathlib import Path
import engine
import numpy as np
import scipy
import datetime
from scipy import stats

from pipeline_func.clean import clean
from pipeline_func.process import process

ENGINE = engine.get_engine()
ENGINE_rename = engine.get_engine()

module_json = 'config/indicators.json'
with open(module_json) as f:
    for section in json.load(f):
        for p in section['data']:
            ENGINE[p['identifier']] = p['value']

startTime = datetime.datetime.now()


#########################################
#     Get data in the right format      #
#########################################

# Data paths

new_dhis_path = ENGINE['new_instance_data']
old_dhis_path = ENGINE['old_instance_data']
new_dhis_report_path = ENGINE['new_instance_data_report']
old_dhis_report_path = ENGINE['old_instance_data_report']
var_correspondance_path = ENGINE['var_correspondance_data']

if __name__ == "__main__":
    '''Executes the functiosn defined above'''

    clean_tall_data = clean(new_dhis_path, old_dhis_path,
                            new_dhis_report_path, old_dhis_report_path)

    (processed_wide_data, report_data, outlier_data,
     std_no_outlier_data, iqr_no_outlier_data) = process(clean_tall_data)

    print('DONE')

print(datetime.datetime.now() - startTime)
