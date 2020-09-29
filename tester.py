import pandas as pd
import numpy as np

USECOLS = list(range(0, 9))

DTYPES = {'Unnamed: 0': int,
          'dataElement': str,
          'period': str,
          'orgUnit': str,
          'categoryOptionCombo': str,
          'attributeOptionCombo': str,
          'value': object,
          'storedBy': str,
          'created': str,
          'lastUpdated': str}

config = pd.read_csv('config/variable_correspondance.csv')

new_files = ['new_main_2020_Jul.csv',
             'new_main_2020_Apr.csv', 'new_main_2020_Feb.csv']

old_files = ['old_main_2019_Jul.csv', 'old_main_2019_Apr.csv', 'old_main_2019_Feb.csv',
             'old_main_2018_Jul.csv', 'old_main_2018_Apr.csv', 'old_main_2018_Feb.csv']


def get_breakdown(config, new_files, old_files):

    config_new = list(config[config['instance'] == 'new']['name'])
    config_old = list(config[config['instance'] == 'old']['name'])

    new_out = pd.DataFrame(columns=['breakdown'])

    for f in new_files:

        df = pd.read_csv(
            f'data/input/dhis2/processed/{f}', usecols=USECOLS, dtype=DTYPES)

        df1 = pd.DataFrame(columns=['breakdown'])

        for x in config_new:
            df1.loc[x, 'breakdown'] = np.sort(
                df[df.dataElement == x]['categoryOptionCombo'].unique())

        new_out = pd.concat([new_out, df1])

    old_out = pd.DataFrame(columns=['breakdown'])

    for f in old_files:

        df = pd.read_csv(
            f'data/input/dhis2/processed/{f}', usecols=USECOLS, dtype=DTYPES)

        df1 = pd.DataFrame(columns=['breakdown'])

        for x in config_old:
            df1.loc[x, 'breakdown'] = np.sort(
                df[df.dataElement == x]['categoryOptionCombo'].unique())

        old_out = pd.concat([new_out, df1])

    new_out['instance'] = 'new'
    old_out['instance'] = 'old'

    out = pd.concat([old_out, new_out])

    return out


breakdown = get_breakdown(
    config, new_files, old_files).reset_index(inplace=True)

print(breakdown.head())
