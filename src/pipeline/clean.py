###########################
#     Initialize file     #
###########################


# For sample :

# engine sample path

import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import scipy
from scipy import stats

from src.helpers import make_note, INDICATORS

from dotenv import load_dotenv, find_dotenv  # NOQA: E402
load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402
from src.db import adpter as db  # NOQA: E402


###################################
#     Define functions used       #
###################################


# Get user-input data

VAR_CORR = pd.read_csv(INDICATORS['var_correspondence_data'])

USECOLS = list(range(0, 10))

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

# TODO : define period to clean based on latest API query

FACILITY_IDS = (pd.read_csv(INDICATORS['name_district_map'])['facilitycode']
                .unique()
                .tolist())

START_TIME = datetime.now()

# Extracting reporting data


def get_reporting_data(path, instance):

    month_dict = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
                  '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
                  '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}

    cols = ['Unnamed: 0',
            'orgunitlevel1',
            'orgunitlevel2',
            'orgunitlevel3',
            'orgunitlevel4',
            'orgunitlevel5',
            'organisationunitname',
            'organisationunitcode',
            'organisationunitdescription',
            'periodid',
            'periodname',
            'periodcode',
            'perioddescription']

    # Get the right data
    df = pd.read_csv(path, dtype='object')
    metrics = list(VAR_CORR[(VAR_CORR['domain'] == 'REPORT')
                            & (VAR_CORR['instance'] == instance)]['name'])

    for x in metrics:
        df[x] = pd.to_numeric(df[x], errors='coerce')

    make_note(str(instance)+' reporting data loaded', START_TIME)

    # Formatting dates
    df['year'] = df['periodcode'].astype('str')\
                                 .apply(lambda x: x[:4])
    df['month'] = df['periodcode'].astype('str')\
                                  .apply(lambda x: x[-2:])\
                                  .replace(month_dict)
    df.rename(columns={'organisationunitid': 'orgUnit'}, inplace=True)
    df.set_index(['orgUnit', 'year', 'month'],
                 drop=True,
                 inplace=True)

    # Dropping unused columns and renaming
    df.drop(cols, axis=1, inplace=True)
    df1 = df.copy()\
            .stack(dropna=False)\
            .reset_index()
    df1.rename(columns={0: 'value', 'level_3': 'dataElement'}, inplace=True)

    return df1

# Extracting our target data


def get_data(path, instance):
    data = pd.read_csv(path, usecols=USECOLS, dtype=DTYPES)

    new_indic_list = VAR_CORR[VAR_CORR['instance'] == instance]['name']\
        .tolist()
    new_df = data[data["dataElement"].isin(new_indic_list)]\
        .reset_index(drop=True)

    del data

    new_df['value'] = pd.to_numeric(new_df['value'], errors='coerce')
    new_df = new_df.groupby(
        ['dataElement', 'orgUnit', 'period'], as_index=False).agg({'value': 'sum'})
    new_df = new_df[['period', 'orgUnit', 'value', 'dataElement']]

    return new_df


# Adding composite indicators

def get_variable_addition_dict(instance):
    '''build a dict with target vars as keys and original vars to add up as values'''

    target_dict = {}

    df = VAR_CORR[VAR_CORR['instance'] == instance]
    df = df[df.duplicated('identifier', keep=False)]
    df = df[['identifier', 'name']]

    for x in df['identifier'].unique().tolist():
        target_dict[x] = df[df['identifier'] == x]['name'].tolist()

    return target_dict


def compute_indicators(df, indic_name, indicator_list):
    '''Compute new vars by summing original vars, and drop original vars '''

    df_new = df[df['dataElement'].isin(indicator_list)]
    df_new = df_new.groupby(['period', 'orgUnit'],
                            as_index=False).agg({'value': 'sum'})
    df_new['dataElement'] = indic_name

    df = pd.concat([df, df_new])
    df.reset_index(drop=True, inplace=True)
    df = df[~df['dataElement'].isin(indicator_list)]

    return df

# Transforming dates


def process_date_monthly(df):

    month_dict = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
                  '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
                  '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}

    df['weekly'] = df.period.str.find('W')

    df_m = df[df['weekly'] == -1].copy()
    df_m['year'] = df_m['period'].astype('str').apply(lambda x: x[:4])
    df_m['month'] = df_m['period'].astype('str').apply(
        lambda x: x[-2:]).replace(month_dict)
    df_m.drop(['weekly', 'period'], inplace=True, axis=1)

    return df_m


def process_date_weekly(df):

    month_dict = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
                  5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
                  9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

    df['weekly'] = df.period.str.find('W')

    df_w = df[df['weekly'] != -1].copy()
    df_w[['year', 'week']] = df_w.period.str.split("W", expand=True)
    df_w['datetime'] = pd.to_datetime(
        df_w.year + '/' + df_w.week + '/1', format='%G/%V/%u')
    df_w['month'] = df_w['datetime'].dt.month.replace(month_dict)
    df_w.drop(['week', "weekly", 'datetime', 'period'], inplace=True, axis=1)
    df_w['year'] = df_w.year.astype('str')

    return df_w


def process_date(df):
    df_m = process_date_monthly(df)
    df_w = process_date_weekly(df)
    df_new = pd.concat([df_m, df_w])
    return df_new


def add_indicators(file_path, instance):
    make_note(f'Creating additional indicators for {instance}', START_TIME)
    add_dict = get_variable_addition_dict(instance)
    dhis_df = get_data(file_path, instance)

    for indicator in add_dict.keys():
        dhis_df = compute_indicators(
            dhis_df, indicator, add_dict.get(indicator))

    dhis_df = process_date(dhis_df)

    make_note(f'Data transformed and cleaned for {instance}', START_TIME)

    return dhis_df

# Putting together cleaning steps


def clean_raw_file(raw_path):
    '''Take one file, checks whether it fits expected format, and clean it'''

    renaming_dict = dict(zip(VAR_CORR.name, VAR_CORR.identifier))

    # Check file name format

    f = raw_path.split('/')[-1]
    instance = f.split('_')[0]
    datatype = f.split('_')[1]

    assert datatype in [
        'main', 'report'], f'Unexpected data type in file name for {f}: file should be of the format [instance_datatype_YYYYMmm], e.g. new_main_2020Apr'

    assert instance in [
        'new', 'old'], f'Unexpected dhis2 instance in file name for {f}: file should be of the format [instance_datatype_YYYYMmm], e.g. new_main_2020Apr'

    # import file and get to standard format

    if datatype == 'main':
        df = add_indicators(raw_path, instance)
    elif datatype == 'report':
        df = get_reporting_data(raw_path, instance)

    assert df['year']\
        .nunique() == 1, f'Data for several years found in file {f}'
    assert df['month']\
        .nunique() == 1, f'Data for several months found in file {f}'

    make_note(f'data imported for file {f}', START_TIME)

    # cleaning formatted table

    df['dataElement'].replace(renaming_dict, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df\
        .groupby(["dataElement", 'orgUnit', "year", "month"], as_index=False)\
        .agg({'value': 'sum'})
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    df = df[df['orgUnit'].isin(FACILITY_IDS)]

    return df


def insert_clean_data(clean_df, raw_path):

    # TODO : Put back the commented out code clarify how these function will be affected by the fact we process one file at a time

    f = raw_path.split('/')[-1]
    f_short = f[-4]

    indicator_map = db.pg_read_lookup('indicator')
    clean_df['dataElement'] = clean_df['dataElement'].map(indicator_map)

    facility_map = db.pg_read_lookup('location')
    clean_df['orgUnit'] = clean_df['orgUnit'].map(facility_map)

    clean_df[['orgUnit', 'dataElement', 'year', 'month', 'value']].to_csv(
        f'data/temp/{f_short}_clean.csv', index=False, header=False)

    db.pg_write_table(f'data/temp/{f_short}_clean.csv', 'repository')


def move_csv_files(raw_path, processed_path):
    os.rename(raw_path, processed_path)

#########################
#     Run functions     #
#########################


# new_dhis_path, old_dhis_path, new_dhis_report_path, old_dhis_report_path):
def clean(raw_path, processed_path):

    file_name = raw_path.split('/')[-1]
    make_note(f'Starting the cleaning process for {file_name}', START_TIME)

    clean_df = clean_raw_file(raw_path)

    insert_clean_data(clean_df, raw_path)

    move_csv_files(raw_path, processed_path)

    make_note('full data import and cleaning done', START_TIME)
