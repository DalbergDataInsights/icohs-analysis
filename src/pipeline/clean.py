###########################
#     Initialize file     #
###########################


# For sample :

# engine sample path

import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import scipy
from scipy import stats

from src.helpers import make_note, INDICATORS


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

PERIODS = ['2018Jan', '2018Feb', '2018Mar', '2018Apr',
           '2018May', '2018Jun', '2018Jul', '2018Aug',
           '2018Sep', '2018Oct', '2018Nov', '2018Dec',
           '2019Jan', '2019Feb', '2019Mar', '2019Apr',
           '2019May', '2019Jun', '2019Jul', '2019Aug',
           '2019Sep', '2019Oct', '2019Nov', '2019Dec',
           '2020Jan', '2020Feb', '2020Mar', '2020Apr',
           '2020May', '2020Jun']

START_TIME = datetime.now()

# Extracting reporting data and facility names correspondences


def get_path_list(instance, filetype, period):
    paths = []
    if type(period) is 'list':
        for x in period:
            path = instance+'/'+filetype+'/'+x+'.csv'
            paths.append(path)
    else:
        path = instance+'/'+filetype+'/'+period+'.csv'
        paths.append(path)

    return paths


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

# Selecting target indicators


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

# Loops through each month to get the data

# def put_together_get_data ()

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


#########################
#     Run functions     #
#########################


def clean(new_dhis_path, old_dhis_path, new_dhis_report_path, old_dhis_report_path):
    make_note('Starting the cleaning process', START_TIME)

    # Renaming dict

    renaming_dict = dict(zip(VAR_CORR.name, VAR_CORR.identifier))

    # New reporting data

    new_dhis_report_df = get_reporting_data(new_dhis_report_path, 'new')
    new_dhis_report_df['dataElement'].replace(renaming_dict, inplace=True)
    make_note('new reporting data formatted for use', START_TIME)

    # New instance

    new_dhis_df = add_indicators(new_dhis_path, 'new')
    new_dhis_df['dataElement'].replace(renaming_dict, inplace=True)

    # Old reporting data

    old_dhis_report_df = get_reporting_data(old_dhis_report_path, 'old')
    old_dhis_report_df['dataElement'].replace(renaming_dict, inplace=True)
    make_note('old reporting data formatted for use', START_TIME)

    # Old instance
    old_dhis_df = add_indicators(old_dhis_path, 'old')
    old_dhis_df['dataElement'].replace(renaming_dict, inplace=True)

    # concatenate old and new instance

    combined_df = pd.concat([old_dhis_df,
                             new_dhis_df,
                             old_dhis_report_df,
                             new_dhis_report_df])

    combined_df.reset_index(drop=True, inplace=True)
    make_note('datasets concatenated', START_TIME)

    # Dealing with duplicates datesdue to conversion of weekly data to monthly

    combined_df = combined_df.groupby(
        ["dataElement", 'orgUnit', "year", "month"], as_index=False).agg({'value': 'sum'})
    make_note('duplicate dates summed', START_TIME)

    combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')

    # Keeping only the ids from HMIS reporting data

    all_report_facilities = (pd.read_csv(INDICATORS['name_district_map'])['facilitycode']
                             .unique()
                             .tolist())

    # TODO Find a way to output excluded ids
    # all_data_facilities = (combined_df['orgUnit']
    # .unique()
    # .tolist())

    combined_df = combined_df[combined_df['orgUnit']
                              .isin(all_report_facilities)]

    # Export to csv

    combined_df.to_csv(INDICATORS["clean_tall_data"])

    make_note('full data import and cleaning done', START_TIME)

    return combined_df
