###########################
#     Initialize file     #
###########################


# For sample :

# l.371 csv export
# l.459 valid ids
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

START_TIME = datetime.now()

# Extracting reporting data, districts and facility names correspondences


def get_reporting_data(path, instance):

    month_dict = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
                  '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
                  '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
    district_names_dict = {'SEMBABULE': 'SSEMBABULE',
                           'MADI-OKOLLO': 'MADI OKOLLO', 'LUWEERO': 'LUWERO'}

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

    make_note(str(instance)+' reporting data loaded')

    # Cleaning the district names
    df['districts'] = df['orgunitlevel3'].apply(lambda x: x[:-9].upper())
    df['districts'].replace(district_names_dict, inplace=True)

    # Formatting dates
    df['year'] = df['periodcode'].astype('str')\
                                 .apply(lambda x: x[:4])
    df['month'] = df['periodcode'].astype('str')\
                                  .apply(lambda x: x[-2:])\
                                  .replace(month_dict)
    df.rename(columns={'organisationunitid': 'orgUnit'}, inplace=True)
    df.set_index(['districts', 'orgUnit', 'year', 'month'],
                 drop=True,
                 inplace=True)

    # Dropping unused columns and renaming
    df.drop(cols, axis=1, inplace=True)
    df1 = df.copy()\
            .stack(dropna=False)\
            .reset_index()
    df1.rename(columns={0: 'value', 'level_4': 'dataElement'}, inplace=True)

    return df1

# Selecting target indicators


def get_data(path, instance):

    with pd.read_csv(path, usecols=USECOLS, dtype=DTYPES) as data:
        new_indic_list = VAR_CORR[VAR_CORR['instance'] == instance]['name'].tolist()
        new_df = data[data["dataElement"].isin(new_indic_list)] \
                                         .reset_index(drop=True)

    new_df['value'] = pd.to_numeric(new_df['value'], errors='coerce')
    new_df = new_df.groupby(['dataElement', 'orgUnit', 'period'], as_index=False).agg({'value': 'sum'})
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
    df_new = df_new.groupby(['period', 'orgUnit'], as_index=False).agg({'value': 'sum'})
    df_new['dataElement'] = indic_name

    df = pd.concat([df, df_new])
    df.reset_index(drop=True, inplace=True)
    df = df[~df['dataElement'].isin(indicator_list)]

    return df

# Transforming dates


def date_convert(date):
    if 'W' not in date:
        x = pd.to_datetime(date, format='%Y%m')
    else:
        y, w = *date.split("W")
        x = pd.to_datetime(f'{y} {w} 1', format='%G %V %u')
    return x


def process_date(df):
    df['datetime'] = np.vectorize(date_convert)(df['period'].astype(str))
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    map_day = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
               5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
               9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

    df['month'] = df['month'].map(map_day)
    df = df.drop(['datetime', 'period'], axis=1)
    df['year'] = df['year'].apply(str)
    return df


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

    # Dealing with duplicates dates

    combined_df = combined_df.groupby(
        ["dataElement", 'orgUnit', "year", "month"], as_index=False).agg({'value': 'sum'})
    make_note('duplicate dates summed', START_TIME)

    # Add district columns

    districts = pd.merge(combined_df['orgUnit'],
                         old_dhis_report_df[['orgUnit', 'districts']],
                         how='left',
                         left_on='orgUnit',
                         right_on='orgUnit')
    combined_df['districts'] = districts['districts']
    make_note('district column added', START_TIME)

    # Selecting only the facilities that are in both old and new
    old_ids = set(old_dhis_report_df['orgUnit'].tolist())  # FIXME
    new_ids = set(new_dhis_report_df['orgUnit'].tolist())
    list_ids = list(new_ids.intersection(old_ids))

    # TODO remove
    # Alternative used when running tests
    #df = pd.read_csv('data/input/dhis2/valid_ids.csv')
    #list_ids = list(df['ids'].unique())

    series_ids = pd.Series(list_ids, name='series_ids')
    combined_df = combined_df[combined_df['orgUnit'].isin(series_ids)]

    # END FIXME

    make_note('correct ids selected', START_TIME)

    combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
    # TODO: Change that to exporting the data to the NITA-U database
    combined_df.to_csv(INDICATORS["clean_tall_data"])
    make_note('full data import and cleaning done', START_TIME)

    return combined_df
