###########################
#     Initialize file     #
###########################


# For sample :

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

ENGINE = engine.get_engine()
ENGINE_rename = engine.get_engine()

module_json = 'config/indicators.json'
with open(module_json) as f:
    for section in json.load(f):
        for p in section['data']:
            ENGINE[p['identifier']] = p['value']

startTime = datetime.datetime.now()


def make_note(statement):
    print(statement, str(datetime.datetime.now() - startTime))


###################################
#     Define functions used       #
###################################


# Get user-input data

VAR_CORR_PATH = ENGINE['var_correspondance_data']
VAR_CORR = pd.read_csv(VAR_CORR_PATH)

# Extracting reporting data, districts and facility names correspondances


def get_reporting_data(path, instance):

    month_dict = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
                  '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
                  '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
    district_names_dict = {'SEMBABULE': 'SSEMBABULE',
                           'MADI-OKOLLO': 'MADI OKOLLO', 'LUWEERO': 'LUWERO'}

    cols = ['Unnamed: 0', 'orgunitlevel1', 'orgunitlevel2', 'orgunitlevel3', 'orgunitlevel4', 'orgunitlevel5', 'organisationunitname',
            'organisationunitcode', 'organisationunitdescription', 'periodid', 'periodname', 'periodcode', 'perioddescription']

    # Get the right data
    df = pd.read_csv(path, dtype='object')
    metrics = list(VAR_CORR[(VAR_CORR['domain'] == 'REPORT') & (
        VAR_CORR['instance'] == instance)]['name'])
    for x in metrics:
        df[x] = pd.to_numeric(df[x], errors='coerce')

    make_note(str(instance)+' reporting data loaded')

    # Cleaning the district names
    df['districts'] = df['orgunitlevel3'].apply(lambda x: x[:-9].upper())
    df['districts'].replace(district_names_dict, inplace=True)

    # Formatting dates
    df['year'] = df['periodcode'].astype('str').apply(lambda x: x[:4])
    df['month'] = df['periodcode'].astype('str').apply(
        lambda x: x[-2:]).replace(month_dict)
    df.rename(columns={'organisationunitid': 'orgUnit'}, inplace=True)
    df.set_index(['districts', 'orgUnit', 'year', 'month'],
                 drop=True, inplace=True)

    # Dropping unused columns and renaming
    df.drop(cols, axis=1, inplace=True)
    df1 = df.copy().stack(dropna=False).reset_index()
    df1.rename(columns={0: 'value', 'level_4': 'dataElement'}, inplace=True)

    return df1

# Selecting target indicators


USECOLS = list(range(0, 10))
DTYPES = {'Unnamed: 0': int, 'dataElement': str, 'period': str, 'orgUnit': str, 'categoryOptionCombo': str,
          'attributeOptionCombo': str, 'value': object, 'storedBy': str, 'created': str, 'lastUpdated': str}


def get_data(path, instance):

    data = pd.read_csv(path, usecols=USECOLS, dtype=DTYPES)
    data['value'] = pd.to_numeric(data['value'], errors='coerce')

    make_note(str(instance)+" instance data loaded")

    new_indic_list = list(VAR_CORR[VAR_CORR['instance'] == instance]['name'])
    new_df = data[data["dataElement"].isin(new_indic_list)]
    new_df = pd.DataFrame(new_df.groupby(['dataElement', 'orgUnit', 'period'])[
                          'value'].sum()).reset_index()
    new_df = new_df[['period', 'orgUnit', 'value', 'dataElement']]

    make_note(str(instance)+" instance data indicator subset selected")

    return new_df

# Adding composite indicators


def get_variable_addition_dict(df, instance):
    '''build a dict with target vars as keys and original vars to add up as values'''

    target_dict = {}

    df1 = df[df['instance'] == instance]
    df2 = df1[df1.duplicated('identifier', keep=False)]
    df3 = df2[['identifier', 'name']]

    for x in list(df3['identifier'].unique()):
        target_dict[x] = df3[df3['identifier'] == x]['name'].tolist()

    return target_dict


def compute_indicators(df, indic_name, indicator_list):
    '''Compute new vars by summing original vars, and drop original vars '''

    df_new = df[df['dataElement'].isin(indicator_list)]
    df_new = pd.DataFrame(df_new.groupby(['period', 'orgUnit'])[
                          'value'].sum()).reset_index()
    df_new['dataElement'] = indic_name

    df = pd.concat([df, df_new])
    df.reset_index(drop=True, inplace=True)
    df = df[~df['dataElement'].isin(indicator_list)]

    return df

# Transforming dates


def date_convert(date):
    if date.find("W") == -1:
        x = pd.to_datetime(date, format='%Y%m')
    else:
        y = date.split("W")[0]
        w = date.split("W")[1]
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

#########################
#     Run functions     #
#########################


def clean(new_dhis_path, old_dhis_path, new_dhis_report_path, old_dhis_report_path):

    new_var_add_dict = get_variable_addition_dict(VAR_CORR, 'new')
    old_var_add_dict = get_variable_addition_dict(VAR_CORR, 'old')

    # Reporting data

    new_dhis_report_df = get_reporting_data(new_dhis_report_path, "new")
    make_note('new reporting data formatted for use')

    old_dhis_report_df = get_reporting_data(old_dhis_report_path, "old")
    make_note('old reporting data formatted for use')

    # New instance

    new_dhis_df = get_data(new_dhis_path, "new")

    for x in list(new_var_add_dict.keys()):
        new_dhis_df = compute_indicators(new_dhis_df, x, new_var_add_dict[x])
    make_note('new data additional indicators created')

    new_dhis_df = process_date(new_dhis_df)
    make_note('new data transformed and cleaned')

    # Old instance

    old_dhis_df = get_data(old_dhis_path, "old")
    for x in list(old_var_add_dict.keys()):
        old_dhis_df = compute_indicators(old_dhis_df, x, old_var_add_dict[x])
    make_note('old data additional indicators created')

    old_dhis_df = process_date(old_dhis_df)
    make_note('old data transformed and cleaned')

    # Renaming variables

    renaming_dict = dict(zip(VAR_CORR.name, VAR_CORR.identifier))

    old_dhis_df['dataElement'].replace(renaming_dict, inplace=True)
    new_dhis_df['dataElement'].replace(renaming_dict, inplace=True)
    old_dhis_report_df['dataElement'].replace(renaming_dict, inplace=True)
    new_dhis_report_df['dataElement'].replace(renaming_dict, inplace=True)
    make_note('variable names updated')

    # concatenate old and new instance

    combined_df = pd.concat([old_dhis_df, new_dhis_df,
                             old_dhis_report_df,
                             new_dhis_report_df])
    combined_df.reset_index(drop=True, inplace=True)
    make_note('datasets concatenated')

    # Dealing with duplicates dates

    combined_df = combined_df.groupby(
        ["dataElement", 'orgUnit', "year", "month"], as_index=False).agg({'value': 'sum'})
    make_note('duplicate dates summed')

    # Add district columns

    districts = pd.merge(combined_df['orgUnit'], old_dhis_report_df[[
        'orgUnit', 'districts']], how='left', left_on='orgUnit', right_on='orgUnit')
    combined_df['districts'] = districts['districts']
    make_note('district column added')

    # Selecting only the facilities that are in both old and new
    old_ids = set(old_dhis_report_df['orgUnit'].unique())
    new_ids = set(new_dhis_report_df['orgUnit'].unique())
    list_ids = list(new_ids.intersection(old_ids))

    # TODO remove
    # Alternative used when running tests
    #df = pd.read_csv('data/input/dhis2/valid_ids.csv')
    #list_ids = list(df['ids'].unique())

    series_ids = pd.Series(list_ids, name='series_ids')
    combined_df = combined_df[combined_df['orgUnit'].isin(series_ids)]

    make_note('correct ids selected')

    combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
    # TODO: Change that to exporting the data to the NITA-U database
    combined_df.to_csv(ENGINE["clean_tall_data"])
    make_note('full data import and cleaning done')

    return combined_df
