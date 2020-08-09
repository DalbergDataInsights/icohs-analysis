###########################
#     Initialize file     #
###########################

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

####################
#     Get data     #
####################

# Data paths

new_dhis_path = ENGINE['new_instance_data']
old_dhis_path = ENGINE['old_instance_data']

district_facility_path = ENGINE['facility_dist_data']
all_facilities_path = ENGINE['facility_data']

var_correspondance_path = ENGINE['var_correspondance_data']

# Get DHIS2 data

USECOLS = list(range(0, 10))
DTYPES = {'Unnamed: 0': int, 'dataElement': str, 'period': str, 'orgUnit': str, 'categoryOptionCombo': str,
          'attributeOptionCombo': str, 'value': object, 'storedBy': str, 'created': str, 'lastUpdated': str}

new_dhis_df = pd.read_csv(new_dhis_path, usecols=USECOLS, dtype=DTYPES)
old_dhis_df = pd.read_csv(old_dhis_path, usecols=USECOLS, dtype=DTYPES)

# Excluding all non numeric data from the dataset

new_dhis_df['value'] = pd.to_numeric(new_dhis_df['value'], errors='coerce')
old_dhis_df['value'] = pd.to_numeric(old_dhis_df['value'], errors='coerce')

# Get static data

dist_facility = pd.read_csv(district_facility_path)
district_facility_map = dict(
    zip(dist_facility.id, dist_facility.district))

facilities_df = pd.read_csv(all_facilities_path)

# Get user-input data

var_corr = pd.read_csv(var_correspondance_path)


#################################################
#     Define data transformation functions      #
#################################################

# Selecting target indicators

def get_new_data(data):
    new_indic_list = list(var_corr[var_corr['instance'] == 'new']['name'])
    new_df = data[data["dataElement"].isin(new_indic_list)]
    new_df = pd.DataFrame(new_df.groupby(['dataElement', 'orgUnit', 'period'])[
                          'value'].sum()).reset_index()
    new_df = new_df[['period', 'orgUnit', 'value', 'dataElement']]
    return new_df


def get_old_data(data):
    old_indic_list = list(var_corr[var_corr['instance'] == 'old']['name'])
    old_df = data[data["dataElement"].isin(old_indic_list)]
    old_df = pd.DataFrame(old_df.groupby(['dataElement', 'orgUnit', 'period'])[
                          'value'].sum()).reset_index()
    old_df = old_df[['period', 'orgUnit', 'value', 'dataElement']]
    return old_df

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

# Pivoting the data, replacing outliers, unpivoting the data


def pivot_stack(df):
    pivot_outliers = df.copy().pivot_table(index=[
        'districts', 'orgUnit', 'dataElement'], columns=['year', 'month'])
    pivot_outliers.rename(
        columns={'value': 'with_outiers'}, level=0, inplace=True)
    pivot_outliers.columns.rename('type', level=0, inplace=True)
    pivot_outliers.dropna(how='all', axis=0, inplace=True)
    return pivot_outliers


def replace_outliers(pivot_outliers, cutoff):
    ''' Replace ouliers using a std deviation method'''

    pivot_no_outliers = pd.DataFrame(
        columns=pivot_outliers.columns, index=pivot_outliers.index)
    pivot_no_outliers.rename(
        columns={'with_outiers': 'without_outliers'}, level=0, inplace=True)

    for x in pivot_outliers.index:
        values = pivot_outliers.loc[x, :].values
        if np.nanstd(values) != 0 and np.isnan(values).sum() != len(values):
            zscore = abs(stats.zscore(values, nan_policy='omit'))
            new_values = np.where(
                zscore > cutoff, np.nanmedian(values), values)
        else:
            new_values = values

        pivot_no_outliers.iloc[pivot_outliers.index.get_loc(
            x), :] = new_values.astype('float')

    return pivot_no_outliers


def replace_outliers_iqr(pivot_outliers, k):
    ''' Replace outliers using an iqr deviation method'''

    pivot_no_outliers = pd.DataFrame(
        columns=pivot_outliers.columns, index=pivot_outliers.index)
    pivot_no_outliers.rename(
        columns={'with_outiers': 'without_outliers'}, level=0, inplace=True)

    for x in pivot_outliers.index:
        values = pivot_outliers.loc[x, :].values
        if np.nanstd(values) != 0 and np.isnan(values).sum() != len(values):
            Q1 = np.nanquantile(values, 0.25)
            Q3 = np.nanquantile(values, 0.75)
            IQR = Q3 - Q1
            LB = Q1 - k*IQR
            UB = Q3 + k*IQR
            new_values = np.where((values < LB) | (
                values > UB), np.nanmedian(values), values)
        else:
            new_values = values

        pivot_no_outliers.iloc[pivot_outliers.index.get_loc(
            x), :] = new_values.astype('float')

    return pivot_no_outliers


def pivot_stack_post_process(pivot):
    ''' stack outlier corrected data '''

    stack = pivot.stack(level=[0, 1, 2], dropna=False).reset_index()
    stack.rename(columns={0: 'value'}, inplace=True)
    stack.drop('type', axis=1, inplace=True)
    stack['value'] = stack['value'].astype(dtype='float64')

    return stack

##########################################
#     Packaging the data for export      #
##########################################


def export_to_csv(stack_t_noreport, stack_t_noout, stack_t_noout_iqr):

    fac_stack_final = pd.merge(stack_t_noreport, stack_t_noout, how='left',
                               left_on=['districts', 'orgUnit',
                                        'year', 'dataElement', 'month'],
                               right_on=['districts', 'orgUnit', 'year', 'dataElement', 'month']).rename(columns={'value_x': 'value_out', 'value_y': 'value_noout'})

    fac_stack_final = pd.merge(fac_stack_final, stack_t_noout_iqr, how='left',
                               left_on=['districts', 'orgUnit',
                                        'year', 'dataElement', 'month'],
                               right_on=['districts', 'orgUnit', 'year', 'dataElement', 'month']).rename(columns={'value': 'value_noout_iqr'})

    # Make a note of whcih facilities reported on each indicator and which didn't
    fac_stack_final['reported'] = (
        fac_stack_final['value_out'] > 0).astype('int')

    # Add in the reporting rate data, that did not go through theoutlier precocedure
    data_df_report.rename(columns={'value': 'reported'}, inplace=True)
    data_df_report.set_index(
        ['districts', 'orgUnit', 'year', 'dataElement', 'month'], inplace=True, drop=True)

    data_df_report.reset_index(inplace=True)

    # Puts it all together
    fac_stack_final = pd.concat(
        [data_df_report, fac_stack_final], ignore_index=True)
    fac_stack_final = pd.melt(fac_stack_final, id_vars=['districts', 'orgUnit', 'year', 'dataElement', 'month'], value_vars=[
                              'reported', 'value_out', 'value_noout', 'value_noout_iqr'])
    fac_stack_final.rename(columns={'variable': 'dataset'}, inplace=True)

    # Create a pivot
    fac_pivot_final = fac_stack_final.pivot_table(
        index=['districts', 'orgUnit', 'year', 'month', 'dataset'], columns=['dataElement'], aggfunc='mean')
    fac_pivot_final = fac_pivot_final.stack(level=[0])
    fac_pivot_final.to_csv('data/output/corrected_data.csv')

    return fac_pivot_final


def main(new_dhis_df, old_dhis_df):

    new_var_add_dict = get_variable_addition_dict(var_corr, 'new')
    old_var_add_dict = get_variable_addition_dict(var_corr, 'old')

    # New instance

    new_dhis_df = get_new_data(new_dhis_df)

    for x in list(new_var_add_dict.keys()):
        new_dhis_df = compute_indicators(new_dhis_df, x, new_var_add_dict[x])

    new_dhis_df = process_date(new_dhis_df)

    print('new data retrieved')

    # Old instance

    old_dhis_df = get_old_data(old_dhis_df)

    for x in list(old_var_add_dict.keys()):
        old_dhis_df = compute_indicators(old_dhis_df, x, old_var_add_dict[x])

    old_dhis_df = process_date(old_dhis_df)

    print('old data retrieved')

    # Renaming variables

    renaming_dict = dict(zip(var_corr.name, var_corr.identifier))

    old_dhis_df['dataElement'].replace(renaming_dict, inplace=True)
    new_dhis_df['dataElement'].replace(renaming_dict, inplace=True)

    # concatenate old and new instance

    combined_df = pd.concat([old_dhis_df, new_dhis_df])
    combined_df.reset_index(drop=True, inplace=True)

    # Selecting only the facilities that are in both old and new
    #old_ids = set(old_dhis_df['orgUnit'].unique())
    #new_ids = set(new_dhis_df['orgUnit'].unique())
    #list_ids = list(new_ids.intersection(old_ids))

    # temporary
    df = pd.read_csv('data/input/dhis2/old/valid_ids.csv')
    list_ids = list(df['id'].unique())

    combined_df = combined_df[combined_df['orgUnit'].isin(list_ids)]

    # I should not need that once the  reporting data is in, I should however make sure the reported data is pivotedwith the rest

    combined_df = combined_df.groupby(
        ["year", "month", "dataElement", "orgUnit"], as_index=False).sum()

    combined_df['districts'] = combined_df['orgUnit'].map(
        district_facility_map)

    return combined_df


if __name__ == "__main__":
    '''Executes the functiosn defined above'''

    data_df = main(new_dhis_df, old_dhis_df)
    print('data import and cleaning done')

    # separe reports and non reports indicators
    report_indics = ['actual_105_1_reporting', 'expected_105_1_reporting']
    data_df_noreport = data_df[~data_df['dataElement'].isin(
        report_indics)].copy()
    data_df_report = data_df[data_df['dataElement'].isin(report_indics)].copy()

    # outlier computattion on non report data

    pivot_outliers = pivot_stack(data_df_noreport)
    print('data pivot for outlier exclusion done')

    pivot_no_outliers = replace_outliers(pivot_outliers, cutoff=3)
    print('first outlier exclusion process done')

    pivot_no_outliers_iqr = replace_outliers_iqr(pivot_outliers, k=3)
    print('second outlier exclusion process done')

    stack_t_noout = pivot_stack_post_process(pivot_no_outliers)
    stack_t_noout_iqr = pivot_stack_post_process(pivot_no_outliers_iqr)
    print('stacking of the outlier-excluded data done')

    pivot_final = export_to_csv(
        data_df_noreport, stack_t_noout, stack_t_noout_iqr)
    print('data concatenatation done')

    print(data_df.head())

print(datetime.datetime.now() - startTime)
