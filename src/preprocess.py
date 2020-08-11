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


def make_note(statement):
    print(statement, str(datetime.datetime.now() - startTime))

####################
#     Get data     #
####################

# Data paths


new_dhis_path = ENGINE['new_instance_data']
old_dhis_path = ENGINE['old_instance_data']
new_dhis_report_path = ENGINE['new_instance_data_report']
old_dhis_report_path = ENGINE['old_instance_data_report']
var_correspondance_path = ENGINE['var_correspondance_data']

# Get DHIS2 data

USECOLS = list(range(0, 10))
DTYPES = {'Unnamed: 0': int, 'dataElement': str, 'period': str, 'orgUnit': str, 'categoryOptionCombo': str,
          'attributeOptionCombo': str, 'value': object, 'storedBy': str, 'created': str, 'lastUpdated': str}

new_dhis_df = pd.read_csv(new_dhis_path, usecols=USECOLS, dtype=DTYPES)
old_dhis_df = pd.read_csv(old_dhis_path, usecols=USECOLS, dtype=DTYPES)

new_dhis_report_df = pd.read_csv(new_dhis_report_path, dtype='object')
old_dhis_report_df = pd.read_csv(old_dhis_report_path, dtype='object')

# Get user-input data

var_corr = pd.read_csv(var_correspondance_path)
# Excluding all non numeric data from the dataset

new_dhis_df['value'] = pd.to_numeric(new_dhis_df['value'], errors='coerce')
old_dhis_df['value'] = pd.to_numeric(old_dhis_df['value'], errors='coerce')

make_note('old and new data loaded')

new_report_metrics = list(var_corr[(var_corr['domain'] == 'REPORT') & (
    var_corr['instance'] == 'new')]['name'])
old_report_metrics = list(var_corr[(var_corr['domain'] == 'REPORT') & (
    var_corr['instance'] == 'old')]['name'])

for x in new_report_metrics:
    new_dhis_report_df[x] = pd.to_numeric(
        new_dhis_report_df[x], errors='coerce')
for x in old_report_metrics:
    old_dhis_report_df[x] = pd.to_numeric(
        old_dhis_report_df[x], errors='coerce')

make_note('old and new reporting data loaded')

#################################################
#     Define data transformation functions      #
#################################################

# Extracting reporting data, districts and facility names correspondances


def get_reporting_data(df):
    # TODO break down in smaller functions

    month_dict = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
                  '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
                  '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
    district_names_dict = {'SEMBABULE': 'SSEMBABULE',
                           'MADI-OKOLLO': 'MADI OKOLLO', 'LUWEERO': 'LUWERO'}

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

    # Dropping unused columns
    cols = ['Unnamed: 0', 'orgunitlevel1', 'orgunitlevel2', 'orgunitlevel3', 'orgunitlevel4', 'orgunitlevel5', 'organisationunitname',
            'organisationunitcode', 'organisationunitdescription', 'periodid', 'periodname', 'periodcode', 'perioddescription']
    df.drop(cols, axis=1, inplace=True)

    # renaming

    df1 = df.copy().stack(dropna=False).reset_index()
    df1.rename(columns={0: 'value', 'level_4': 'dataElement'}, inplace=True)

    return df1

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


def export_full_table_to_csv(stack_t_noreport, stack_t_noout, stack_t_noout_iqr):

    # TODO: break down in smaller functions

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


def parse_date(date):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    year, month = date.split('-')
    month = months.index(month)+1
    return datetime.date(year=int(year), month=month, day=1)


def get_report_types(e, a, i):
    if i == 1:
        y = 'reported_on_this_indic'
    elif e == 1 and a == 1:
        y = 'did_not_report_on_this_indic'
    elif e == 1 and a != 1:
        y = 'did_not_report_on_this_indic_nor_on_105_1_form'
    elif e != 1:
        y = 'not_expected_to_report'
    return y


def add_report_columns(data):
    # TODO check the hardcodednumber of columns is sustainable
    for x in list(data.columns[8:]):
        data[str(x)+'_rpt'] = np.vectorize(get_report_types)(
            data['expected_105_1_reporting'], data['actual_105_1_reporting'], data[x])
        data.drop(x, axis=1, inplace=True)
        data.rename(columns={str(x)+'_rpt': str(x)}, inplace=True)
    return data


def export_broken_down_table_to_csv(data):

    data.reset_index(level=[0, 1, 2, 3, 4], col_level=1, inplace=True)

    # Combine the date into one column and format

    data['date'] = data['year'].astype(str) + '-' + data['month']
    data['date'] = data.date.apply(parse_date)
    data['date'] = pd.to_datetime(data.date)

    # Breakdown in several dfs

    reporting = data[data['dataset'] == 'reported'].copy()
    with_outliers = data[data['dataset'] == 'value_out'].copy()
    no_outliers_std = data[data['dataset'] == 'value_noout'].copy()
    no_outliers_iqr = data[data['dataset'] == 'value_noout_iqr'].copy()

    # Add the reporting status columns

    reporting_add = add_report_columns(reporting)

    # Export to csv

    reporting_add.to_csv(ENGINE['report_data'])
    with_outliers.to_csv(ENGINE['outlier_data')
    no_outliers_std.to_csv(ENGINE['std_no_outlier_data'])
    no_outliers_iqr.to_csv(ENGINE['iqr_no_outlier_data'])

    return (reporting_add, with_outliers, no_outliers_std, no_outliers_iqr)


#############################
#     Run all functions     #
#############################


def main(new_dhis_df, old_dhis_df, new_dhis_report_df, old_dhis_report_df):

    new_var_add_dict = get_variable_addition_dict(var_corr, 'new')
    old_var_add_dict = get_variable_addition_dict(var_corr, 'old')

    # Reporting data

    new_dhis_report_df = get_reporting_data(new_dhis_report_df)

    make_note('new reporting data formatted for use')

    old_dhis_report_df = get_reporting_data(old_dhis_report_df)

    make_note('old reporting data formatted for use')

    # New instance

    new_dhis_df = get_new_data(new_dhis_df)

    make_note('new data indicators selected')

    for x in list(new_var_add_dict.keys()):
        new_dhis_df = compute_indicators(new_dhis_df, x, new_var_add_dict[x])

    make_note('new data additional indicators created')

    new_dhis_df = process_date(new_dhis_df)

    # Old instance

    old_dhis_df = get_old_data(old_dhis_df)

    make_note('old data indicators selected')

    for x in list(old_var_add_dict.keys()):
        old_dhis_df = compute_indicators(old_dhis_df, x, old_var_add_dict[x])

    old_dhis_df = process_date(old_dhis_df)

    make_note('old data additional indicators created')

    # Renaming variables

    renaming_dict = dict(zip(var_corr.name, var_corr.identifier))

    old_dhis_df['dataElement'].replace(renaming_dict, inplace=True)
    new_dhis_df['dataElement'].replace(renaming_dict, inplace=True)
    old_dhis_report_df['dataElement'].replace(renaming_dict, inplace=True)
    new_dhis_report_df['dataElement'].replace(renaming_dict, inplace=True)

    make_note('variable names updated')

    # concatenate old and new instance

    combined_df = pd.concat([old_dhis_df, new_dhis_df[['orgUnit', 'year', 'month', 'dataElement', 'value']],
                             old_dhis_report_df, new_dhis_report_df[['orgUnit', 'year', 'month', 'dataElement', 'value']]])
    combined_df.reset_index(drop=True, inplace=True)

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

    # Dealing with duplicates dates
    # TODO optimize that part

    combined_df = combined_df.groupby(
        ["dataElement", 'districts', 'orgUnit', "year", "month"], as_index=False).agg({'value': 'sum'})

    combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')

    return combined_df


if __name__ == "__main__":
    '''Executes the functiosn defined above'''

    data_df = main(new_dhis_df, old_dhis_df,
                   new_dhis_report_df, old_dhis_report_df)
    make_note('full data import and cleaning done')

    # separe reports and non reports indicators
    report_indics = ['actual_105_1_reporting', 'expected_105_1_reporting']
    data_df_noreport = data_df[~data_df['dataElement'].isin(
        report_indics)].copy()
    data_df_report = data_df[data_df['dataElement'].isin(report_indics)].copy()

    # outlier computattion on non report data

    pivot_outliers = pivot_stack(data_df_noreport)
    make_note('data pivot for outlier exclusion done')

    pivot_no_outliers = replace_outliers(pivot_outliers, cutoff=3)
    make_note('first outlier exclusion process done')

    pivot_no_outliers_iqr = replace_outliers_iqr(pivot_outliers, k=3)
    make_note('second outlier exclusion process done')

    stack_t_noout = pivot_stack_post_process(pivot_no_outliers)
    stack_t_noout_iqr = pivot_stack_post_process(pivot_no_outliers_iqr)
    make_note('stacking of the outlier-excluded data done')

    pivot_final = export_full_table_to_csv(
        data_df_noreport, stack_t_noout, stack_t_noout_iqr)
    make_note('data concatenatation done')

    (facility_data_reporting, facility_data_with_outliers, facility_data_no_outliers_std,
     facility_data_no_outliers_iqr) = export_broken_down_table_to_csv(pivot_final)
    make_note('breakdown in four tables done')
    print(facility_data_with_outliers.head())

print(datetime.datetime.now() - startTime)
