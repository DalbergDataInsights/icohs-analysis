###########################
#     Initialize file     #
###########################

# For sample :

# csv export
# sample cols

import datetime

import numpy as np
import pandas as pd
from scipy.stats import stats

from src.helpers import INDICATORS, make_note, get_unique_indics, format_date


#################################################
#                    CONSTANTS                  #
#################################################

START_TIME = datetime.datetime.now()
VAR_CORR = pd.read_csv(INDICATORS['var_correspondence_data'])


#################################################
#     Define data transformation functions      #
#################################################


# Pivoting the data, replacing outliers, unpivoting the data


def pivot_stack(df):
    pivot_outliers = df.pivot_table(index=['orgUnit', 'dataElement'],
                                    columns=['year', 'month']).copy()

    pivot_outliers.rename(columns={'value': 'with_outliers'},
                          level=0,
                          inplace=True)

    pivot_outliers.columns.rename('type', level=0, inplace=True)
    pivot_outliers.dropna(how='all', axis=0, inplace=True)

    return pivot_outliers


def replace_outliers(pivot_outliers, cutoff):
    '''Replace outliers using a std deviation method'''

    pivot_no_outliers = pd.DataFrame(columns=pivot_outliers.columns,
                                     index=pivot_outliers.index)
    pivot_no_outliers.rename(columns={'with_outliers': 'without_outliers'},
                             level=0,
                             inplace=True)

    for x in pivot_outliers.index:
        values = pivot_outliers.loc[x, :].values
        if np.nanstd(values) != 0 and np.isnan(values).sum() != len(values):
            zscore = abs(stats.zscore(values, nan_policy='omit'))
            new_values = np.where(
                zscore > cutoff, np.nanmedian(values), values)
        else:
            new_values = values

        pivot_no_outliers.iloc[pivot_outliers.index
                                             .get_loc(x), :] = new_values.astype('float')

    return pivot_no_outliers


def replace_outliers_iqr(pivot_outliers, k):
    '''Replace outliers using an iqr deviation method'''

    pivot_no_outliers = pd.DataFrame(columns=pivot_outliers.columns,
                                     index=pivot_outliers.index)
    pivot_no_outliers.rename(columns={'with_outliers': 'without_outliers'},
                             level=0,
                             inplace=True)

    for x in pivot_outliers.index:
        values = pivot_outliers.loc[x, :].values
        if np.nanstd(values) != 0 and np.isnan(values).sum() != len(values):
            Q1 = np.nanquantile(values, 0.25)
            Q3 = np.nanquantile(values, 0.75)
            IQR = Q3 - Q1
            LB = Q1 - k * IQR
            UB = Q3 + k * IQR
            new_values = np.where((values < LB) | (values > UB),
                                  np.nanmedian(values),
                                  values)
        else:
            new_values = values

        pivot_no_outliers.iloc[pivot_outliers.index.get_loc(
            x), :] = new_values.astype('float')

    return pivot_no_outliers


def pivot_stack_post_process(pivot):
    '''Stack outlier corrected data'''

    stack = pivot.stack(level=[0, 1, 2], dropna=True).reset_index()
    stack.rename(columns={0: 'value'}, inplace=True)
    stack.drop('type', axis=1, inplace=True)
    stack['value'] = stack['value'].astype(dtype='float64')

    return stack


def add_info_and_format(data, location, columns=['id', 'facility_id', 'facility_name', 'date', 'dataElement', 'value']):

    # create date and delete year and month

    data['date'] = data['year'].astype(str)\
        + '-' + data['month']
    data.drop(columns=['year', 'month'], inplace=True)
    data['date'] = data.date.apply(format_date)

    # add facility_name and district

    data = pd.merge(data, location,
                    left_on='orgUnit', right_on='facilitycode', how='left')

    # rename cols

    data = data.reset_index().rename(
        columns={'orgUnit': 'facility_id', 'facilityname': 'facility_name', 'districtname': 'district_name'})

    return data[columns]


def compute_outliers_stack(pivot, policy, location):
    if policy == 'std':
        pivot_processed = replace_outliers(pivot, cutoff=3)
    elif policy == 'iqr':
        pivot_processed = replace_outliers_iqr(pivot, k=3)

    stack = pivot_stack_post_process(pivot_processed)

    stack = add_info_and_format(stack, location)

    make_note(f'{policy} outlier exclusion process done', START_TIME)

    return stack


def full_pivot_for_export(data, index=['id', 'facility_id', 'facility_name', 'date']):

    data_pivot = data.pivot_table(index=index,
                                  columns=['dataElement'],
                                  aggfunc='sum')
    data_pivot.columns = data_pivot.columns.droplevel(0)

    columns = sorted(data_pivot.columns)

    return data_pivot[columns]

###################################
#     Create the report data      #
###################################


def add_report_to_pivot(data_pivot, report, location):

    report = add_info_and_format(report, location)

    report_pivot = full_pivot_for_export(report)

    full_pivot = pd.merge(report_pivot, data_pivot,  how='left',
                          left_index=True,
                          right_index=True)

    return full_pivot


def add_report_columns(data):

    # TODO review the rationale for this, as 17,7, 8 and 1 should not exist

    value_dict = {18: 3,
                  11: 2,
                  10: 1,
                  0: 0,
                  17: -1,
                  7: -1,
                  8: -1,
                  1: -1}

    data['e'] = data['expected_105_1_reporting'] * 9
    data['a+e'] = (data[['expected_105_1_reporting',
                         'actual_105_1_reporting']].sum(axis=1))

    cols = get_unique_indics(VAR_CORR)

    for x in cols:
        data['i'] = data[x] * 7
        data['sum'] = data[['i', 'e', 'a+e']].sum(axis=1)
        data.drop([x, 'i'], axis=1, inplace=True)
        data[x] = data['sum'].replace(value_dict)
        data.drop('sum', axis=1, inplace=True)

    data.drop(columns=['e', 'a+e'], inplace=True)

    return data


def create_reporting_pivot(pivot, report, location):

    full_pivot = add_report_to_pivot(pivot, report, location)

    for c in full_pivot.columns:
        full_pivot[c] = (full_pivot[c] > 0).astype('int')

    report = add_report_columns(full_pivot)
    report.drop(columns=['expected_105_1_reporting',
                         'actual_105_1_reporting'], inplace=True)

    make_note('Reporting done', START_TIME)

    return report


def stack_reporting(pivot):
    '''Stack outlier corrected data'''

    stack = pivot.stack().reset_index()
    stack.rename(columns={0: 'value', 'level_4': 'dataElement'}, inplace=True)
    stack['value'] = stack['value'].astype(dtype='float64')

    return stack

###############################
#     Create final stack      #
###############################


def add_to_final_stack(final_stack, input_stack, policy):
    input_stack['dataElement'] = input_stack['dataElement'] + f'__{policy}'
    final_stack = pd.concat([final_stack, input_stack])
    return final_stack

#############################
#     Run all functions     #
#############################


def process(main, report, location):
    make_note('Starting the data processing', START_TIME)

    pivot_outliers = pivot_stack(main)

    make_note('data pivot for outlier exclusion done', START_TIME)

    # outlier computations

    outliers_stack = add_info_and_format(main, location)

    outliers = full_pivot_for_export(outliers_stack)
    stack = pd.DataFrame(columns=outliers_stack.columns)
    stack = add_to_final_stack(stack, outliers_stack, 'outliers')
    del outliers_stack

    std_stack = compute_outliers_stack(pivot_outliers, 'std', location)
    std = full_pivot_for_export(std_stack)
    stack = add_to_final_stack(stack, std_stack, 'std')
    del std_stack

    iqr_stack = compute_outliers_stack(pivot_outliers, 'iqr', location)
    iqr = full_pivot_for_export(iqr_stack)
    stack = add_to_final_stack(stack, iqr_stack, 'iqr')
    del iqr_stack

    make_note('outlier exclusion done', START_TIME)

    # creating the reporting table

    report = create_reporting_pivot(outliers, report, location)
    report_stack = stack_reporting(report)
    stack = add_to_final_stack(stack, report_stack, 'report')
    del report_stack

    # Exportng

    # TODO Export to DB)

    report.to_csv(INDICATORS['report_data'])
    outliers.to_csv(INDICATORS['outlier_data'])
    std.to_csv(INDICATORS['std_no_outlier_data'])
    iqr.to_csv(INDICATORS['iqr_no_outlier_data'])
    stack.to_csv(INDICATORS["tall_data"], index=False)

    make_note('breakdown in four tables done', START_TIME)
