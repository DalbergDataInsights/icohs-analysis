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

from src.helpers import INDICATORS, make_note, format_date

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
        columns={'orgUnit': 'facility_id', 'facilityname': 'facility_name', 'districtname': 'id'})

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

    return data_pivot

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
                  17: 3,
                  7: 3,
                  8: 3,
                  1: 2}

    data['e'] = data['expected_105_1_reporting'] * 9
    data['a+e'] = (data[['expected_105_1_reporting',
                         'actual_105_1_reporting']].sum(axis=1))

    cols = VAR_CORR[VAR_CORR['domain'] !=
                    'REPORT']['identifier'].unique().tolist()

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

    # main.drop('value', axis=1, inplace=True)
    # main.rename(columns={'reported': 'report'}, inplace=True)
    #reporting = full_pivot(main, location, report_data=report)

    report = add_report_columns(full_pivot)
    report.drop(columns=['expected_105_1_reporting',
                         'actual_105_1_reporting'], inplace=True)

    make_note('Reporting done', START_TIME)

    return report


# def pivot_for_export(data):

#     data_pivot = data.pivot_table(index=['orgUnit', 'year', 'month'],
#                                   columns=['dataElement'],
#                                   aggfunc='sum')
#     data_pivot.columns = data_pivot.columns.droplevel(0)

#     return data_pivot


# def full_pivot(data, location, **report_data):

#     data_pivot = pivot_for_export(data)

#     if report_data != {} and isinstance(report_data['report_data'], pd.DataFrame):

#         data_report_pivot = pivot_for_export(report_data['report_data'])
#         data_pivot = pd.merge(data_pivot, data_report_pivot,
#                               left_index=True, right_index=True, how='left')

#     columns = sorted(data_pivot.columns)

#     data_pivot = data_pivot.reset_index()

#     # Create date and delete year and month

#     data_pivot['date'] = data_pivot['year'].astype(str)\
#         + '-' + data_pivot['month']
#     data_pivot.drop(columns=['year', 'month'], inplace=True)
#     data_pivot['date'] = data_pivot.date.apply(format_date)

#     # add facility_name and district

#     data_pivot = pd.merge(data_pivot, location,
#                           left_on='orgUnit', right_on='facilitycode', how='left')

#     data_pivot = data_pivot.reset_index().rename(
#         columns={'orgUnit': 'facility_id', 'facilityname': 'facility_name', 'districtname': 'id'})

#     columns = ['id', 'date', 'facility_id', 'facility_name'] + columns

#     return data_pivot[columns]


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

    # with add_info_and_format(main, location) as outliers_stack:
    outliers_stack = add_info_and_format(main, location)
    outliers = full_pivot_for_export(outliers_stack)
    stack = pd.DataFrame(columns=outliers_stack.columns)
    stack = add_to_final_stack(stack, outliers_stack, 'outliers')
    del outliers_stack

    # std_stack = compute_outliers_stack(pivot_outliers, 'std', location)
    # std = full_pivot_for_export(std_stack)
    # stack = add_to_final_stack(stack, std_stack, 'std')
    # del std_stack

    # iqr_stack = compute_outliers_stack(pivot_outliers, 'iqr', location)
    # iqr = full_pivot_for_export(iqr_stack)
    # stack = add_to_final_stack(stack, iqr_stack, 'iqr')
    # del iqr_stack

    make_note('outlier exclusion done', START_TIME)

    # creating the reporting table

    report = create_reporting_pivot(outliers, report, location)

    # TODO unpivot report and add to stack

    report.to_csv(INDICATORS['report_data'], index=False)
    outliers.to_csv(INDICATORS['outlier_data'], index=False)
    #std.to_csv(INDICATORS['std_no_outlier_data'], index=False)
    #iqr.to_csv(INDICATORS['iqr_no_outlier_data'], index=False)
    stack.to_csv(INDICATORS["tall_data"], index=False)

    make_note('breakdown in four tables done', START_TIME)
