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

from src.helpers import INDICATORS, make_note

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

    stack = pivot.stack(level=[0, 1, 2], dropna=False).reset_index()
    stack.rename(columns={0: 'value'}, inplace=True)
    stack.drop('type', axis=1, inplace=True)
    stack['value'] = stack['value'].astype(dtype='float64')

    return stack

##########################################
#     Packaging the data for export      #
##########################################


def parse_date(date):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    year, month = date.split('-')
    month = months.index(month) + 1
    return datetime.date(year=int(year), month=month, day=1)


def add_report_columns(data):

    # TODO review the rationale for this, as 17,7, 8 and 1 should not exist

    value_dict = {18: 'reported_on_this_indic',
                  11: 'did_not_report_on_this_indic',
                  10: 'did_not_report_on_this_indic_nor_on_105_1_form',
                  0: 'not_expected_to_report',
                  17: 'reported_on_this_indic',
                  7: 'reported_on_this_indic',
                  8: 'reported_on_this_indic',
                  1: 'did_not_report_on_this_indic'}
    data['e'] = data['expected_105_1_reporting'] * 9
    data['a+e'] = (data[['expected_105_1_reporting',
                         'actual_105_1_reporting']].sum(axis=1))

    cols = VAR_CORR[VAR_CORR['domain'] !=
                    'REPORT']['identifier'].unique().tolist()
    # cols = set(cols).intersection(set(data.columns))  # Sample change #TODO remove

    for x in cols:
        data['i'] = data[x] * 7
        data['sum'] = data[['i', 'e', 'a+e']].sum(axis=1)
        data.drop([x, 'i'], axis=1, inplace=True)
        sum_col = data['sum'].replace(value_dict, inplace=True)
        data[x] = sum_col
        data.drop('sum', axis=1, inplace=True)

    data.drop(['e', 'a+e'], axis=1, inplace=True)

    return data


def pivot_for_export(data):

    data_pivot = data.pivot_table(index=['orgUnit', 'year', 'month'],
                                  columns=['dataElement'],
                                  aggfunc='sum')
    data_pivot.columns = data_pivot.columns.droplevel(0)

    return data_pivot


def full_pivot(data, report_data):

    data_pivot = pivot_for_export(data)
    data_report_pivot = pivot_for_export(report_data)

    data_pivot = pd.merge(data_pivot, data_report_pivot,
                          left_index=True, right_index=True, how='left')

    columns = sorted(data_pivot.columns)

    return data_pivot[columns]

#############################
#     Run all functions     #
#############################


def process(data):
    make_note('Starting the data processing', START_TIME)

    # Isolate teh data requiring outlier exclusion
    report_indics = VAR_CORR[VAR_CORR['domain'] == 'REPORT']['identifier'] \
        .unique().tolist()

    data_df_noreport = data[~data['dataElement'].isin(report_indics)].copy()

    # outlier computation

    pivot_outliers = pivot_stack(data_df_noreport)
    make_note('data pivot for outlier exclusion done', START_TIME)

    pivot_no_outliers = replace_outliers(pivot_outliers, cutoff=3)
    make_note('first outlier exclusion process done', START_TIME)

    pivot_no_outliers_iqr = replace_outliers_iqr(pivot_outliers, k=3)
    make_note('second outlier exclusion process done', START_TIME)

    stack_t_noout = pivot_stack_post_process(pivot_no_outliers)
    stack_t_noout_iqr = pivot_stack_post_process(pivot_no_outliers_iqr)
    make_note('outlier exclusion done', START_TIME)

    # Pivoting the data with and without outliers

    data_df_report = data[data['dataElement'].isin(report_indics)].copy()

    with_outliers = full_pivot(data_df_noreport, data_df_report)
    no_outliers_std = full_pivot(stack_t_noout, data_df_report)
    no_outliers_iqr = full_pivot(
        stack_t_noout_iqr, data_df_report)

    # creating the reporting table

    report_flag = data_df_noreport.copy()
    report_flag['reported'] = (report_flag['value'] > 0).astype('int')
    report_flag.drop('value', axis=1, inplace=True)
    report_flag.rename(columns={'reported': 'value'}, inplace=True)

    reporting_original = full_pivot(report_flag, data_df_report)
    reporting = add_report_columns(reporting_original)
    columns = sorted(reporting.columns)
    reporting = reporting[columns]

    reporting.to_csv(INDICATORS['report_data'])
    with_outliers.to_csv(INDICATORS['outlier_data'])
    no_outliers_std.to_csv(INDICATORS['std_no_outlier_data'])
    no_outliers_iqr.to_csv(INDICATORS['iqr_no_outlier_data'])
    make_note('breakdown in four tables done', START_TIME)

    return (reporting,
            with_outliers,
            no_outliers_std,
            no_outliers_iqr)
