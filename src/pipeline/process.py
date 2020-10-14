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


def compute_outliers(pivot, policy, location):
    if policy == 'std':
        pivot_processed = replace_outliers(pivot, cutoff=3)
    elif policy == 'iqr':
        pivot_processed = replace_outliers_iqr(pivot, k=3)

    stack = pivot_stack_post_process(pivot_processed)
    pivot_export = full_pivot(stack, location)

    make_note(f'{policy} outlier exclusion process done', START_TIME)

    return pivot_export

##########################################
#     Packaging the data for export      #
##########################################


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

    data.drop(['e', 'a+e'], axis=1, inplace=True)

    return data


def create_reporting_pivot(main, report, location):

    main['reported'] = (main['value'] > 0).astype('int')
    main.drop('value', axis=1, inplace=True)
    main.rename(columns={'reported': 'value'}, inplace=True)

    reporting = full_pivot(main, location, for_report=True, report_data=report)
    reporting = add_report_columns(reporting)
    reporting.drop(columns=['expected_105_1_reporting',
                            'actual_105_1_reporting'], inplace=True)

    make_note('Reporting done', START_TIME)

    return reporting


def pivot_for_export(data):

    data_pivot = data.pivot_table(index=['orgUnit', 'year', 'month'],
                                  columns=['dataElement'],
                                  aggfunc='sum')
    data_pivot.columns = data_pivot.columns.droplevel(0)

    return data_pivot


def full_pivot(data, location, for_report=False, *report_data):

    data_pivot = pivot_for_export(data)

    if for_report == True:
        data_report_pivot = pivot_for_export(report_data)
        data_pivot = pd.merge(data_pivot, data_report_pivot,
                              left_index=True, right_index=True, how='left')

    columns = sorted(data_pivot.columns)

    data_pivot = data_pivot.reset_index()

    # Create date and delete year and month

    data_pivot['date'] = data_pivot['year'].astype(str)\
        + '-' + data_pivot['month']
    data_pivot.drop(columns=['year', 'month'], inplace=True)
    data_pivot['date'] = data_pivot.date.apply(format_date)

    # add facility_name and district

    data_pivot = pd.merge(data_pivot, location,
                          left_on='orgUnit', right_on='facilitycode', how='left')

    data_pivot = data_pivot.reset_index().rename(
        columns={'orgUnit': 'facility_id', 'facilityname': 'facility_name', 'districtname': 'id'})

    columns = ['id', 'date', 'facility_id', 'facility_name'] + columns

    return data_pivot[columns]

#############################
#     Run all functions     #
#############################


def process(main, report, location):
    make_note('Starting the data processing', START_TIME)

    pivot_outliers = pivot_stack(main)
    with_outliers = full_pivot(main, location)
    make_note('data pivot for outlier exclusion done', START_TIME)

    # outlier computations

    #no_outliers_std = compute_outliers(pivot_outliers, 'std', location)
    #no_outliers_iqr = compute_outliers(pivot_outliers, 'iqr', location)
    make_note('outlier exclusion done', START_TIME)

    # creating the reporting table

    reporting = create_reporting_pivot(main, report, location)

    print(reporting.head())

    #reporting.to_csv(INDICATORS['report_data'], index=False)
    #with_outliers.to_csv(INDICATORS['outlier_data'], index=False)
    #no_outliers_std.to_csv(INDICATORS['std_no_outlier_data'], index=False)
    #no_outliers_iqr.to_csv(INDICATORS['iqr_no_outlier_data'], index=False)

    make_note('breakdown in four tables done', START_TIME)
