###########################
#     Initialize file     #
###########################


# For sample :

# l.305 csv export
# l.335,337 chnage sample cols
# l.371 csv export
# l.459 valid ids
# engine sample path

from datetime import datetime

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
    pivot_outliers = df.pivot_table(index=['districts', 'orgUnit', 'dataElement'],
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

        pivot_no_outliers.iloc[pivot_outliers.index \
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

        pivot_no_outliers.iloc[pivot_outliers.index.get_loc(x), :] = new_values.astype('float')

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


def export_full_table_to_csv(stack_t_noreport, stack_t_noout, stack_t_noout_iqr, data_df_report):

    # TODO: break down in smaller functions

    fac_stack_final = pd.merge(stack_t_noreport,
                               stack_t_noout,
                               how='left',
                               left_on=['districts', 'orgUnit','year', 'dataElement', 'month'],
                               right_on=['districts', 'orgUnit', 'year', 'dataElement', 'month']) \
                        .rename(columns={'value_x': 'value_out', 'value_y': 'value_noout'})

    fac_stack_final = pd.merge(fac_stack_final,
                               stack_t_noout_iqr,
                               how='left',
                               left_on=['districts', 'orgUnit','year', 'dataElement', 'month'],
                               right_on=['districts', 'orgUnit', 'year', 'dataElement', 'month']) \
                         .rename(columns={'value': 'value_noout_iqr'})

    # Make a note of whcih facilities reported on each indicator and which didn't
    fac_stack_final['reported'] = (fac_stack_final['value_out'] > 0).astype('int')

    # Add in the reporting rate data, that did not go through theoutlier precocedure
    data_df_report.rename(columns={'value': 'reported'}, inplace=True)
    data_df_report.set_index(['districts', 'orgUnit', 'year', 'dataElement', 'month'],
                             inplace=True,
                             drop=True)

    data_df_report.reset_index(inplace=True)

    # Puts it all together
    fac_stack_final = pd.concat([data_df_report, fac_stack_final],
                                ignore_index=True)
    fac_stack_final = pd.melt(fac_stack_final,
                              id_vars=['districts', 'orgUnit', 'year', 'dataElement', 'month'],
                              value_vars=['reported', 'value_out', 'value_noout', 'value_noout_iqr'])
    fac_stack_final.rename(columns={'variable': 'dataset'}, inplace=True)

    # Create a pivot
    fac_pivot_final = fac_stack_final.pivot_table(
        index=['districts', 'orgUnit', 'year', 'month', 'dataset'], columns=['dataElement'], aggfunc='mean')
    fac_pivot_final = fac_pivot_final.stack(level=[0])
    # fac_pivot_final.to_csv('data/output/processed_wide_data.csv')

    return fac_pivot_final


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

    cols = VAR_CORR[VAR_CORR['domain'] != 'REPORT']['identifier'].unique().tolist()
    #sample_cols = set(cols).intersection(set(data.columns))

    for x in cols:  # Sample change
        data['i'] = data[x] * 7
        data['sum'] = data[['i', 'e', 'a+e']].sum(axis=1)
        data.drop([x, 'i'], axis=1, inplace=True)
        sum_col = data['sum']  # .replace(value_dict)
        data[x] = sum_col
        data.drop('sum', axis=1, inplace=True)
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

    # reporting_add.to_csv(ENGINE['report_data'])
    # with_outliers.to_csv(ENGINE['outlier_data'])
    # no_outliers_std.to_csv(ENGINE['std_no_outlier_data'])
    # no_outliers_iqr.to_csv(ENGINE['iqr_no_outlier_data'])

    reporting_add.to_csv('test.csv')

    return (reporting_add, with_outliers, no_outliers_std, no_outliers_iqr)


#############################
#     Run all functions     #
#############################

def process(data):
    make_note('Starting the data processing', START_TIME)

    # separe reports and non reports indicators
    report_indics = VAR_CORR[VAR_CORR['domain'] == 'REPORT']['identifier'] \
        .unique().tolist()

    data_df_noreport = data[~data['dataElement'].isin(report_indics)].copy()
    data_df_report = data[data['dataElement'].isin(report_indics)].copy()

    # outlier computattion on non report data

    pivot_outliers = pivot_stack(data_df_noreport)
    make_note('data pivot for outlier exclusion done', START_TIME)

    pivot_no_outliers = replace_outliers(pivot_outliers, cutoff=3)
    make_note('first outlier exclusion process done', START_TIME)

    pivot_no_outliers_iqr = replace_outliers_iqr(pivot_outliers, k=3)
    make_note('second outlier exclusion process done', START_TIME)

    stack_t_noout = pivot_stack_post_process(pivot_no_outliers)
    stack_t_noout_iqr = pivot_stack_post_process(pivot_no_outliers_iqr)
    make_note('stacking of the outlier-excluded data done', START_TIME)

    pivot_final = export_full_table_to_csv(
        data_df_noreport, stack_t_noout, stack_t_noout_iqr, data_df_report)
    make_note('data concatenatation done', START_TIME)

    (facility_data_reporting,
    facility_data_with_outliers,
    facility_data_no_outliers_std,
    facility_data_no_outliers_iqr) = export_broken_down_table_to_csv(pivot_final)
    make_note('breakdown in four tables done', START_TIME)

    return (pivot_final,
            facility_data_reporting,
            facility_data_with_outliers,
            facility_data_no_outliers_std,
            facility_data_no_outliers_iqr)
