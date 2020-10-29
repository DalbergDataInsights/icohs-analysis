import numpy as np
from numpy.lib.shape_base import _column_stack_dispatcher
import pandas as pd
import json
from src.helpers import INDICATORS

from dotenv import load_dotenv, find_dotenv  # NOQA: E402
load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402
from src.db import adpter as db  # NOQA: E402


#################################################
#     Define data transformation functions      #
#################################################


with open(INDICATORS['viz_config'], 'r') as f:
    CONFIG = json.load(f)


# std = db.pg_read('std_no_outlier_output', getdict=False)
# iqr = db.pg_read('iqr_no_outlier_output', getdict=False)
# report = db.pg_read('report_output', getdict=False)

outlier = db.pg_read('outlier_output', getdict=False)
pop = db.pg_read('pop', getdict=False)


def prep_pop(pop, df):

    fcount = \
        df[['district_name', 'facility_id']]\
        .drop_duplicates()\
        .groupby('district_name', as_index=False)\
        .count()\
        .rename(columns={'facility_id': 'count'})

    new_pop = pd.merge(pop, fcount,
                       how='left',
                       left_on='district_name',
                       right_on='district_name')

    for c in new_pop.columns[3:-1]:
        new_pop[c] = new_pop[c]/new_pop['count']

    new_pop = new_pop[new_pop.columns[1:-1]]

    return new_pop


def add_pop(pop, df):

    pop = prep_pop(pop, df)

    df['year'] = pd.to_datetime(df['date']).dt.year

    df = pd.merge(df, pop,
                  how='left',
                  left_on=['district_name', 'year'],
                  right_on=['district_name', 'year'])\
        .drop(columns=['year'])

    return df


outlier = add_pop(pop, outlier)


def get_value_indic(df, i):

    func = getattr(np, i.get("function"))
    elements = []
    for el in i.get("elements"):
        elements.append(df[el])
    value = func(elements, axis=0)

    # if i.get("function") == "diff":
    # value = value[0]

    return value


for i in CONFIG:

    if i.get("function") == "sum":
        value = get_value_indic(outlier, i)
        outlier[i.get("indicator")] = value

    elif i.get("function") == "ratio":

        formula = i.get("elements")

        denominator = get_value_indic(outlier, formula.get("denominator"))
        value = get_value_indic(outlier, formula.get("numerator"))/denominator
        outlier[i.get("indicator")] = value

        total = get_value_indic(outlier, formula.get("denominator")).sum()
        weight = denominator/total
        outlier[f'{i.get("indicator")} -- weight'] = weight

        weighted_ratio = value * weight
        outlier[f'{i.get("indicator")} -- weighted_ratio'] = weighted_ratio


# outlier.to_csv('test_indicator_add.csv')

cols = ['date', 'DPT3 coverage (all)', 'DPT3 coverage (all) -- weight',
        'DPT3 coverage (all) -- weighted_ratio']

check = outlier[cols].groupby('date').agg(
    {cols[1]: 'mean', cols[2]: 'sum', cols[3]: 'sum'})

check['actual ratio'] = check[cols[3]]/check[cols[2]]

print(check.loc['2019-10-01', '2019-11-01', '2019-12-01'])


# FIXME : check if we have interest to have the wright be done not for each date but per date
