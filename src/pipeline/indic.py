import numpy as np
import pandas as pd
import json
from src.helpers import INDICATORS

from dotenv import load_dotenv, find_dotenv  # NOQA: E402
load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402
from src.db import adpter as db  # NOQA: E402


#################################################
#     Define data transformation functions      #
#################################################

with open(INDICATORS['indic_config'], 'r') as f:
    CONFIG = json.load(f)

##############################
#     Support functions      #
##############################


def add_pop(pop, df):

    pop = pop[pop.columns[1:]]

    df['year'] = pd.to_datetime(df['date']).dt.year

    df = pd.merge(df, pop,
                  how='left',
                  left_on=['district_name', 'year'],
                  right_on=['district_name', 'year'])\
        .drop(columns=['year'])

    fcount = \
        df[['district_name', 'facility_id', 'date']]\
        .drop_duplicates()\
        .groupby(['district_name', 'date'], as_index=False)\
        .count()\
        .rename(columns={'facility_id': 'count'})

    df = pd.merge(df, fcount,
                  how='left',
                  left_on=['district_name', 'date'],
                  right_on=['district_name', 'date'])

    for c in pop.columns[2:]:
        df[c] = df[c]/df['count']

    for c in ['childbearing_age', 'pregnant', 'not_pregnant', 'birth', 'u1', 'u5', 'u15']:
        df[c] = df[c]/12

    return df


def get_value_indic(df, i):

    func = getattr(np, i.get("function"))
    elements = []
    for el in i.get("elements"):
        elements.append(df[el])
    value = func(elements, axis=0)

    return value


def get_indicators(df, report=False):

    cols = list(df.columns[4:])

    for i in CONFIG:

        if i.get("function") == "single":
            value = df[i.get("elements")[0]]
            df[i.get("indicator")] = value

        if report == False:

            if i.get("function") == "sum":
                value = get_value_indic(df, i)
                df[i.get("indicator")] = value

            elif i.get("function") == "ratio":

                formula = i.get("elements")

                denominator = get_value_indic(df, formula.get("denominator"))
                value = get_value_indic(
                    df, formula.get("numerator"))/denominator
                total = get_value_indic(df, formula.get("denominator")).sum()

                weight = denominator/total
                weighted_ratio = value * weight

                df[f'{i.get("indicator")} -- weight'] = weight
                df[f'{i.get("indicator")} -- weighted_ratio'] = weighted_ratio

    df = df.drop(columns=cols)

    return df

#########################
#     Run function      #
#########################


def transform_to_indic(df, pop, name):

    df = add_pop(pop, df)

    if name == 'rep':
        df = get_indicators(df, report=True)
    else:
        df = get_indicators(df)

    df.to_csv(INDICATORS[f'{name}_indic'])


def pass_on_config():

    with open(INDICATORS['indic_config'], 'r') as f:
        df = pd.read_json(f)

    df = df.drop(columns='elements')

    df.to_csv(INDICATORS['viz_config'], index=False)
