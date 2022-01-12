import numpy as np
import os
import pandas as pd
import json
from datetime import datetime
from src.helpers import INDICATORS, cap_string


from dotenv import load_dotenv, find_dotenv  # NOQA: E402

load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402
from src.db import adpter as db  # NOQA: E402


#################################################
#     Define data transformation functions      #
#################################################

with open(INDICATORS["indic_config"], "r") as f:
    CONFIG = json.load(f)


def add_pop(pop, df):

    pop = pop[pop.columns[1:]]

    df["year"] = pd.to_datetime(df["date"]).dt.year

    df = pd.merge(
        df,
        pop,
        how="left",
        left_on=["district_name", "year"],
        right_on=["district_name", "year"],
    ).drop(columns=["year"])

    fcount = (
        df[["district_name", "facility_id", "date"]]
        .drop_duplicates()
        .groupby(["district_name", "date"], as_index=False)
        .count()
        .rename(columns={"facility_id": "count"})
    )

    df = pd.merge(
        df,
        fcount,
        how="left",
        left_on=["district_name", "date"],
        right_on=["district_name", "date"],
    )

    for c in pop.columns[2:]:
        df[c] = df[c] / df["count"]

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

        if i.get("function") == "nansum":
            value = get_value_indic(df, i)
            df[i.get("indicator")] = value

        if report == False and i.get("function") == "ratio":

            formula = i.get("elements")

            denominator = get_value_indic(df, formula.get("denominator"))
            value = get_value_indic(df, formula.get("numerator")) / denominator
            total = np.nansum(get_value_indic(df, formula.get("denominator")))

            weight = denominator / total
            weighted_ratio = value * weight

            df[f'{cap_string(i.get("indicator"),58)}__wr'] = weighted_ratio * int(10e6)

            weight_name = cap_string('_'.join(formula
                                              .get("denominator")
                                              .get("elements")),
                                     58)


            if f"{weight_name}__w" not in df.columns:
                df[f"{weight_name}__w"] = weight * int(10e9)

    df = df.drop(columns=cols)

    return df


#########################
#     Run function      #
#########################


def transform_to_indic(df, pop, name):

    df = add_pop(pop, df)

    if name == "rep":
        df = get_indicators(df, report=True)
    else:
        #print(df)
        #print(df.columns)
        df = get_indicators(df)

    df = df.fillna(0)

    for col in df.columns[4:]:
        df[col] = round(df[col]).astype(int)

    # db.output_to_test_sqlite(df, name, os.environ.get("SQLITE_URL"))

    df.to_csv(INDICATORS[f"{name}_indic"], index=False)


def pass_on_config():

    with open(INDICATORS["indic_config"], "r") as f:
        df = pd.read_json(f)

    denominator_df = df[df.function == "ratio"]
    df["denominator"] = ""

    for x in denominator_df.index:
        df.loc[x, "denominator"] = "_".join(
            df.loc[x, "elements"].get("denominator").get("elements")
        )

    df = df.drop(columns="elements")

    #db.config_to_test_sqlite(df, os.environ.get("SQLITE_URL"))
    df.to_csv(INDICATORS["viz_config"], index=False)


def transform_for_dhis2(df, map, outtype):

    # stack in a tall format and renaming

    df = df.set_index(['district_name', 'facility_id', 'facility_name', 'date'])

    stack = df.stack(dropna=True).reset_index()
    stack.rename(columns={0: 'value',
                          'level_4': 'dataelement',
                          'facility_id': 'orgunit',
                          'date': 'period'},
                 inplace=True)
    stack.drop('facility_name', axis=1, inplace=True)

    # Restoring proper types/format

    stack['value'] = stack['value'].astype(dtype='float64')
    stack['period'] = stack.period.astype('str').apply(lambda x: x[:4] + x[5:7])

    # replace with codes and reorder columns

    map_dict = dict(zip(map.loc[:, 'indicatorname'], map.loc[:, f'indicatorcode_{outtype}']))

    stack['dataelement'] = stack.dataelement.map(map_dict)
    stack["catoptcombo"] = 'HllvX50cXC0'
    stack["attroptcombo"] = 'HllvX50cXC0'

    print(f"{outtype} formatting for DHIS export done")

    return stack[["dataelement", "period", "orgunit", "catoptcombo", "attroptcombo", "value"]]
