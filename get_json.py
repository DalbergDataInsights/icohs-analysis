import pandas as pd


import json
import numpy as np
import pandas as pd

# from store import Database

# db = Database()

# df = db.raw_data

# df = rename_df_columns(df, rename_from="indicator_name",
#                        rename_to="indicator_id")


f = open('config_test.json', 'r')

indicator = json.load(f)

# switch(i.get("type"))
# "ratio":
# code

df = pd.DataFrame({"abortions_gbv": range(1, 100, 2),
                   "abortions_other": range(100, 1, -2)})


def get_value_indic(i):

    func = getattr(np, i.get("function"))
    elements = []
    for de in i.get("elements"):
        elements.append(df[de])
    value = func(elements, axis=0)

    # if i.get("function") == "diff":
    # value = value[0]

    return value


for i in indicator:

    if i.get("function") == "sum":
        value = get_value_indic(i)
    elif i.get("function") == "ratio":
        formula = i.get("elements")
        denominator = get_value_indic(formula.get("denominator"))
        value = get_value_indic(formula.get("numerator"))/denominator
    else:
        value = np.nan

    df[i.get("indicator")] = value

print(df)
