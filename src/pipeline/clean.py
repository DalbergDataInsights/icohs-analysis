###########################
#     Initialize file     #
###########################


# For sample :

# engine sample path

import json
import os
from datetime import datetime

import pandas as pd
from dotenv import find_dotenv, load_dotenv  # NOQA: E402
from src.helpers import INDICATORS, get_flat_list_json, make_note

load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402

###################################
#     Define functions used       #
###################################


# Get user-input data

with open(INDICATORS["data_config"], "r", encoding="utf-8") as f:
    VAR_CORR = json.load(f)

BREAK_CORR = pd.read_csv(INDICATORS["breakdown_correspondence_data"])

USECOLS = list(range(0, 9))

DTYPES = {
    "Unnamed: 0": int,
    "dataElement": str,
    "period": str,
    "orgUnit": str,
    "categoryOptionCombo": str,
    "attributeOptionCombo": str,
    "value": object,
    "storedBy": str,
    "created": str,
    "lastUpdated": str,
}

FACILITY_IDS = (
    pd.read_csv(INDICATORS["name_district_map"])["facilitycode"].unique().tolist()
)

START_TIME = datetime.now()


# Extracting reporting data


def get_reporting_data(path, instance):

    month_dict = {
        "01": "Jan",
        "02": "Feb",
        "03": "Mar",
        "04": "Apr",
        "05": "May",
        "06": "Jun",
        "07": "Jul",
        "08": "Aug",
        "09": "Sep",
        "10": "Oct",
        "11": "Nov",
        "12": "Dec",
    }

    cols = [
        "orgunitlevel1",
        "orgunitlevel2",
        "orgunitlevel3",
        "orgunitlevel4",
        "orgunitlevel5",
        "organisationunitname",
        "organisationunitcode",
        "organisationunitdescription",
        "periodid",
        "periodname",
        "periodcode",
        "perioddescription",
    ]

    # Get the right data
    df = pd.read_csv(path, dtype="object")

    var_corr_rep = [el for el in VAR_CORR if el.get("domain") == "REPORT"]

    metrics = get_flat_list_json(var_corr_rep, instance)

    for x in metrics:
        df[x] = pd.to_numeric(df[x], errors="coerce")

    make_note(str(instance) + " reporting data loaded", START_TIME)

    # Formatting dates
    df["year"] = df["periodcode"].astype("str").apply(lambda x: x[:4])
    df["month"] = (
        df["periodcode"].astype("str").apply(lambda x: x[-2:]).replace(month_dict)
    )
    df.rename(columns={"organisationunitid": "orgUnit"}, inplace=True)
    df.set_index(["orgUnit", "year", "month"], drop=True, inplace=True)

    # Dropping unused columns and renaming
    df.drop(cols, axis=1, inplace=True)
    df1 = df.copy().stack(dropna=False).reset_index()
    df1.rename(columns={0: "value", "level_3": "dataElement"}, inplace=True)
    df1["value"] = df1["value"].fillna(0).astype(int)

    return df1


# Extracting our target data


def get_data(path, instance):
    data = pd.read_csv(path, usecols=USECOLS, dtype=DTYPES)

    new_indic_list = get_flat_list_json(VAR_CORR, instance)
    new_df = data[data["dataElement"].isin(new_indic_list)].reset_index(drop=True)

    del data

    new_df["value"] = pd.to_numeric(new_df["value"], errors="coerce")

    # TODO Check this groupby for breakdown addition

    new_df = new_df.groupby(
        ["dataElement", "orgUnit", "period", "categoryOptionCombo"], as_index=False
    ).agg({"value": "sum"})
    new_df = new_df[
        ["period", "orgUnit", "value", "dataElement", "categoryOptionCombo"]
    ]

    return new_df


# Adding composite indicators


def get_variable_breakdown_dict(instance):
    """build a dict nested dictionnary matching identifiers to variables and breakdown"""

    corr = {}

    brk = BREAK_CORR[BREAK_CORR["instance"] == instance][["identifier", "breakdown"]]

    for el in VAR_CORR:

        breakdown = []

        if len(el.get("breakdown")) > 0:
            breakdown = brk[brk["identifier"].isin(el.get("breakdown"))][
                "breakdown"
            ].tolist()

        corr[el.get("identifier")] = {
            "indics": el.get(instance),
            "breakdown": breakdown,
        }

    return corr


def compute_indicators(df_in, df_out, indic_name, group_dict):
    """Compute new vars by summing original vars, and drop original vars """

    df_new = df_in[df_in["dataElement"].isin(group_dict["indics"])]

    if group_dict.get("breakdown") != []:
        df_new = df_new[df_new["categoryOptionCombo"].isin(group_dict.get("breakdown"))]

    df_new = df_new.groupby(["period", "orgUnit"], as_index=False).agg({"value": "sum"})

    df_new["dataElement"] = indic_name

    df = pd.concat([df_out, df_new])
    df.reset_index(drop=True, inplace=True)

    return df


# Transforming dates


def process_date_monthly(df):

    month_dict = {
        "01": "Jan",
        "02": "Feb",
        "03": "Mar",
        "04": "Apr",
        "05": "May",
        "06": "Jun",
        "07": "Jul",
        "08": "Aug",
        "09": "Sep",
        "10": "Oct",
        "11": "Nov",
        "12": "Dec",
    }

    df["weekly"] = df.period.str.find("W")

    df_m = df[df["weekly"] == -1].copy()
    df_m["year"] = df_m["period"].astype("str").apply(lambda x: x[:4])
    df_m["month"] = (
        df_m["period"].astype("str").apply(lambda x: x[-2:]).replace(month_dict)
    )
    df_m.drop(["weekly", "period"], inplace=True, axis=1)

    return df_m


def process_date_weekly(df):

    month_dict = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }

    df["weekly"] = df.period.str.find("W")

    df_w = df[df["weekly"] != -1].copy()
    df_w[["year", "week"]] = df_w.period.str.split("W", expand=True)
    df_w["datetime"] = pd.to_datetime(
        df_w.year + "/" + df_w.week + "/1", format="%G/%V/%u"
    )
    df_w["month"] = df_w["datetime"].dt.month.replace(month_dict)
    df_w.drop(["week", "weekly", "datetime", "period"], inplace=True, axis=1)
    df_w["year"] = df_w.year.astype("str")

    return df_w


def process_date(df):
    df_m = process_date_monthly(df)
    df_w = process_date_weekly(df)
    df_new = pd.concat([df_m, df_w])
    return df_new


def clean_add_indicators(file_path, instance):

    make_note(f"Creating additional indicators for {instance}", START_TIME)

    add_dict = get_variable_breakdown_dict(instance)

    dhis_df = get_data(file_path, instance)

    df = pd.DataFrame(columns=dhis_df.columns)

    for indicator in add_dict.keys():
        df = compute_indicators(dhis_df, df, indicator, add_dict.get(indicator))

    df = process_date(df)

    return df


# Putting together cleaning steps


def get_renaming_dict():

    out_dict = {}

    for el in VAR_CORR:
        for m in el.get("new"):
            out_dict[m] = el.get("identifier")
        for m in el.get("old"):
            out_dict[m] = el.get("identifier")

    return out_dict


def clean_raw_file(raw_path):
    """Take one file, checks whether it fits expected format, and clean it"""

    # TODO check what is up with that renaming thing - I thinkI don't need it

    # Check file name format

    f = raw_path.split("/")[-1][:-4]
    instance, table, year, month = f.split("_")

    assert table in [
        "main",
        "report",
    ], f"Unexpected data type in file name for {f}: correct format is [instance_datatype_YYYYMmm], e.g. new_main_2020Apr"
    assert instance in [
        "new",
        "old",
    ], f"Unexpected dhis2 instance in file name for {f}: correct format is [instance_datatype_YYYYMmm], e.g. new_main_2020Apr"

    # import file and get to standard format

    if table == "main":
        df = clean_add_indicators(raw_path, instance)
    else:
        df = get_reporting_data(raw_path, instance)
        renaming_dict = get_renaming_dict()
        df["dataElement"].replace(renaming_dict, inplace=True)

    assert df["year"].nunique() == 1, f"Data for several years found in file {f}"
    assert df["month"].nunique() == 1, f"Data for several months found in file {f}"

    assert int(df["year"].unique()[0]) == int(
        year
    ), f"Data from a different year than what file name indicates for {f}"
    assert (
        df["month"].unique()[0] == month
    ), f"Data from a different year than what file name indicates for {f}"

    make_note(f"data imported for file {f}", START_TIME)

    # cleaning formatted table

    df.reset_index(drop=True, inplace=True)

    # TODO Check this groupby for breakdown addition

    df = df.groupby(["dataElement", "orgUnit", "year", "month"], as_index=False).agg(
        {"value": "sum"}
    )
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df[df["orgUnit"].isin(FACILITY_IDS)]

    if table == "report":
        df["value"] = (df["value"] > 0).astype("int")

    return df


#########################
#     Run functions     #
#########################
def clean_pop_to_temp(pop_path, pop_perc_path):

    pop = pd.read_csv(pop_path)

    district_name_dict = {
        "SEMBABULE": "SSEMBABULE",
        "MADI-OKOLLO": "MADI OKOLLO",
        "LUWEERO": "LUWERO",
    }
    pop["District"] = pop["District"].apply(lambda x: x.upper())
    pop["District"].replace(district_name_dict, inplace=True)

    pop["Age"] = pop["Single Years"].apply(lambda x: " ".join(x.split(" ")[:1]))
    pop["Age"].replace({"80+": "80"}, inplace=True)
    pop["Age"] = pop["Age"].astype("int")

    pop.drop(["Single Years", "Year2", "FY"], axis=1, inplace=True)

    pop = pop.groupby(["District", "Year"], as_index=False).sum()

    pop = pop[["District", "Year", "Male", "Female", "Total"]]

    perc = pd.read_csv(pop_perc_path).set_index("metric")

    for x in perc.index:
        pop[x] = pop["Total"] * (perc.loc[x, "percentage"] / 100)

    for c in [
        "childbearing_age",
        "pregnant",
        "not_pregnant",
        "births_estimated",
        "u1",
        "u5",
        "u15",
        "girls_10",
    ]:
        pop[c] = pop[c] / 12

    pop.to_csv("data/temp/pop.csv", index=False, header=False)

    return ", ".join(perc.index)


def clean(raw_path):

    file_name = raw_path.split("/")[-1]
    make_note(f"Starting the cleaning process for {file_name}", START_TIME)

    clean_df = clean_raw_file(raw_path)
    make_note(f"Cleaning of raw file done for {file_name}", START_TIME)

    return clean_df


def map_to_temp(raw_path, map, clean_df):

    f = raw_path.split("/")[-1]
    f_short = f[:-4]
    instance, table, year, month = f_short.split("_")

    map_dict = dict(zip(map.loc[:, 'indicatorname'], map.loc[:, 'indicatorcode_out']))

    clean_df["dataElement"] = clean_df["dataElement"].map(map_dict)

    f_path = f"data/temp/{f_short}_clean.csv"

    clean_df[["orgUnit", "dataElement", "year", "month", "value"]].to_csv(
        f_path, index=False, header=False
    )

    make_note(f"Creation of temporary csv done for {f}", START_TIME)

    return f_path, year, month, table


def move_csv_files(raw_path, processed_path):

    try:
        os.remove(processed_path)
    except Exception as e:
        print(e)

    os.rename(raw_path, processed_path)
