import json
from datetime import datetime


def parse_config(
    config_path="config/paths.json",
    config_section=["input", "static", "to_classify", "output", "logs"],
):

    with open(config_path) as f:

        ENGINE = {
            p["identifier"]: p["value"]
            for section in json.load(f)
            for x in config_section
            for p in section[x]
        }

    return ENGINE


INDICATORS = parse_config()


def parse_config1(
    config_path="config/paths.json",
    config_section=["input", "static", "to_classify", "output"],
):

    with open(config_path) as f:
        for section in json.load(f):
            for x in config_section:
                print(section[x])

        ENGINE = {
            p["identifier"]: p["value"] for section in json.load(f) for p in section[x]
        }

    return ENGINE


def make_note(statement, start_time):
    log_path = INDICATORS["log_file"]
    now = datetime.now()
    timed_statement = statement, str(now - start_time)
    print(timed_statement)
    with open(log_path, "a+") as f:
        f.write(f"{now}: {timed_statement}\n")


def get_unique_indics(var_corr, excl_domain=None):

    indics = []

    for el in var_corr:
        indics.append(el.get("identifier"))

    out = list(set(indics))

    return out


def format_date(date):
    dates = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    assert len(dates) == 12
    year, month = date.split("-")
    month_order = str(dates.index(month) + 1)
    month_order = "0" + month_order if len(month_order) == 1 else month_order
    return year + "-" + month_order + "-01"


def cap_string(string, cap):
    return (string[:cap] + "..") if len(string) > cap else string


def get_flat_list_json(json_dict, key):

    l = []
    for el in json_dict:
        for x in el.get(key):
            l.append(x)

    return l
