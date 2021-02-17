import json
from datetime import datetime
import calendar


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


def make_note(statement, start_time):
    log_path = INDICATORS["log_file"]
    now = datetime.now()
    timed_statement = statement, str(now - start_time)
    print(timed_statement)
    with open(log_path, "a+") as f:
        f.write(f"{now}: {timed_statement}\n")


def get_unique_indics(var_corr, excl_report=False):

    indics = []

    for el in var_corr:
        indics.append(el.get("identifier"))

    out = list(set(indics))

    if excl_report:
        report_list = ["expected_105_1_reporting", "actual_105_1_reporting"]
        out = [ele for ele in out if ele not in report_list]

    return out


def format_date(date):
    year, month = date.split("-")
    month_order = str(list(calendar.month_abbr).index(month))
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

def get_from_config(identifier):
    return [id_ for name, id_ in api_pull.get_engine(
    "config/api_config.json", identifier).items()]