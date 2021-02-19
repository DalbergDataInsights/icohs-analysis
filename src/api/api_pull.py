import pandas as pd
import requests
import json
import datetime
from io import StringIO
import calendar
from tenacity import retry, stop_after_attempt


def get_engine(json_file, sec):
    with open(json_file) as f:
        ENGINE = dict(
            (p["identifier"], p["value"])
            for section in json.load(f)
            for p in section[sec]
        )
        return ENGINE


def get_from_config(identifier):
    return [
        id_ for name, id_ in get_engine("config/api_config.json", identifier).items()
    ]


@retry(stop=stop_after_attempt(5))
def get_pageSize(resource, auth, url):
    """
    Given a given resource, this function returns the number of pages
    resource: name of the resource
    """
    page_size = (
        requests.get(url + f"{resource}", auth=auth).json().get("pager").get("total")
    )
    return page_size


def processes_facility(auth, url, id, table="organisationUnitGroups"):
    page_size = 100000  # get_pageSize(table, auth, url)
    facilitiesPath = "all_facilities.pid"
    response = requests.get(url + f"{table}/{id}?pageSize={page_size}", auth=auth)
    orgs = pd.DataFrame(response.json().get("organisationUnits"))
    orgs = orgs["id"]
    num_fac = len(orgs)

    open(facilitiesPath, "w+").write(str(num_fac))
    return list(orgs)


def get_resourceID_string(identifier, ids_list):
    resource_id_string = f"&{identifier}=".join(ids_list)
    resource_id_string = f"{identifier}=" + resource_id_string
    return resource_id_string


def get_date_resourceID_string(identifier, ids_list):
    resource_id_string = f"{identifier}".join(ids_list)
    return resource_id_string


@retry(stop=stop_after_attempt(5))
def get_dhis_index_table(auth, url, table="dataElements"):
    page_size = 100000  # get_pageSize(table, auth, url)
    response = requests.get(url + f"{table}?pageSize={page_size}", auth=auth)

    df = pd.DataFrame(response.json().get(table))
    return df


@retry(stop=stop_after_attempt(5))
def set_name_from_index(
    df, url, column_name, index_table=None, fetch_index=False, auth=None
):
    if fetch_index:
        assert (
            auth is not None
        ), "Cannot retrieve the index tables \
            without authentication object"
        index_table = get_dhis_index_table(auth, url, table=column_name + "s")
    else:
        assert (
            index_table is not None
        ), "Set fetch index or \
            pass the index table"
    if column_name == "organisationUnit":
        column_name = "orgUnit"
        index_table.rename(columns={"organisationUnit": "orgUnit"})
    if column_name == "categoryOptions":
        column_name = "categoryOptionCombo"
        index_table.rename(columns={"categoryOptions": "categoryOptionCombo"})
    df[column_name] = df[column_name].apply(
        lambda x: index_table[index_table["id"].str.contains(x)]["displayName"].values[
            0
        ]
    )
    return df


def download_report(startDate, endDate, url, auth_, file_name, date_resource):
    response = requests.get(url, auth=auth_)
    dset = pd.DataFrame(pd.read_csv(StringIO(response.text)))
    dset.to_csv(file_name, header=True)


def days_in_month(year, month):
    if (month < 1) or (month > 12):
        print("please enter a valid month")
    elif (year < 1) or (year > 9999):
        print("please enter a valid year between 1 - 9999")
    else:
        return calendar.monthrange(year, month)[1]


def get_previous_month():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)

    return lastMonth.year, lastMonth.month


@retry(stop=stop_after_attempt(5))
def download_report_url(url, date_resource, file_name, auth_, actual_id, expect_id):
    report_url = (
        url
        + f"29/analytics.csv?dimension=dx:{actual_id}.\
        ACTUAL_REPORTS;{expect_id}.\
            EXPECTED_REPORTS&dimension=ICjKVP3jwYl:l4UMmqvSBe5&dimension=ou:\
                LEVEL-5;akV6429SUqu&dimension=pe:{date_resource}\
                    &displayProperty=NAME&tableLayout=true&columns=dx;\
                    ICjKVP3jwYl&rows=ou;pe&showHierarchy=true"
    )
    response = requests.get(report_url, auth=auth_)
    dset = pd.DataFrame(pd.read_csv(StringIO(response.text)))
    dset.to_csv(file_name, header=True)
