import dateutil.relativedelta as delta
import requests
from requests.auth import HTTPBasicAuth
from datetime import timedelta
import datetime
import pandas as pd
import os
from src.api import api_pull


# Declaring global variables

ENGINE = api_pull.get_engine("config/cred.json", "credentialsEngine")
api_config_path = "config/api_config.json"

FacilitiesENGINE = api_pull.get_engine(api_config_path, "facilities_id")
ReportsENGINE = api_pull.get_engine(api_config_path, "report_Ids")
data_path = api_pull.get_engine(api_config_path, "data")
download_period = api_pull.get_engine(api_config_path, "download_period")

bulk_months = download_period["months_bulk"]
current_months = download_period["months_curent"]

pid = str(os.getpid())
pidfile = "mydaemon.pid"
num_facilities = "facility.pid"


def get_set_up_dict(instance, duration, months):

    if instance == "old":

        auth = HTTPBasicAuth(ENGINE["old_DHIS2_uname"], ENGINE["old_DHIS2_password"])
        dataset_ids = api_pull.get_resourceID_string(
            "dataSet", api_pull.get_from_config("old_datasetIDs")
        )
        elements_groups_string = api_pull.get_resourceID_string(
            "dataElementGroup", api_pull.get_from_config("old_dataElementsGroups")
        )

        actual_id = ReportsENGINE["oldReportId"]
        expect_id = ReportsENGINE["oldReportId"]

        url = ENGINE["old_DHIS2_url"]
        data_loc = data_path["old_instance_data"]
        report_loc = data_path["old_instance_report"]
        facilities = FacilitiesENGINE["oldFacilitiesId"]

        if duration == "bulk":
            start_date = datetime.datetime.strptime("2018-01-01", "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime("2019-12-31", "%Y-%m-%d").date()

        elif duration == "current":
            start_date = datetime.datetime.strptime("2019-09-01", "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime("2019-12-01", "%Y-%m-%d").date()

    elif instance == "new":

        auth = HTTPBasicAuth(ENGINE["new_DHIS2_uname"], ENGINE["new_DHIS2_password"])
        dataset_ids = api_pull.get_resourceID_string(
            "dataSet", api_pull.get_from_config("new_datasetIDs")
        )
        elements_groups_string = api_pull.get_resourceID_string(
            "dataElementGroup", api_pull.get_from_config("new_dataElementsGroups")
        )

        facilities = FacilitiesENGINE["newFacilitiesId"]

        actual_id = ReportsENGINE["newReportId"]
        expect_id = ReportsENGINE["newReportId"]
        url = ENGINE["new_DHIS2_url"]

        data_loc = data_path["new_instance_data"]
        report_loc = data_path["new_instance_report"]

        if duration == "bulk":
            start_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d").date()
            end_date = datetime.date.today()

        elif duration == "current":
            today = datetime.datetime.now().replace(day=1)
            if datetime.datetime.now().day < 18:
                today = today + delta.relativedelta(months=-1)
            end_date = today - datetime.timedelta(days=1)
            start_date = today + delta.relativedelta(months=-int(months))

    param_dict = {
        "auth": auth,
        "dataset_ids": dataset_ids,
        "elements_groups_string": elements_groups_string,
        "actual_id": actual_id,
        "expect_id": expect_id,
        "url": url,
        "data_loc": data_loc,
        "report_loc": report_loc,
        "facilities": facilities,
        "start_date": start_date,
        "end_date": end_date,
    }

    return param_dict


def run(instance, duration, months):

    param_dict = get_set_up_dict(instance, duration, months)

    try:
        org_group_list = api_pull.processes_facility(
            param_dict.get("auth"), param_dict.get("url"), param_dict.get("facilities")
        )
        org_group_list = org_group_list[:300]  # TODO Remove once test done
        print(len(org_group_list))

        categoryOption = api_pull.get_dhis_index_table(
            param_dict.get("auth"), param_dict.get("url"), table="categoryOptionCombos"
        )

        org_unit_index = api_pull.get_dhis_index_table(
            param_dict.get("auth"), param_dict.get("url"), table="organisationUnits"
        )

        categoryOption = api_pull.get_dhis_index_table(
            param_dict.get("auth"), param_dict.get("url"), table="categoryOptionCombos"
        )

        batchsize = 50
        i = 0
        writeHeader = True
        open(pidfile, "w+").write(pid)  # write processs ID
        total_facilities = 0

        start_date = param_dict.get("start_date")
        end_date = param_dict.get("end_date")

        while start_date <= end_date:

            delta = timedelta(api_pull.days_in_month(start_date.year, start_date.month))
            print(delta)
            startDate = start_date.strftime("%Y-%m-%d")
            endDate = (start_date + delta).strftime("%Y-%m-%d")

            date_resource = start_date.strftime("%Y%m")

            filename_month = format(start_date, "%b %Y").split()[0]
            filename_year = format(start_date, "%b %Y").split()[1]

            report_path = (
                param_dict.get("report_loc")
                + "_"
                + filename_year
                + "_"
                + filename_month
                + ".csv"
            )
            main_path = (
                param_dict.get("data_loc")
                + "_"
                + filename_year
                + "_"
                + filename_month
                + ".csv"
            )

            api_pull.download_report_url(
                param_dict.get("url"),
                date_resource,
                report_path,
                param_dict.get("auth"),
                param_dict.get("actual_id"),
                param_dict.get("expect_id"),
            )
            facilities = 0

            for i in range(0, len(org_group_list), batchsize):
                org_units_string = api_pull.get_resourceID_string(
                    "orgUnit", org_group_list[i : i + batchsize]
                )

                open(num_facilities, "w+").write(str(total_facilities))

                df = pd.DataFrame(
                    requests.get(
                        param_dict.get("url")
                        + f'dataValueSets?{param_dict.get("dataset_ids")}&{org_units_string}&startDate={startDate}&endDate={endDate}&{param_dict.get("elements_groups_string")}',
                        auth=param_dict.get("auth"),
                    )
                    .json()
                    .get("dataValues")
                )

                df = api_pull.set_name_from_index(
                    df,
                    param_dict.get("url"),
                    "dataElement",
                    auth=param_dict.get("auth"),
                    fetch_index=True,
                )

                df = api_pull.set_name_from_index(
                    df,
                    param_dict.get("url"),
                    "categoryOptionCombo",
                    index_table=categoryOption,
                )

                facilities = facilities + i  # sum facilities
                print(facilities)
                if writeHeader is True:
                    df.to_csv(main_path, header=True)
                    writeHeader = False
                else:
                    df.to_csv(main_path, mode="a", header=False)

            start_date += delta
            writeHeader = True
            total_facilities = total_facilities + facilities
            open(num_facilities, "w+").write(str(total_facilities))

        os.unlink(pidfile)

    except Exception as e:
        print(e)
        os.unlink(pidfile)
