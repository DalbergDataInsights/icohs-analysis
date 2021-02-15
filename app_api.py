
import argparse
import dateutil.relativedelta
import requests
from requests.auth import HTTPBasicAuth
from datetime import timedelta
import datetime
import pandas as pd
import os
import dhis_api


ENGINE = dhis_api.get_engine("config/cred.json", "credentialsEngine")
FacilitiesENGINE = dhis_api.get_engine(
    "config/api_config.json", "facilities_id")

ReportsENGINE = dhis_api.get_engine(
    "config/api_config.json", "report_Ids")

new_instance_element_groups = [id_ for name, id_ in dhis_api.get_engine(
    "config/api_config.json", "new_dataElementsGroups").items()]
old_instance_element_groups = [id_ for name, id_ in dhis_api.get_engine(
    "config/api_config.json", "old_dataElementsGroups").items()]

new_instance_dataset_id = [id_ for name, id_ in dhis_api.get_engine(
    "config/api_config.json", "new_datasetIDs").items()]
old_instance_dataset_id = [id_ for name, id_ in dhis_api.get_engine(
    "config/api_config.json", "old_datasetIDs").items()]

new_instance_report = [id_ for name, id_ in dhis_api.get_engine(
    "config/api_config.json", "report_new").items()]
old_instance_report = [id_ for name, id_ in dhis_api.get_engine(
    "config/api_config.json", "report_old").items()]

data_path = dhis_api.get_engine("config/api_config.json", "data")

new_username = ENGINE['new_DHIS2_uname']
new_password = ENGINE['new_DHIS2_password']
new_ENTRY = ENGINE['new_DHIS2_url']

old_username = ENGINE['old_DHIS2_uname']
old_password = ENGINE['old_DHIS2_password']
old_ENTRY = ENGINE['old_DHIS2_url']

download_period = dhis_api.\
    get_engine('config/api_config.json', 'download_period')

bulk_months = download_period['months_bulk']
current_months = download_period['months_curent']

pid = str(os.getpid())
pidfile = "/tmp/mydaemon.pid"
num_facilities = "facility.pid"

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('instance')
    parser.add_argument('duration')
    parser.add_argument('months')
    args = parser.parse_args()

    if args.instance == "old":

        auth = HTTPBasicAuth(old_username, old_password)
        dataset_ids = dhis_api.\
            get_resourceID_string("dataSet", old_instance_dataset_id)
        elements_groups_string = dhis_api.\
            get_resourceID_string(
                "dataElementGroup", old_instance_element_groups)

        report_ids = dhis_api.\
            get_resourceID_string("dataSet", old_instance_report)
        actual_id = ReportsENGINE['oldReportId']
        expect_id = ReportsENGINE['oldReportId']

        url = old_ENTRY
        data_loc = data_path['old_instance_data']
        report_loc = data_path['old_instance_report']
        facilities = FacilitiesENGINE['oldFacilitiesId']

        if args.duration == 'bulk':
            start_date = datetime.datetime.strptime(
                '2018-01-01', '%Y-%m-%d').date()
            start_date = datetime.datetime.strptime(
                '2019-12-31', '%Y-%m-%d').date()

        elif args.duration == 'current':
            start_date = datetime.datetime.strptime(
                '2019-09-01', '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(
                '2019-12-01', '%Y-%m-%d').date()
    elif args.instance == "new":

        auth = HTTPBasicAuth(new_username, new_password)
        dataset_ids = dhis_api.get_resourceID_string(
            "dataSet", new_instance_dataset_id)
        elements_groups_string = dhis_api.get_resourceID_string(
            "dataElementGroup", new_instance_element_groups)

        report_ids = dhis_api.get_resourceID_string(
            "dataSet", new_instance_report)
        facilities = FacilitiesENGINE['newFacilitiesId']

        actual_id = ReportsENGINE['newReportId']
        expect_id = ReportsENGINE['newReportId']
        url = new_ENTRY

        data_loc = data_path['new_instance_data']
        report_loc = data_path['new_instance_report']

        if args.duration == 'bulk':
            start_date = datetime.datetime.\
                strptime('2020-01-01', '%Y-%m-%d').date()
            end_date = datetime.date.today()

        elif args.duration == 'current':
            today = datetime.datetime.now().replace(day=1)
            end_date = today - datetime.timedelta(days=1)
            start_date = datetime.datetime.now().\
                replace(day=1) + dateutil.relativedelta.\
                relativedelta(months=-int(args.months))
    try:
        org_group_list = dhis_api.\
            processes_facility(auth, url, facilities)
        print(len(org_group_list))

        categoryOption = dhis_api.get_dhis_index_table(
            auth, url, table='categoryOptionCombos')

        org_unit_index = dhis_api.get_dhis_index_table(
            auth, url, table='organisationUnits')
        categoryOption = dhis_api.get_dhis_index_table(
            auth, url, table='categoryOptionCombos')

        batchsize = 50
        i = 0
        writeHeader = True
        open(pidfile, 'w+').write(pid)  # write processs ID
        total_facilities = 0

        while start_date <= end_date:

            delta = timedelta(dhis_api.days_in_month(
                start_date.year, start_date.month))
            print(delta)
            startDate = start_date.strftime("%Y-%m-%d")
            endDate = (start_date + delta).strftime("%Y-%m-%d")

            date_resource = start_date.strftime("%Y%m")

            filename_month = format(start_date, '%b %Y').split()[0]
            filename_year = format(start_date, '%b %Y').split()[1]

            report_path = \
                report_loc+"_"+filename_year+"_"+filename_month+".csv"
            main_path = \
                data_loc+"_"+filename_year+"_"+filename_month+".csv"

            dhis_api.download_report_url(
                url, date_resource, report_path, auth,
                actual_id, expect_id, id_)
            facilities = 0

            for i in range(0, len(org_group_list), batchsize):
                org_units_string = dhis_api.get_resourceID_string(
                    'orgUnit', org_group_list[i:i+batchsize])

                open(num_facilities, 'w+').write(str(total_facilities))

                df = pd.DataFrame(requests.get(
                    url + f'dataValueSets?{dataset_ids}&{org_units_string}\
                        &startDate={startDate}&endDate=\
                            {endDate}&{elements_groups_string}',
                    auth=auth).json().get('dataValues'))

                df = dhis_api.set_name_from_index(
                    df, url, 'dataElement', auth=auth, fetch_index=True)

                df = dhis_api.set_name_from_index(
                    df, url, 'categoryOptionCombo', index_table=categoryOption)

                facilities = facilities + i  # sum facilities
                print(facilities)
                if writeHeader is True:
                    df.to_csv(main_path, header=True)
                    writeHeader = False
                else:
                    df.to_csv(main_path, mode='a', header=False)

            start_date += delta
            writeHeader = True
            total_facilities = total_facilities + facilities
            open(num_facilities, 'w+').write(total_facilities)

        os.unlink(pidfile)
    except:
        os.unlink(pidfile)
