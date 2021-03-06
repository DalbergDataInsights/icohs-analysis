import os
import subprocess
import shlex
import multiprocessing
from multiprocessing.pool import ThreadPool
import pandas as pd
import json
import requests
from requests.auth import HTTPBasicAuth
import re
from datetime import datetime
from io import StringIO


class Dhis:
    def __init__(self, username, password, url, log_list=None):
        self.username = username
        self.password = password
        self.url = url
        if log_list == None:
            self.log_list = []

    def __run(self, cmd):
        args = shlex.split(cmd)
        process = subprocess.Popen(
            args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        print(stdout)

        return stdout

    def to_list(self, file):
        if isinstance(file, list):
            return file
        else:
            return [file]

    def log_result(self, result):

        self.log_list.append(result)

    def get_resourceID_string(self, identifier, ids_list):
        resource_id_string = f"&{identifier}=".join(ids_list)
        resource_id_string = f"{identifier}=" + resource_id_string
        return resource_id_string

    def get_report(self, report_id, period, filepath=None):
        cmd = """
            {}/api/analytics.csv?dimension=dx:{}.ACTUAL_REPORTS;{}.EXPECTED_REPORTS&dimension=ou:LEVEL-5&dimension=pe:{}\
                &displayProperty=NAME&tableLayout=true&columns=dx&rows=ou;pe&showHierarchy=true\
            """.format(self.url, report_id, report_id, period)
        print(cmd)
        auth = HTTPBasicAuth(self.username, self.password)
        response = requests.get(cmd, auth=auth)
        dset = pd.DataFrame(pd.read_csv(StringIO(response.text)))
        print(dset)
        if filepath is None:
            return dset
        else:
            dset.to_csv(filepath, index=False)
        return None

    def get(self, datasetID, startDate, endDate, rename=True, filename=None, orgUnit=None):
        data = []
        writeHeader = True
        if orgUnit == None:
            orgUnit = self.get_facilities()
        else:
            orgUnit = self.to_list(orgUnit)
        datasetID = self.get_resourceID_string("dataSet", self.to_list(datasetID))
        startDate = self.format_date(startDate)
        endDate = self.format_date(endDate)

        pool = ThreadPool(multiprocessing.cpu_count())
        for i in range(0, len(orgUnit), 50):
            org_units_string = self.get_resourceID_string(
                "orgUnit", orgUnit[i: i + 50]
            )

            cmd = """ curl -k "{}/dataValueSets?{}&{}&startDate={}&endDate={}" -H "Accept:application/json" -u {}:{}
            """.format(
                self.url,
                datasetID,
                org_units_string,
                startDate,
                endDate,
                self.username,
                self.password,
            )

            pool.apply_async(self.__run, args=(cmd,), callback=self.log_result)

        pool.close()
        pool.join()

        # TOD DO: move this part to threading
        for i in self.log_list:
            print(i)
            if len(i) == 0:
                continue

            else:
                df = pd.DataFrame(json.loads(i).get("dataValues"))
                if filename != None:
                    if rename is False:
                        if writeHeader is True:
                            df.to_csv(filename, header=writeHeader)
                            writeHeader = False
                        else:
                            df.to_csv(filename, mode="a", header=writeHeader)
                    else:
                        auth = HTTPBasicAuth(self.username, self.password)
                        df = self.set_name_from_index(df, "dataElement", auth=auth)
                        df = self.set_name_from_index(df, "categoryOptionCombo", auth=auth)
                        df = self.set_name_from_index(df, "organisationUnit", auth=auth)

                        if writeHeader is True:
                            df.to_csv(filename, header=writeHeader)
                            writeHeader = False
                        else:
                            df.to_csv(filename, mode="a", header=writeHeader)
                    return None
                else:
                    if rename:
                        auth = HTTPBasicAuth(self.username, self.password)
                        df = self.set_name_from_index(df, "dataElement", auth=auth)
                        df = self.set_name_from_index(df, "categoryOptionCombo", auth=auth)
                        df = self.set_name_from_index(df, "organisationUnit", auth=auth)
                        data.append(df)
                    else:
                        data.append(df)
            return pd.concat(data)

    def post(self, files):
        files = self.to_list(files)
        pool = ThreadPool(multiprocessing.cpu_count())

        for filename in files:
            print(filename)

            cmd = """ curl -k -H "Content-Type: application/csv" --data-binary @{} "{}/dataValueSets" -u {}:{} -v \n
            """.format(
                filename, self.url, self.username, self.password
            )
            print(cmd)
            pool.apply_async(self.__run, args=(cmd,), callback=self.log_result)

        print(self.log_list)
        pool.close()
        pool.join()

    def put(self, files):

        files = self.to_list(files)
        pool = ThreadPool(multiprocessing.cpu_count())

        for filename in files:

            cmd = """ curl -k -X PUT -H "Content-Type: application/csv" --data-binary @{} "{}/dataValueSets" -u {}:{} -v \n
            """.format(
                filename, self.url, self.username, self.password
            )

            pool.apply_async(self.__run, args=(cmd,), callback=self.log_result)

        pool.close()
        pool.join()
        print(self.log_list)

    def get_facilities(self, table="organisationUnits"):
        auth = HTTPBasicAuth(self.username, self.password)
        orgs = self.get_index_table(auth, table="organisationUnits")

        orgs = orgs["id"]
        return list(orgs)

    def get_index_table(self, auth, table):

        page_size = 100000
        response = requests.get(self.url + f"/api/{table}?pageSize={page_size}", auth=auth)
        df = pd.DataFrame(response.json().get(table))
        return df

    def set_name_from_index(self, df, column_name, fetch_index=True, auth=None):

        assert (
            auth is not None
        ), "Cannot retrieve the index tables without authentication object"
        index_table = self.get_index_table(auth, table=column_name + "s")

        if column_name == "organisationUnit":
            column_name = "orgUnit"
            index_table.rename(columns={"organisationUnit": "orgUnit"})
        if column_name == "categoryOptions":
            column_name = "categoryOptionCombo"
            index_table.rename(columns={"categoryOptions": "categoryOptionCombo"})
        df[column_name] = df[column_name].apply(
            lambda x: index_table[index_table["id"].str.contains(x)][
                "displayName"
            ].values[0]
        )
        return df

    def format_date(self, date):
        """
        Formats date to 2020-12-02 (%Y-%m-%d) format
        """
        if re.match(r"^\d{8}$", date):
            date = datetime.strptime(date, "%Y%m%d")
        elif re.match(r"^\d{1,2}/", date):
            date = datetime.strptime(date, "%m/%d/%Y")
        elif re.match(r"^[a-z]{3}", date, re.IGNORECASE):
            date = datetime.strptime(date, "%b %d %Y")
        elif re.match(r"^\d{1,2} [a-z]{3}", date, re.IGNORECASE):
            date = datetime.strptime(date, "%d %b %Y")
        else:
            date = datetime.strptime(date, "%Y-%m-%d")
        date = date.strftime("%Y-%m-%d")
        return date
