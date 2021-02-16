import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from src.api import api_pull


facilitiesPath = 'all_facilities.pid'
ENGINE = api_pull.get_engine("config/cred.json", "credentialsEngine")
FacilitiesENGINE = api_pull.get_engine(
    "config/api_config.json", "facilities_id")

new_username = ENGINE['new_DHIS2_uname']
new_password = ENGINE['new_DHIS2_password']
new_ENTRY = ENGINE['new_DHIS2_url']

old_username = ENGINE['old_DHIS2_uname']
old_password = ENGINE['old_DHIS2_password']
old_ENTRY = ENGINE['old_DHIS2_url']

new_facilities = FacilitiesENGINE['newFacilitiesId']
old_facilities = FacilitiesENGINE['oldFacilitiesId']

auth = HTTPBasicAuth(new_username, new_password)


def processes_facility(auth, url, id, table="organisationUnitGroups"):
    page_size = 100000  # get_pageSize(table, auth, url)
    response = requests.get(
        url + f'{table}/{id}?pageSize={page_size}', auth=auth)
    orgs = pd.DataFrame(response.json().get("organisationUnits"))
    orgs = orgs['id']

    num_fac = len(orgs)
    total_fac = num_fac - (num_fac % 50)+50
    open(facilitiesPath, 'w+').write(str(total_fac))


processes_facility(auth, new_ENTRY, new_facilities)
