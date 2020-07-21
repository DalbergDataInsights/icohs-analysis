import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

import argparse

ENGINE = engine.get_engine()

module_json =  '../conf/indicators.json'
with open(module_json) as f:
    for section in json.load(f):
        for p in section['credentialsEngine']:
            ENGINE[p['identifier']] = p['value']



username = ENGINE['new_DHIS2_uname']
password = ENGINE['new_DHIS2_password']
ENTRY = ENGINE['new_DHIS2_url']

auth = HTTPBasicAuth(username, password)

def get_pageSize(resource):
    """
    Given a given resource, this function returns the number of pages 

    resource: name of the resource
    """
    page_size = requests.get(
        ENTRY + f'{resource}', auth=auth).json().get('pager').get('total')
    return page_size


def get_dataElement_id(data_element_name, page_size):
    """
    Given the data element name you want to download data for,
    this function returns its id

    dataElement_name:  name of the data element to download. 
    page_size: total number of pages 
    """

    data_elements = pd.DataFrame(requests.get(
        ENTRY + f'dataElements?pageSize={page_size}', auth=auth).json().get('dataElements'))
    return data_elements[data_elements['displayName'].str.contains(data_element_name)].values[-1][0]


def get_dataset_id(data_element_id):
    """
    Given the dataElement id, get the dataset id 

    """
    print(ENTRY + f'dataElements/{data_element_id}?fields=dataSetElements')
    data_set_id = [x.get('dataSet').get('id') for x in requests.get(
        ENTRY + f'dataElements/{data_element_id}?fields=dataSetElements', auth=auth).json().get('dataSetElements')]
    return data_set_id[0]


                 


def get_organisation_groups(dataset_id='RtEYsASU7PG'):
    """
    Given the dataset id , get the organisations
    """
    org_units = [x.get('id') for x in requests.get(
        ENTRY + f'dataSets/{dataset_id}', auth=auth).json().get('organisationUnits')]
    return org_units

def processes_facility():
    orgs = get_dhis_index_table(auth, page_size=70000, table='organisationUnits')
    orgs = orgs[orgs['displayName'].str.contains('Maternity|Clinic|HC|Hospital|Medical')]['id']
    return list(orgs)


def get_elements_groups(page_size,  resource_name):

    response = requests.get(
        ENTRY + f'{resource_name}?pageSize={page_size}', auth=auth)

    df = pd.DataFrame(response.json().get(resource_name))

    return list(df['id'])


def get_resourceID_string(identifier, ids_list):

    resource_id_string = f'&{identifier}='.join(ids_list)
    return resource_id_string


def get_dhis_index_table(auth, page_size=70000, table='dataElements'):

    response = requests.get(ENTRY + f'{table}?pageSize={page_size}', auth=auth)
    df = pd.DataFrame(response.json().get(table))
    return df


def set_name_from_index(df, column_name, index_table=None, fetch_index=False, auth=None):
    if fetch_index:
        assert auth is not None, "Cannot retrieve the index tables without authentication object"
        index_table = get_dhis_index_table(
            auth, page_size=10000, table=column_name + 's')
    else:
        assert index_table is not None, 'Set fetch index or pass the index table'
    if column_name == 'organisationUnit':
        column_name = 'orgUnit'
        index_table.rename(columns={'organisationUnit': 'orgUnit'})
    if column_name == 'categoryOptions':
        column_name = 'categoryOptionCombo'
        index_table.rename(columns={'categoryOptions': 'categoryOptionCombo'})
    df[column_name] = df[column_name].apply(
        lambda x: index_table[index_table['id'].str.contains(x)]['displayName'].values[0])
    return df


if __name__ == "__main__":

    categoryOption = get_dhis_index_table(
        auth, page_size=10000, table='categoryOptionCombos')

    #dataset_id = get_dataset_id(data_element_id)
    dataset_id = 'RtEYsASU7PG'
    
    org_group_list = processes_facility()
    

    elements_groups_list = get_elements_groups(
        get_pageSize("dataElementGroups"),  "dataElementGroups")

    #org_group_string = get_resourceID_string("orgUnit", org_group_list)
    elements_groups_string = get_resourceID_string(
        "dataElementGroup", elements_groups_list)
    
    org_unit_index = get_dhis_index_table(
        auth, page_size=10000, table='organisationUnits')
    categoryOption = get_dhis_index_table(
        auth, page_size=10000, table='categoryOptionCombos')

    # set start date and end date
    startDate = '2020-01-01'
    endDate = '2020-07-05'

    
    dataset_ids = ['RtEYsASU7PG']
    dataset_ids = get_resourceID_string("dataSet", dataset_ids)


    batchsize = 50
    i = 0
    writeHeader = True
    
    for i in range(0, len(org_group_list), batchsize):
        org_units_string = get_resourceID_string(
            'orgUnit', org_group_list[i:i+batchsize])
        df = pd.DataFrame(requests.get(
            f'https://hmis.health.go.ug/api/dataValueSets?{dataset_ids}&{org_units_string}&startDate={startDate}&endDate={endDate}&{elements_groups_string}', auth=auth).json().get('dataValues'))
        print(df.shape)
        df = set_name_from_index(
            df, 'dataElement', auth=auth, fetch_index=True)
        #df = set_name_from_index(df, 'orgUnit', index_table=org_unit_index)
        df = set_name_from_index(
            df, 'categoryOptionCombo', index_table=categoryOption)
        print(i)
        if writeHeader is True:
            df.to_csv("new_instance.csv", mode='a', header=True)
            writeHeader = False
        else:
             df.to_csv("old_instance.csv", mode='a', header=False)

        i=i+1
    
    
    
    
    
    
    
    
