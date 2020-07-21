import csv
import psycopg2
import pandas as pd
import json 

ENGINE = engine.get_engine()

module_json =  '../conf/indicators.json'
with open(module_json) as f:
    for section in json.load(f):
        for p in section['data']:
            ENGINE[p['identifier']] = p['value']

## TODO Move this to the data processing file file
district_df = pd.read_csv(ENGINE['districts database mapping'])
facility_df = pd.read_csv(ENGINE['facility database mapping'])
data_df = pd.read_csv(ENGINE['Data from the api'])
indicator_df = pd.read_csv(ENGINE['indicators database mapping'])

            
district_name_dict = {'Sembabule':'SSEMBABULE','Madi-Okollo':'MADI OKOLLO'}
district_df['districtname'].replace(district_name_dict,inplace=True)

#tranform the data 
df3 = (data_df.set_index(["district","organisationunitid","year","month","Unnamed: 4"])
         .stack()
         .reset_index(name='Value')
         .rename(columns={'level_5':'Indicator','Unnamed: 4':'report'}))


########### Preprocess data for database foriegn key mapping ##########

#Map Disrtict Id to Disrtict name for repository table foreign key
district_df['districtname']=district_df['districtname'].str.upper()
dist_dict = dict(zip(district_df.districtname, district_df.districtcode))
df3['districtcode']=df3['district'].map(dist_dict)

#Map facility Id to facility name for repository table foreign key
facility_dict = dict(zip(facility_df.facilityname, facility_df.facilitycode))
df3['facilitycode']=df3['organisationunitid'].map(facility_dict)

#Map Indicator Id to Indicator name for repository table foreign key
indicator_dict = dict(zip(indicator_df.indicatorname, indicator_df.indicatorcode))
df3['indicatorcode']=df3['Indicator'].map(indicator_dict)

final_df = df3[['districtcode','facilitycode', 'indicatorcode','Value',	'year','month' ,'report']]

final_df.pivot_export.to_csv('data/aggregate_codes.csv')
data_file = 'data/aggregate_codes.csv'
param_dic = "host=154.72.194.185 dbname=coc_db user=ddi_unicef password=eat1@Gabfest"

############### write data to the database ############################
conn = psycopg2.connect(param_dic)
cur = conn.cursor()

with open(data_file, 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
        "INSERT INTO st_repository(districtcode,facilitycode,indicatorcode, value,year,month,report) VALUES (%s, %s, %s,%s, %s,%s,%s)",
        row)
conn.commit()
