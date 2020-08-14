from src.db import adpter as db 

import pandas as pd
import numpy as np 


#db.pg_write_lookup("data/orgUnit.csv", 'facility')
#db.pg_write_lookup("data/districts.csv", 'district')
#db.pg_write_lookup("data/indicators.csv", 'indicator')


clean_df = pd.read_csv('data/test.csv')

###################################
##### ADD to the clean.py #####
####################################

clean_df['value'] = clean_df['value'].astype(int)

#read lookupdata and map the foreign keys
facility = db.pg_read_lookup("facility")
indicator = db.pg_read_lookup("indicator")
district = db.pg_read_lookup("district")

clean_df['districts']= clean_df['districts'].map(district)
clean_df['dataElement']= clean_df['dataElement'].map(indicator)
clean_df['orgUnit']= clean_df['orgUnit'].map(facility)

# order of clean csv file [district, facility, indicator, year, month , value]
clean_df.to_csv('data/Test.csv', index=False,header=False)


#####################################################################
############### copy clean csv to the repository table ###############
######################################################################
db.pg_write_table("data/Test.csv","repository")  #### save the entire file


#delete for a given month and year and update with the new one.
#db.pg_update_write( 2020, 3, "data/march.csv", "repository")   ### save by month and year 

###############################################################
################ Read all data in the clean df  ################
# ############################################################## 

#print(db.pg_read_table().head())  # read all 
print(db.pg_read_table_by_indicator('malaria_tests').head()) # read by indicator


######################################################
############### Save the four csv final outputs ###############
##########################################

#db.pg_final_table('data/facility_data_no_outliers_iqr.csv', "no_outliers_iqr")







