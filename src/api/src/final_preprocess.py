import pandas as pd 
import sys
import glob
import json
import os
from pathlib import Path
import engine
import numpy as np
import scipy
import datetime
from scipy import stats

ENGINE = engine.get_engine()
ENGINE_rename = engine.get_engine()


module_json =  '../conf/indicators.json'
with open(module_json) as f:
    for section in json.load(f):
        for p in section['new_instance']:
            ENGINE[p['identifier']] = p['value']
        for p in section['data']:
            ENGINE[p['identifier']] = p['value']
        for p in section['old_indicators']:
            ENGINE[p['identifier']] = p['value']
            

# dictionary used to rename old indicators to new
with open(module_json) as f:
    for section in json.load(f):
        for p in section['old_indicators']:
            ENGINE_rename[p['value']] = p['name']

#Data paths           
new_dhis_path = ENGINE['new_instance_data'] 
district_facility_path = ENGINE['facility_dist_data'] #facility district mapping
old_dhis_path = ENGINE['old_instance_data'] 

new_dhis_df = pd.read_csv(new_dhis_path)
dist_facility = pd.read_csv(district_facility_path)
old_dhis_df = pd.read_csv(new_dhis_path)

new_dhis_df['value']= pd.to_numeric(new_dhis_df['value'],errors='coerce')
old_dhis_df['value']= pd.to_numeric(old_dhis_df['value'],errors='coerce')


# Dictionary for mapping districts to facilities
dist_facility = dist_facility.drop_duplicates(['district','organisationunitid'],keep= 'last')
district_faility_map = dict(zip(dist_facility.organisationunitid, dist_facility.district)) 
      
    
def get_new_data(data):  
    # TODO: loop through the indetifiers to get the indicator names 
    mnch_list = [ENGINE['ANC1'],ENGINE['ANC4'],ENGINE['total_deliveries'],ENGINE['live_deliveries'],
                 ENGINE['still_births'],ENGINE['Macerated_still_births'], ENGINE['PNC'],ENGINE['MAT'],
                 ENGINE['1week_new_born_deaths'],ENGINE['1st_HIV_test_at_labor'],ENGINE['Total_Tested_for_HIV'],ENGINE['Total_New_HIV+'],
                 ENGINE['Total_Linked_to_HIV_Care'],ENGINE['new BCG'],ENGINE['PCV1'], ENGINE['PCV3'],ENGINE['MR1'],
                 ENGINE['DPT-HepB+Hib1'],
                 ENGINE['DPT-HepB+Hib3'],ENGINE['HPV2-Dose2'],ENGINE['HPV1-Dose1'], ENGINE['Macerated_low_birth_weight'], ENGINE['Fresh_low_birth_weight'],
                 ENGINE['Live_births_low_birth_weight'], ENGINE['Vit_A_supplement1'], ENGINE['Vit_A_supplement2'],ENGINE['Women_re-tested_for_HIV_in_labour'],
                 ENGINE['Women tested for HIV+ve in labour 1st time'],ENGINE['Women re-tested for HIV+ve in labour'],ENGINE['Women tested for HIV in labour'],
                 ENGINE['Td1-Dose1'], ENGINE['Td2-Dose2'],ENGINE['Td3-Dose3'],ENGINE['Td4-Dose4'],ENGINE['Td5-Dose5'],
                 ENGINE['ANC newly tested'],ENGINE['ANC newly tested+ve'],ENGINE['ANC initiated on ART for eMTCT'],ENGINE['MAT initiated on ART'],
                 ENGINE['PNC tested'],ENGINE['PNC tested+ve'],ENGINE['PNC initiated ART'],
                 ENGINE['Not tested cases treated'],ENGINE['RDT Negative Cases Treated'],ENGINE['RDT Positive Cases Treated'], 
                 ENGINE['Microscopy Negative Cases Treated'],ENGINE['Microscopy Positive Cases Treated'],ENGINE['Cases Tested with RDT'],
                 ENGINE['Cases Tested with Microscopy'],ENGINE['New using MUAC'],ENGINE['SAM admissions in ITC'], 
                 ENGINE['New attendance'],ENGINE['Re-attendance']]
 
    new_df = data[data["dataElement"].isin(mnch_list)]
  
    new_df = pd.DataFrame(new_df.groupby(['dataElement','orgUnit','period'])['value'].sum()).reset_index()
    new_df = new_df[['period', 'orgUnit', 'value', 'dataElement']]
    
    return new_df

def get_old_data(data):
    # TODO: loop through the indetifiers to get the indicator names 
    indc_list = [ENGINE['old_ANC1'],ENGINE['old_ANC4'],ENGINE['low_birth_weight'],ENGINE['old_Admissions'],
                 ENGINE['old_1week_newborn_deaths'],ENGINE['old_total_deliveries'], ENGINE['old_Fresh_Still_births'],ENGINE['old_Macerated_still_births'],
                 ENGINE['old_Live_Births'],ENGINE['old_Postnatal_Attendances'],
                 ENGINE['old_BCG'],ENGINE['old_DPT1'],
                 ENGINE['old_DPT3'],ENGINE['old_PCV1'], ENGINE['old_PCV3'],ENGINE['old_Vit_A_Suplement1'],ENGINE['old_Vit_A_Suplement2'],
                 ENGINE['old_Vit_A_HPV1-Dose1'],ENGINE['old_Vit_A_HPV1-Dose2'],ENGINE['old_indiv_tested'], 
                 ENGINE['old_indiv_tested_ve'], ENGINE['old_linked_to_care'],
                 ENGINE['old_pregnant_newly_tested'],
                 ENGINE['old_pregnant_newly_tested_ve'], 
                 ENGINE['old_pregnant_initiated_on ART_for_EMTCT'],ENGINE['old_pregnant_tested_in_labour'],
                 ENGINE['old_pregnant_tested_ve_in_labour'],ENGINE['old_pregnant_retested_in_labour'],
                 ENGINE['old_women_initiated_ART_maternity'],
                 ENGINE['old_Breastfeeding_mothers_tested'],ENGINE['old_Breastfeeding_mothers_newly_tested_ve'],
                 ENGINE['old_Breastfeeding_mothers_newly_retested_ve'],
                 ENGINE['old_women_initiating_ART_PNC'],
                 ENGINE['old_Tetanus_Immunization_Dose1'],ENGINE['old_Tetanus_Immunization_Dose2'],
                 ENGINE['old_Tetanus_Immunization_Dose3'],ENGINE['old_Tetanus_Immunization_Dose4'],ENGINE['old_Tetanus_Immunization_Dose5'],
                 ENGINE['old_Measles'],
                 ENGINE['OPD New TB cases_Bacteriologically'],ENGINE['OPD New TB cases_Clinically'],
                 ENGINE['OPD New TB cases_(EPTB)'],ENGINE['Acute_Malnutrition_With_Oedema'],
                 ENGINE['Acute_Malnutrition_Without_Oedema'],ENGINE['OPD New Attendance'],
                 ENGINE['OPD Re-Attendance']]
   
    old_df = data[data["dataElement"].isin(indc_list)]
  
    old_df = pd.DataFrame(old_df.groupby(['dataElement','orgUnit','period'])['value'].sum()).reset_index()
    old_df = old_df[['period', 'orgUnit', 'value', 'dataElement']]
    
    return old_df 


def compute_indicators(indicator_list, df, indic_rename, indic_type):
    
    df_new = df[df['dataElement'].isin(indicator_list)]
    df_new = pd.DataFrame(df_new.groupby(['period','orgUnit'])['value'].sum()).reset_index()
    df_new['dataElement']=indic_rename
    df = pd.concat([df,df_new])
    df.reset_index(drop=True, inplace=True)
    if indic_type !="lbw":
        df = df.drop(df.index[df['dataElement'].isin(indicator_list)])
        
    return df 


def process_data(df):
    
    pd.to_datetime(df['period'])
    df['datetime']= pd.to_datetime(df['period'].astype(str), format='%Y%m')
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    map_day={1:'Jan',2:'Feb',3:'Mar',4:'Apr',
                5:'May',6:'Jun',7:'Jul',8:'Aug',
                9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
  
    df['month']= df['month'].map(map_day)
    df = df.drop(['datetime','period'],axis=1)
    df['year'] = df['year'].apply(str)
    return df 


def get_all_data(hosp_df):
    #Create a dataframe for all health facilities including those that did not report
    data_df = pd.DataFrame(hosp_df['id']).reset_index(drop=True)
    data_df.set_index('id',drop=True, inplace=True)
    columns = [("Test","test","test1")]
    df1 =pd.DataFrame(np.nan, index=list(range(len(hosp_df['id']))), columns=['A'])
    df1['orgUnit'] = hosp_df['id']
    df1.set_index('orgUnit',drop=True, inplace=True)
    df1.columns=pd.MultiIndex.from_tuples(columns)
    return df1

def pack(df3, columns, hosp):
    
    df3 =pd.DataFrame(np.nan, index=list(range(len(hosp['id']))), columns=['A'])
    df3.columns=pd.MultiIndex.from_tuples(columns)
    return df3

def pivot_stack(df):
    pivot_outliers=df.copy().pivot_table(index=['districts', 'orgUnit', 'dataElement'], columns=['year','month' ]) #,dropna=False)
    pivot_outliers.rename(columns={'value':'with_outiers'},level=0,inplace=True)
    pivot_outliers.columns.rename('type', level=0, inplace=True)
    pivot_outliers.dropna(how='all',axis=0,inplace=True) # looks like there is no all na line to drop
    return pivot_outliers


def replace_outliers(pivot_outliers,cutoff):#df
    #Replace ouliers using a std deviation method
    pivot_no_outliers=pd.DataFrame(columns=pivot_outliers.columns,index=pivot_outliers.index)
    pivot_no_outliers.rename(columns={'with_outiers':'without_outliers'},level=0,inplace=True)
    
    for x in pivot_outliers.index: # to exclude
        values = pivot_outliers.loc[x,:].values
        if np.nanstd(values)!=0 and np.isnan(values).sum()!=len(values):
            zscore = abs(stats.zscore(values,nan_policy='omit'))
            new_values = np.where(zscore>cutoff,np.nanmedian(values),values)

        else:
            new_values = values

        pivot_no_outliers.iloc[pivot_outliers.index.get_loc(x),:] = new_values.astype('float')

    return pivot_no_outliers


def replace_outliers_iqr(pivot_outliers,k):#df
    
    pivot_no_outliers=pd.DataFrame(columns=pivot_outliers.columns,index=pivot_outliers.index)
    pivot_no_outliers.rename(columns={'with_outiers':'without_outliers'},level=0,inplace=True)
    
    for x in pivot_outliers.index:
        values = pivot_outliers.loc[x,:].values
        if np.nanstd(values)!=0 and np.isnan(values).sum()!=len(values):
            Q1 = np.nanquantile(values,0.25)
            Q3 = np.nanquantile(values,0.75)
            IQR = Q3 - Q1
            LB = Q1 - k*IQR
            UB = Q3 + k*IQR
            new_values = np.where((values<LB)|(values>UB),np.nanmedian(values),values)

        else:
            new_values = values

        pivot_no_outliers.iloc[pivot_outliers.index.get_loc(x),:] = new_values.astype('float')

    return pivot_no_outliers


def pivot_stack(pivot):
    #stack outlier corrected data
    stack = pivot.stack(level=[0,1,2],dropna=False).reset_index()
    stack.rename(columns={0:'value'},inplace=True)
    stack.drop('type',axis=1,inplace=True)
    stack['value']=stack['value'].astype(dtype='float64')
    return stack

def export_to_csv(stack_t_noreport, stack_t_noout,stack_t_noout_iqr):
    
    fac_stack_final = pd.merge(stack_t_noreport,stack_t_noout,how='left',
                           left_on=['district', 'organisationunitid', 'year', 'indic', 'month'],
                           right_on=['district', 'organisationunitid', 'year', 'indic', 'month']).rename(columns={'value_x':'value_out','value_y':'value_noout'})

    fac_stack_final = pd.merge(fac_stack_final,stack_t_noout_iqr,how='left',
                           left_on=['district', 'organisationunitid', 'year', 'indic', 'month'],
                           right_on=['district', 'organisationunitid', 'year', 'indic', 'month']).rename(columns={'value':'value_noout_iqr'})

    # Make a note of whcih facilities reported which didt 
    fac_stack_final['reported'] = (fac_stack_final['value_out']>0).astype('int')
    # Add in the reporting rate data, that did not go through theoutlier precocedure
    stack_t_report.rename(columns={'value':'reported'},inplace=True)
    stack_t_report.set_index(['district','organisationunitid','year' ,'indic','month'],inplace=True,drop=True)
    #stack_t_report = stack_t_report.loc[~stack_t_report.index.duplicated(keep='first')] # Note here a weird issue of duplicates 
    stack_t_report.reset_index(inplace=True)

    #Puts it all together 
    fac_stack_final=pd.concat([stack_t_report,fac_stack_final],ignore_index=True)

    # Create a pivot
    fac_pivot_final=fac_stack_final.pivot_table(index=['district','organisationunitid','year','month'], columns=['indic'],aggfunc=max)
    fac_pivot_final=fac_pivot_final.stack(level=[0])
    final_df.pivot_export.to_csv('data/corrected_data.csv')
    
    
def main(new_dhis_df,old_dhis_df):

    ############ New instance ########################
    new_dhis_df = get_new_data(new_dhis_df)
    
    lbw_list = [ENGINE['Macerated_low_birth_weight'],ENGINE['Fresh_low_birth_weight'],ENGINE['Live_births_low_birth_weight']]      
    women_hiv_tested_list = [ENGINE['Women_re-tested_for_HIV_in_labour'],ENGINE['Women tested for HIV in labour']]
    women_hivVe_tested_list = [ENGINE['Women re-tested for HIV+ve in labour'],ENGINE['Women tested for HIV+ve in labour 1st time']]
    ttd_5_list = [ENGINE['Td4-Dose4'],ENGINE['Td5-Dose5']]

   # TODO: get rid of this repetition
    new_dhis_df = compute_indicators(lbw_list, new_dhis_df, "Babies Born with low birth weight (<2.5Kgs)",'lbw')
    new_dhis_df = compute_indicators(women_hiv_tested_list, new_dhis_df, "pregnant women tested for HIV in labor", "hiv")
    new_dhis_df = compute_indicators(women_hivVe_tested_list, new_dhis_df, "pregnant women tested HIV+ve in labor","hiv")
    new_dhis_df = compute_indicators(ttd_5_list, new_dhis_df, "105-TD04_5. Td4_5-Dose 4_5","hiv")
    new_dhis_df = process_data(new_dhis_df)
    
    
    ############ Old instance ########################
    old_dhis_df = get_old_data(old_dhis_df)
    #compute old indicators
    compute_dict = {"td4_5":[ENGINE['old_Tetanus_Immunization_Dose4'],ENGINE['old_Tetanus_Immunization_Dose5']],
                     "OPD attendance":[ENGINE['OPD New Attendance'],ENGINE['OPD Re-Attendance']],
                     "pregnant women tested for HIV in labor":[ENGINE['old_pregnant_newly_tested'],ENGINE['old_pregnant_retested_in_labour']],
                     "pregnant women tested HIV+ve in labor":[ENGINE['old_pregnant_newly_tested_ve'],ENGINE['old_pregnant_tested_ve_in_labour']],
                     "breastfeeding mothers tested HIV +ve in PN":[ENGINE['old_Breastfeeding_mothers_newly_tested_ve'],
                    ENGINE['old_Breastfeeding_mothers_newly_retested_ve']],"105-TP04. Total New and relapse TB cases registered in TB treatment unit":
                    [ENGINE['OPD New TB cases_Bacteriologically'],ENGINE['OPD New TB cases_Clinically'], ENGINE['OPD New TB cases_(EPTB)']],
                     "Number of doses of vitamin A distributed":[ENGINE['old_Vit_A_Suplement1'],ENGINE['old_Vit_A_Suplement2']],
                     "Number of SAM admissions":[ENGINE['Acute_Malnutrition_With_Oedema'],ENGINE['Acute_Malnutrition_Without_Oedema']]}

    for key, value in compute_dict.items():
        old_dhis_df = compute_indicators(value, old_dhis_df, key,'hiv')
    
    old_dhis_df =old_dhis_df.replace({"dataElement": ENGINE_rename})# reanme old indicators
    old_dhis_df = process_data(old_dhis_df)
    
    #concatenate old dhis2 data with new dhis2 data
    combined_df=pd.concat([old_dhis_df,new_dhis_df])
    combined_df.reset_index(drop=True,inplace=True)
   
    # Include facilities that that not reported.
    facilities_df = pd.read_csv(ENGINE['facility_data'])
    df3 = get_all_data(facilities_df)

    for i in combined_df['year'].unique():
        for j in combined_df['month'].unique():
            for m in combined_df['dataElement'].unique():
                columns = [(i,j,m)] 
            
                df_months = pack(df3, columns,facilities_df)
            
                df3 = pd.merge(df3, df_months, how='left',left_index=True,right_index=True)
     
    
    df3.drop('Test',axis=1,inplace=True,level=0)
    df3_unstuck = df3.unstack().reset_index()

    df3_unstuck.drop([0], axis=1, inplace=True)
    df3_unstuck.columns= ["year","month","dataElement","orgUnit"]
    
    full_data = df3_unstuck.merge(combined_df, how='left', on=['dataElement','orgUnit','year','month'])
    full_data['districts']= full_data['orgUnit'].map(district_faility_map)
    return full_data

if __name__ == "__main__":
    data_df = main(new_dhis_df,old_dhis_df)
    #separe reports and non reports indicators
    report_indics=['HMIS 105:01 - OPD Monthly Report (Attendance, Referrals, Conditions,TB, Nutrition) Actual reports 1. National',
               'HMIS 105:01 - OPD Monthly Report (Attendance, Referrals, Conditions,TB, Nutrition) Expected reports 1. National']
    data_df_noreport=stack_t[~stack_t['dataElement'].isin(report_indics)].copy()
    data_df_report=stack_t[stack_t['dataElement'].isin(report_indics)].copy()
    
    #outlier computattion on non report data 
    pivot_outliers = pivot_outliers=pivot_stack(data_df_noreport)
    pivot_no_outliers = replace_outliers(pivot_outliers,cutoff=3)
    pivot_no_outliers_iqr = replace_outliers_iqr(pivot_outliers,k=3)
    
    stack_t_noout=pivot_stack(pivot_no_outliers)
    stack_t_noout_iqr=pivot_stack(pivot_no_outliers_iqr)
    export_to_csv(stack_t_noreport, stack_t_noout,stack_t_noout_iqr)
    
    print(data_df.head())
