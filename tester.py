import pandas as pd
from src.helpers import INDICATORS

VAR_CORR = pd.read_csv(INDICATORS['var_correspondence_data'])


def clean_pop_to_temp(pop_path, pop_perc_path):

    pop = pd.read_csv(pop_path)

    district_name_dict = {'SEMBABULE': 'SSEMBABULE',
                          'MADI-OKOLLO': 'MADI OKOLLO', 'LUWEERO': 'LUWERO'}
    pop['District'] = pop['District'].apply(lambda x: x.upper())
    pop['District'].replace(district_name_dict, inplace=True)

    pop['Age'] = pop['Single Years'].apply(
        lambda x: ' '.join(x.split(' ')[:1]))
    pop['Age'].replace({'80+': '80'}, inplace=True)
    pop['Age'] = pop['Age'].astype('int')

    pop.drop(['Single Years', 'Year2', 'FY'], axis=1, inplace=True)

    pop = pop.groupby(['District', 'Year'], as_index=False).sum()

    pop = pop[['District', 'Year', 'Male', 'Female', 'Total']]

    perc = pd.read_csv(pop_perc_path).set_index('metric')

    for x in perc.index:
        pop[x] = pop['Total']*(perc.loc[x, 'percentage']/100)

    pop.to_csv('data/temp/pop.csv', index=False, header=False)


# INDICATORS['pop']

clean_pop_to_temp(INDICATORS['pop'], INDICATORS['pop_perc'])
