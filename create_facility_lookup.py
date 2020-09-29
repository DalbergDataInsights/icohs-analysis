import pandas as pd

cols = ['orgunitlevel3', 'organisationunitname', 'organisationunitcode']
district_name_dict = {'SEMBABULE': 'SSEMBABULE',
                      'MADI-OKOLLO': 'MADI OKOLLO', 'LUWEERO': 'LUWERO'}


def clean_df(df):
    df.rename(columns={'orgunitlevel3': "district", 'organisationunitid': 'id',
                       'organisationunitname': 'name'}, inplace=True)
    df['district'] = df['district'].apply(lambda x: x[:-9].upper())
    df['district'].replace(district_name_dict, inplace=True)
    df = df.groupby(['district', 'id', 'name'], as_index=False).sum()
    df = df[['district', 'id', 'name']]
    df['concat'] = df['district']+'__'+df['id']+'__'
    return df


new = clean_df(pd.read_csv(
    'data/input/dhis2/processed/new_report_2020_May.csv'))
old = clean_df(pd.read_csv(
    'data/input/dhis2/processed/old_report_2019_May.csv'))


df = pd.concat([old, new])
df = df.drop_duplicates(subset=['concat'])
df.drop(columns='concat', inplace=True)
df.rename(columns={"district": 'districtname',
                   'id': 'facilitycode', 'name': 'facilityname'}, inplace=True)

df.to_csv('data/input/static/lookup_facilities.csv')

print(df.head())
