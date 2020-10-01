import pandas as pd
import numpy as np

# Get the long list of

USECOLS = list(range(0, 9))

DTYPES = {'Unnamed: 0': int,
          'dataElement': str,
          'period': str,
          'orgUnit': str,
          'categoryOptionCombo': str,
          'attributeOptionCombo': str,
          'value': object,
          'storedBy': str,
          'created': str,
          'lastUpdated': str}

config = pd.read_csv('config/variable_correspondance.csv')

new_files = ['new_main_2020_Jul.csv',
             'new_main_2020_Apr.csv', 'new_main_2020_Feb.csv']

old_files = ['old_main_2019_Jul.csv', 'old_main_2019_Apr.csv', 'old_main_2019_Feb.csv',
             'old_main_2018_Jul.csv', 'old_main_2018_Apr.csv', 'old_main_2018_Feb.csv']


def get_breakdown_long(config, new_file, old_file):

    month = new_file.split('_')[3]

    config_new = list(config[config['instance'] == 'new']['name'])
    config_old = list(config[config['instance'] == 'old']['name'])

    out = pd.DataFrame(columns=['breakdown', 'instance'])

    new = pd.read_csv(
        f'data/input/dhis2/processed/{new_file}', usecols=USECOLS, dtype=DTYPES)

    for x in config_new:
        out.loc[x, 'breakdown'] = np.sort(
            new[new.dataElement == x]['categoryOptionCombo'].unique())
        out.loc[x, 'instance'] = 'new'

    old = pd.read_csv(
        f'data/input/dhis2/processed/{old_file}', usecols=USECOLS, dtype=DTYPES)

    for x in config_old:
        out.loc[x, 'breakdown'] = np.sort(
            old[old.dataElement == x]['categoryOptionCombo'].unique())
        out.loc[x, 'instance'] = 'old'

    return out, month


breakdown_long, month = get_breakdown_long(config, new_files[1], old_files[1])

breakdown_long.to_csv(f'config/breakdown_full_flat_{month}.csv')


def split_breakdown(df):

    out = pd.DataFrame(columns=['instance', 'domain'])

    for i in df.index:
        b_list = df.loc[i, 'breakdown']
        num = len(b_list)
        instance = df.loc[i, 'instance']
        domain = df.loc[i, 'domain']
        for n in range(0, num):
            out.loc[b_list[n], 'instance'] = instance
            out.loc[b_list[n], 'domain'] = domain

    return out


def get_breakdown(config, old, new):

    config_new = list(config[config['instance'] == 'new']['name'])
    config_old = list(config[config['instance'] == 'old']['name'])

    new_out = pd.DataFrame(columns=['breakdown'])
    for x in config_new:
        new_out.loc[x, 'breakdown'] = list(np.sort(
            new[new.dataElement == x]['categoryOptionCombo'].unique()))

    old_out = pd.DataFrame(columns=['breakdown'])
    for x in config_old:
        old_out.loc[x, 'breakdown'] = list(np.sort(
            old[old.dataElement == x]['categoryOptionCombo'].unique()))

    new_out['instance'] = 'new'
    old_out['instance'] = 'old'

    out = pd.concat([old_out, new_out]).reset_index()

    out = pd.merge(out, config[['name', 'domain']],
                   how='left', left_on='index', right_on='name')

    out = out[['breakdown', 'instance', 'domain']]

    out_old = out[out.instance == 'old']

    out_new = out[out.instance == 'new']

    out_old_split = split_breakdown(out_old)

    out_new_split = split_breakdown(out_new)

    out_split = pd.concat([out_old_split, out_new_split])
    out_split = out_split.reset_index().drop_duplicates()

    return out_split


# new_Jul = pd.read_csv(
#     f'data/input/dhis2/processed/{new_files[0]}', usecols=USECOLS, dtype=DTYPES)

# old_Jul = pd.read_csv(
#     f'data/input/dhis2/processed/{old_files[0]}', usecols=USECOLS, dtype=DTYPES)

# breakdown = get_breakdown(
#     config, old_Jul, new_Jul)

# breakdown.to_csv('config/breakdown.csv')


# bdw = pd.read_csv('config/breakdown.csv')\
#         .drop('Unnamed: 0', axis=1)\
#         .rename(columns={'index': 'breakdown'})


# bdw_new = bdw[bdw.instance == 'new']
# bdw_old = bdw[bdw.instance == 'old']

# for n in range(0, 6):
#     bdw_new[f'old_match_{n}'] = np.nan
#     bdw_new[f'score_{n}'] = np.nan

# bdw_new['concat'] = bdw_new['domain']+"__"+bdw_new['breakdown']
# bdw_old['concat'] = bdw_old['domain']+"__"+bdw_old['breakdown']


# for i in bdw_new.index:
#     print(i)
#     domain = bdw_new.loc[i, 'domain']
#     names = list(bdw_old[bdw_old.domain == domain]['concat'])
#     suggested = process.extract(
#         bdw_new.loc[i, 'concat'], names, limit=10, scorer=fuzz.partial_ratio)
#     for n in range(0, 6):
#         try:
#             bdw_new.loc[i, f'old_match_{n}'] = suggested[n][0].split('__')[1]
#             bdw_new.loc[i, f'score_{n}'] = suggested[n][1]
#         except Exception as e:
#             print(f"{e} for {bdw_new.loc[i,'concat']}")

# bdw_new.to_csv('config/breakdown_suggested.csv')
