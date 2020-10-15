import pandas as pd


df = pd.read_csv("data/output/report_data.csv").set_index(
    ['id', 'facility_id', 'facility_name', 'date'])


def stack_reporting(pivot):
    '''Stack outlier corrected data'''

    stack = pivot.stack().reset_index()
    stack.rename(columns={0: 'value', 'level_4': 'dataElement'}, inplace=True)
    stack['value'] = stack['value'].astype(dtype='float64')

    return stack


print(stack_reporting(df).head())
