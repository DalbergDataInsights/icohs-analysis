cols = ['date', 'DPT3 coverage (all) -- weight',
        'DPT3 coverage (all) -- weighted_ratio']

check = outlier[cols].groupby('date').sum()

check['actual ratio'] = check[cols[2]]/check[cols[1]]

print(check.loc[['2019-10-01', '2019-11-01', '2019-12-01']])
print(check.loc[['2020-06-01', '2020-07-01', '2020-08-01']])
