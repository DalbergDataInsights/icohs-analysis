import pandas as pd

l1 = ['a', 'b', 'c']
l2 = ['a']

df = pd.DataFrame(columns=l1)

c = 1

if f'{c}' not in df.columns:
    print('not here')
elif f'{c}' in df.columns:
    print('here')
else:
    print('error')
