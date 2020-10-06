import pandas as pd
from src.helpers import INDICATORS

VAR_CORR = pd.read_csv(INDICATORS['var_correspondence_data'])


def create_indic_map(path):

    indics = []

    VAR_CORR['breakdown'] = VAR_CORR['breakdown'].astype(str)

    for i in VAR_CORR.index:
        suf = list(VAR_CORR.loc[i, 'breakdown'].split(","))
        iden = VAR_CORR.loc[i, 'identifier']
        if suf[0] != 'nan':
            for s in suf:
                y = iden + "__" + s
                indics.append(y)
        else:
            y = iden
            indics.append(y)

    df = pd.DataFrame(set(indics))

    return df


test = create_indic_map(INDICATORS['indicators_map'])

print(test)
