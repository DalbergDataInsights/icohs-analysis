import json
from datetime import datetime
import pandas as pd


def parse_config(config_path='config/indicators.json', config_section=['input', 'static', 'to_classify', 'output']):

    with open(config_path) as f:

        ENGINE = {p['identifier']: p['value']
                  for section in json.load(f)
                  for x in config_section
                  for p in section[x]}

    return ENGINE


def parse_config1(config_path='config/indicators.json', config_section=['input', 'static', 'to_classify', 'output']):

    with open(config_path) as f:
        for section in json.load(f):
            for x in config_section:
                print(section[x])

        ENGINE = {p['identifier']: p['value']
                  for section in json.load(f)
                  for p in section[x]}

    return ENGINE


def make_note(statement, start_time):
    print(statement, str(datetime.now() - start_time))


INDICATORS = parse_config()


def get_unique_indics(var_corr, excl_domain=None):

    indics = []

    if excl_domain is not None:
        var_corr = var_corr[var_corr['domain'] != 'REPORT']

    var_corr['breakdown'] = var_corr['breakdown'].astype(str)

    for i in var_corr.index:
        suf = list(var_corr.loc[i, 'breakdown'].split(","))
        iden = var_corr.loc[i, 'identifier']
        if suf[0] != 'nan':
            for s in suf:
                y = iden + "__" + s
                indics.append(y)
        else:
            y = iden
            indics.append(y)

    out = list(set(indics))

    return out
