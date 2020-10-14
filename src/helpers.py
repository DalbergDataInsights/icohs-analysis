import json
from datetime import datetime


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


def format_date(date):
    dates = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    assert len(dates) == 12
    year, month = date.split('-')
    month_order = str(dates.index(month)+1)
    month_order = '0' + month_order if len(month_order) == 1 else month_order
    return year + '-' + month_order + '-01'
