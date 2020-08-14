import json
from datetime import datetime


def parse_config(config_path='config/indicators.json', config_section='data'):

    with open(config_path) as f:
        ENGINE = {p['identifier']: p['value']
                  for section in json.load(f)
                  for p in section[config_section]}

    return ENGINE


def make_note(statement, start_time):
    print(statement, str(datetime.now() - start_time))


INDICATORS = parse_config()
