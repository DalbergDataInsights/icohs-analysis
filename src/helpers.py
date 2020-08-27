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
