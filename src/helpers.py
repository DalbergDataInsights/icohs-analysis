import json
from datetime import datetime

INDICATORS = parse_config()

def parse_config(config_path='config/indicators.json', config_section='data'):

    for path in config_path:
        with open(path) as f:
            ENGINE =   {p['identifier']: p['value']
                       for section in json.load(f)
                       for p in section[config_section]}

    return ENGINE


def make_note(statement, start_time):
    print(statement, str(datetime.now() - start_time))

