import json

def get_engine():
    default_engine_json = '../config/engine.json'
    with open(default_engine_json) as f:
        ENGINE = dict((p['identifier'], p['value']) for section in json.load(f) 
                                                    for p in section['engineParameters'])
        return ENGINE