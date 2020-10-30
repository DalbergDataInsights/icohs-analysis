from src.helpers import INDICATORS
import json
import pandas as pd

with open(INDICATORS['viz_config'], 'r') as f:
    CONFIG_DF = pd.read_json(f)

print('Done')
