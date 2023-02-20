import config 
from data_ops import (
    latest_workday, 
    update_daily_quotes, 
    update_instruments_and_capital_increase, 
    update_instrument_types,
    update_instruments_info)


cfg = config.configs 
print('Latest Workday: {}\nLast Database Update: {}'.format(latest_workday,cfg['last_update']['instrument']))

# Main.py
# Update Catalogue
if cfg['last_update']['instrument'] == latest_workday:
    print('Catalogue is already up to date.')
elif cfg['last_update']['instrument'] < latest_workday:
    update_instruments_and_capital_increase()
    if not cfg['last_update']['instrument']:
        update_instrument_types()
        print('Catalogue Initialized.')
    else:
        print('Catalogue updated.')
else:
    print('Error: Invalid Configuration.')

# Update Quotes
if cfg['last_update']['daily_quotes'] == latest_workday:
    print('Daily Quotes are already up to date.')
elif cfg['last_update']['daily_quotes'] < latest_workday:
    update_daily_quotes()
    if not cfg['last_update']['daily_quotes']:
        print('Daily timeframe Initialized.')
    else:
        print('Daily timeframe updated.')
else:
    print('Error: Invalid Configuration.')
