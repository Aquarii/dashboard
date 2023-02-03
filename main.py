import config 
from data_ops import latest_workday, update_table


cfg = config.configs 
print('Latest Workday: {}\nLast Database Update: {}'.format(latest_workday,cfg['last_updates']['instrument']))

# Main.py
if not cfg['last_updates']['instrument']:
    update_table('instrument_and_capital_increase')
    update_table('instrument_type')
    print('Database Initialized.')
elif cfg['last_updates']['instrument'] == latest_workday:
    print('Database is already up to date.')
elif cfg['last_updates']['instrument'] < latest_workday:
    update_table('instrument_and_capital_increase')
    print('Database updated.')
else:
    print('Error: Invalid Configuration.')

