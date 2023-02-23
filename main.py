import config 
from data_ops import (
    latest_workday, 
    update_db,
    # update_daily_quotes, 
    # update_instruments_info_and_share_increase, 
    # update_instrument_types,
    # update_instruments_info
)


cfg = config.configs 
print('Latest Workday: {}\nLast Database Update: {}'.format(latest_workday,cfg['LAST_UPDATE']['INSTRUMENTS']))

# Main.py
# region: Update Database
update_db()
# endregion
