import logging
from datetime import datetime
import config 
from data_ops import (
    latest_workday, 
    update_db,
    # update_daily_quotes, 
    # update_instruments_info_and_share_increase, 
    # update_instrument_types,
    # update_instruments_info
)

logging.info(' ===================== Commencing Session... ====================== ')


cfg = config.configs 
print('\nLatest Workday: {}\nLast Database Update: {}\n'.format(
    datetime.strptime(str(latest_workday), '%Y%m%d').date(), 
    datetime.strptime(str(cfg['LAST_UPDATE']['INSTRUMENTS']), '%Y%m%d').date()))

# Main.py
# region: Update Database
update_db()
# endregion

logging.info(' ===================== End Of Session ====================== ')
