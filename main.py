from datetime import datetime
import config 
from data_ops.utils import logger_app
from data_ops import (
    latest_workday, 
    update_db,
    # update_daily_quotes, 
    # update_instruments_info_and_share_increase, 
    # update_instrument_types,
    # update_instruments_info
)

logger_app.info(' ===================== Commencing Session... ====================== ')


cfg = config.configs 

print('\nLatest Workday: {}\nLast Database Update: {}\n'.format(
    datetime.strptime(str(latest_workday), '%Y%m%d').strftime('%Y-%m-%d (%A)'), 
    cfg['LAST_UPDATE']['INSTRUMENTS']))

# Main.py
# region: Update Database
update_db(async_=True)
# endregion

logger_app.info(' ===================== End Of Session ====================== ')
