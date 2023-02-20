from pathlib import Path
import tomli, tomli_w


config_path = Path(__file__).parent / 'configurations.toml'
config_path.touch(exist_ok=True)
db_path = Path(__file__).parent.parent / 'db' / 'catalogue.db'

# make config private later and def its getter
# def load():
with config_path.open(mode='rb') as file:
    configs = tomli.load(file)
# return tomli.load(file)

def write(conf:dict):
    with config_path.open(mode='wb') as file:
        tomli_w.dump(conf, file)

if not configs: # if not load():
    configs =  {'last_update': {
                    'instrument': 0, 
                    'capital_increase': 0,
                    'instrument_type': 0,
                    'daily_quotes': 0},
                'path': {
                    'config_file': str(config_path),
                    'db': str(db_path)},
                'db_health': {
                    'last_check_time':'datetime -not yet implemented',
                    'last_check_status':'ok/faulty -not yet implemented'},
                'URI': {
                    'DAILY_PRICES_HISTROY_TO_DATE':'http://service.tsetmc.com/tsev2/data/TseClient2.aspx?t=ClosingPrices&a={}',
                    'DAILY_PRICES_LAST_N_DAYS':'http://www.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={}&Top={}&A={}',
                    'TRADE_DETAILS_HISTORY':'http://cdn.tsetmc.com/api/Trade/GetTradeHistory/{}/{}/{}',
                    'TRADE_DETAILS_CURRENT_DAY':'http://www.tsetmc.com/tsev2/data/TradeDetail.aspx?i={}',
                    'CLIENT_TYPE_HISTORY':'http://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{}/{}',
                    'SHAREHOLDER_HISTORY':'http://cdn.tsetmc.com/api/Shareholder/{}/{}'}}
    write(configs)
