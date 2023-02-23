from pathlib import Path
import tomli, tomli_w

root = Path(__file__).parent.parent
config_path = (Path(__file__).parent / 'configurations.toml').relative_to(root)
config_path.touch(exist_ok=True)
db_path = (Path(__file__).parent.parent / 'db' / 'catalogue.db').relative_to(root)

# make config private later and def its getter
# def load():
with config_path.open(mode='rb') as file:
    configs = tomli.load(file)
# return tomli.load(file)
# maybe later: getting item off of cfg= cfg.get(key1, key2,...)

def write(conf:dict):
    with config_path.open(mode='wb') as file:
        tomli_w.dump(conf, file)

if not configs: # if not load():
    configs =  {
        'LAST_UPDATE': {
            'INSTRUMENTS': 0, 
            'CAPITAL_INCREASE': 0,
            'INSTRUMENT_TYPES': 0,
            'DAILY_QUOTES': 0
        },
        'PATH': {
            'CONFIG_FILE': str(config_path),
            'db': str(db_path)
        },
        'URI': {
            'DAILY_PRICES_HISTROY_TO_DATE':'http://service.tsetmc.com/tsev2/data/TseClient2.aspx?t=ClosingPrices&a={}',
            'DAILY_PRICES_LAST_N_DAYS':'http://www.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={}&Top={}&A={}',
            'TRADE_DETAILS_HISTORY':'http://cdn.tsetmc.com/api/Trade/GetTradeHistory/{}/{}/{}',
            'TRADE_DETAILS_CURRENT_DAY':'http://www.tsetmc.com/tsev2/data/TradeDetail.aspx?i={}',
            'CLIENT_TYPE_HISTORY':'http://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{}/{}',
            'SHAREHOLDER_HISTORY':'http://cdn.tsetmc.com/api/Shareholder/{}/{}'
        },
        'DB':{
            'HOST': '127.0.0.1',
            'NAME': '',
            'USERNAME': '',
            'PASSWORD': '',
            # 'HEALTH': 'STATUS@DATETIME' # later
        }
    }
    write(configs)
