from pathlib import Path
import tomli, tomli_w


config_path = Path(__file__).parent / 'configurations.toml'
config_path.touch(exist_ok=True)
db_path = Path(__file__).parent.parent / 'db' / 'catalogue.db'

with config_path.open(mode='rb') as file:
    configs = tomli.load(file)

def write(conf:dict):
    with config_path.open(mode='wb') as file:
        tomli_w.dump(conf, file)

if not configs:
    configs =  {'last_updates': {'instrument': 0, 
                                 'capital_increase': 0,
                                 'instrument_type': 0},
                'configs': {'config_file_path': str(config_path),
                            'db_path': str(db_path)},
                'db_health': {'last_check_time':'datetime -not yet implemented',
                              'last_check_status':'ok/faulty -not yet implemented'}}
    write(configs)
