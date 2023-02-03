from datetime import datetime
import sqlite3 as db
import pandas as pd
import config
from . import transform, api, scrape
from sys import exit
import logging 

logging.basicConfig(filename='log.log', filemode='w', level=logging.INFO,
                    format='%(asctime)s >> %(levelname)s >> %(filename)s >> %(lineno)d >> %(message)s')

latest_workday = int(api.last_possible_deven())
cfg = config.configs


def check_db_integrity(table_names:list): #half-ass
    __con_catalogue = db.connect(cfg['configs']['db_path'])
    __cur = __con_catalogue.cursor()
    for table in table_names:
        __cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
        result = __cur.fetchone()
        if not result:
            print(f'debug info: "{table}" is not defined yet, creation in progress...')
            update_table(table, initialize=True)
            cfg['last_updates']['instrument'] = latest_workday 
    __con_catalogue.close()
    config.write(cfg)


def update_table(table_name:str, force_init=False):
    __con_catalogue = db.connect(cfg['configs']['db_path'])
    
    match table_name:
        case 'instrument':
            if force_init:
                confimation = input("NOT gonna take a long time anymore! xp (approx. 45 mins). Say 'yes' if you sure?")
                if confimation == 'yes':
                    cfg['last_updates']['instrument'] = 0
                else:
                    exit(0)
        
        # --- Instruments: get updates, overwrite moded, append new, save mod history
            instrument_update = transform.Instruments(cfg['last_updates']['instrument'])
            ids_to_get_identity = instrument_update[instrument_update['tableu_code'] != '6'].index
            
            if cfg['last_updates']['instrument']:
                instrument_prev = pd.read_sql(
                    sql='SELECT * FROM instrument', 
                    con=__con_catalogue, 
                    index_col='id')
                
                common_filter = instrument_update.index.isin(instrument_prev.index) 
                new_instruments = instrument_update[~common_filter]
                repeated_instruments = instrument_update[common_filter]
                
                for idx in repeated_instruments.index:
                    instrument_prev.loc[idx] = repeated_instruments.loc[idx]
                
                instrument_update = pd.concat([instrument_prev, new_instruments])
                instrument_mod_history = pd.concat([instrument_prev, instrument_update])
                instrument_mod_history.drop_duplicates(['isin','mod_date'], inplace=True)
                
                instrument_mod_history.to_sql(
                    'instrument_history', 
                    con=__con_catalogue, 
                    if_exists='replace', 
                    index=True, 
                    method='multi') # dtypes later
                print(len(new_instruments.index), 
                    'new instruments registered at tse db since last update: ', 
                    new_instruments['ticker'])
            instrument_update.to_sql(
                'instrument', 
                con=__con_catalogue, 
                if_exists='replace', 
                index=True, 
                method='multi') # dtypes later
        # --- Identities(attrs): get updates, overwrite moded, append new, write to db
            if cfg['last_updates']['instrument']:
                identities = pd.read_sql('SELECT * FROM identity', con=__con_catalogue)
            else: 
                identities = pd.DataFrame()
            print('Getting Identities...')
            matrices = scrape.get_identities_async(ids_to_get_identity)
            
            updated_ids = pd.DataFrame()
            for matrix in matrices:
                inst_id = pd.DataFrame(
                    matrix, 
                    columns= ['attr', 'value'], 
                    index= None)
                inst_id['isin'] = matrix[0][1]
                updated_ids = pd.concat([updated_ids, inst_id])
            
            identities = pd.concat([identities, updated_ids])
            identities.drop_duplicates(['isin','attr'], keep='last', inplace=True)
            identities.to_sql(
                'identity', 
                con=__con_catalogue, 
                if_exists='replace', 
                index=False, 
                method='multi')
            # set conf
            cfg['last_updates']['instrument'] = latest_workday 
        
        case 'instrument_type':
            instrument_types = transform.instrument_type(scrape.instrument_types())
            instrument_types.to_sql(
                'instrument_type', 
                con=__con_catalogue, 
                if_exists='replace', 
                index=True, 
                method='multi') # dtypes later
            # set conf
            cfg['last_updates']['instrument_type'] = datetime.now()
        
        case 'instrument_and_capital_increase':
            if force_init:
                confimation = input("Say 'yes' if you're sure?")
                if confimation == 'yes':
                    cfg['last_updates']['instrument'] = 0
                    cfg['last_updates']['capital_increase'] = 0
                else:
                    exit(0)
        
        # --- Instruments: get updates, overwrite moded, append new, save mod history, write to db
            instrument_update, capital_increase = transform.instrument_and_capital_increase(
                last_fetch_date=cfg['last_updates']['instrument'],
                last_record_id=cfg['last_updates']['capital_increase'])
            ids_to_get_identity = instrument_update[instrument_update['tableu_code'] != '6'].index
            
            if cfg['last_updates']['instrument']:
                instrument_prev = pd.read_sql(sql='SELECT * FROM instrument', con=__con_catalogue, index_col='id')
                
                common_filter = instrument_update.index.isin(instrument_prev.index) 
                new_instruments = instrument_update[~common_filter]
                repeated_instruments = instrument_update[common_filter]
                
                for idx in repeated_instruments.index:
                    instrument_prev.loc[idx] = repeated_instruments.loc[idx]
                
                instrument_update = pd.concat([instrument_prev, new_instruments])
                instrument_mod_history = pd.concat([instrument_prev, instrument_update])
                instrument_mod_history.drop_duplicates(['isin','mod_date'], inplace=True)
                
                instrument_mod_history.to_sql(
                    'instrument_history', 
                    con=__con_catalogue, 
                    if_exists='replace', 
                    index=True, 
                    method='multi') # dtypes later
                print(len(new_instruments.index), 
                    'new instruments registered at tse db since last update: ', 
                    new_instruments['ticker'])
            
            instrument_update.to_sql(
                'instrument', 
                con=__con_catalogue, 
                if_exists='replace', 
                index=True, 
                method='multi') # dtypes later
            
        # --- Identities(attrs): get updates, overwrite moded, append new, write to db
            if cfg['last_updates']['instrument']:
                identities = pd.read_sql('SELECT * FROM identity', con=__con_catalogue)
            else: 
                identities = pd.DataFrame()
            print('Getting Identities...')
            matrices = scrape.get_identities_async(ids_to_get_identity)
            
            updated_ids = pd.DataFrame()
            for matrix in matrices:
                inst_id = pd.DataFrame(
                    matrix, 
                    index= None).transpose()
                inst_id.drop(0, inplace=True)
                inst_id.columns=['code','code_5','name_latin','code_4','company_name','ticker','description','isin','market','tableu_code','industry_sector_code','industry_sector','industry_subsector_code','industry_subsector']
                updated_ids = pd.concat([updated_ids, inst_id])
            
            identities = pd.concat([identities, updated_ids])
            identities.drop_duplicates('isin', keep='last', inplace=True)
            identities.to_sql(
                'identity', 
                con=__con_catalogue, 
                if_exists='replace', 
                index=False, 
                method='multi')
            # set conf
            cfg['last_updates']['instrument'] = latest_workday
            if capital_increase is not None:
                capital_increase.to_sql(
                    'capital_increase', 
                    con=__con_catalogue, 
                    if_exists='append', 
                    index=True, 
                    method='multi') # dtypes later
                # set conf
                cfg['last_updates']['capital_increase'] = capital_increase.index[-1]
    __con_catalogue.close()
    config.write(cfg)


def load_table(table_name, param):
    pass
