from datetime import datetime
import sqlite3 as db
import pandas as pd
import config
from . import scrape, fetch
from sys import exit
import logging 
from tqdm import tqdm


logging.basicConfig(filename='log.log', filemode='w', level=logging.INFO,
                    format='%(asctime)s >> %(levelname)s >> %(filename)s >> %(lineno)d >> %(message)s')

latest_workday = int(fetch.last_possible_deven())
cfg = config.configs


def check_db_integrity(table_names:list): #later
    pass


def update_instruments_info(force_init=False):
    if force_init:
        confimation = input("NOT gonna take a long time anymore! xp (approx. 45 mins). Say 'yes' if you sure?")
        if confimation == 'yes':
            cfg['last_update']['instrument'] = 0
        else:
            exit(0)
    
    # Instruments: get updates, overwrite moded, append new, save mod history
    all_open_instruments = fetch.instruments(0) 
    instrument_update = fetch.instruments(cfg['last_update']['instrument'])
    
    ids_to_get_attrs_for = instrument_update[instrument_update['tableu_code'] != '6'].index
    
    __conn = db.connect(cfg['path']['db'])
    
    if cfg['last_update']['instrument']:
        instrument_prev = pd.read_sql(
            sql='SELECT * FROM instrument', 
            con=__conn, 
            index_col='id')
        
        common_filter = instrument_update.index.isin(instrument_prev.index) 
        new_instruments = instrument_update[~common_filter]
        repeated_instruments = instrument_update[common_filter]
        shelved_instruments = instrument_prev[~instrument_prev.index.isin(all_open_instruments.index)]
        
        for idx in repeated_instruments.index:
            instrument_prev.loc[idx] = repeated_instruments.loc[idx]
        
        instrument_update = pd.concat([instrument_prev, new_instruments])
        
        # instruments_mod_history = pd.concat([instrument_prev, instrument_update])
        # instruments_mod_history.drop_duplicates(['isin','mod_date'], inplace=True)
        
        # instruments_mod_history.to_sql(
        #     'instrument_history', 
        #     con=__conn, 
        #     if_exists='replace', 
        #     index=True, 
        #     method='multi') # dtypes later
        
        logging.info(
            len(new_instruments.index), 
            'new instruments registered at tse db since last update: ', 
            new_instruments['ticker'])
    
    instrument_update.to_sql(
        'instruments', 
        con=__conn, 
        if_exists='replace', 
        index=True, 
        method='multi') # dtypes later
    
    # Identities(attrs/شناسه): get updates, overwrite moded, append new, write to db
    if cfg['last_update']['instrument']:
        identities = pd.read_sql('SELECT * FROM identity', con=__conn)
    else: 
        identities = pd.DataFrame()
    
    identities_update = fetch.identities_async(ids_to_get_attrs_for)
    
    identities = pd.concat([identities, identities_update])
    identities.drop_duplicates(['isin','attr'], keep='last', inplace=True)
    identities.to_sql(
        'identity', 
        con=__conn, 
        if_exists='replace', 
        index=False, 
        method='multi')
    
    __conn.close()
    
    # set conf
    cfg['last_update']['instrument'] = latest_workday
    config.write(cfg)


def update_instrument_types():
    instrument_types = fetch.instrument_types()
    
    __conn = db.connect(cfg['path']['db'])
    
    instrument_types.to_sql(
        'instrument_type', 
        con=__conn, 
        if_exists='replace', 
        index=True, 
        method='multi') # dtypes later
    
    __conn.close()
    
    # set conf
    cfg['last_update']['instrument_type'] = datetime.now()
    config.write(cfg)


def update_instruments_and_capital_increase(force_init=False):
    if force_init:
        confimation = input("Say 'yes' if you're sure?")
        if confimation == 'yes':
            cfg['last_update']['instrument'] = 0
            cfg['last_update']['capital_increase'] = 0
        else:
            exit(0)
    
    # Instruments: get updates, overwrite moded, append new, save mod history, write to db
    # all_open_instruments = fetch.instruments(0)
    instrument_update, capital_increase = fetch.instruments_and_capital_increase(
        last_fetch_date=cfg['last_update']['instrument'],
        last_record_id=cfg['last_update']['capital_increase'])
    
    ids_to_get_attrs_for = instrument_update[instrument_update['tableu_code'] != '6'].index
    
    __conn = db.connect(cfg['path']['db'])
    
    if cfg['last_update']['instrument']:
        instrument_prev = pd.read_sql(
            sql='SELECT * FROM instrument', 
            con=__conn, 
            index_col='id')
        
        common_filter = instrument_update.index.isin(instrument_prev.index) 
        new_instruments = instrument_update[~common_filter]
        repeated_instruments = instrument_update[common_filter]
        # shelved_instruments = instrument_prev[~instrument_prev.index.isin(all_open_instruments.index)]
        
        for idx in repeated_instruments.index:
            instrument_prev.loc[idx] = repeated_instruments.loc[idx]
        
        instrument_update = pd.concat([instrument_prev, new_instruments])
        
        # instruments_mod_history = pd.concat([instrument_prev, instrument_update])
        # instruments_mod_history.drop_duplicates(['isin','mod_date'], inplace=True)
        
        # instruments_mod_history.to_sql(
        #     'instrument_history', 
        #     con=__conn, 
        #     if_exists='replace', 
        #     index=True, 
        #     method='multi') # dtypes later
        
        logging.info(
            len(new_instruments.index), 
            'new instruments registered at tse db since last update: ', 
            new_instruments['ticker'])
    
    instrument_update.to_sql(
        'instruments', 
        con=__conn, 
        if_exists='replace', 
        index=True, 
        method='multi') # dtypes later
    
# --- Identities(attrs/شناسه): get updates, overwrite moded, append new, write to db
    if cfg['last_update']['instrument']:
        identities = pd.read_sql('SELECT * FROM identity', con=__conn)
    else: 
        identities = pd.DataFrame()
    
    identities_update = fetch.identities_async(ids_to_get_attrs_for)
    
    identities = pd.concat([identities, identities_update])
    identities.drop_duplicates(['isin','attr'], keep='last', inplace=True)
    identities.to_sql(
        'identity', 
        con=__conn, 
        if_exists='replace', 
        index=False, 
        method='multi')
    
    # set conf
    cfg['last_update']['instrument'] = latest_workday
    
    if capital_increase is not None:
        capital_increase.to_sql(
            'capital_increase', 
            con=__conn, 
            if_exists='append', 
            index=True, 
            method='multi') # dtypes later
    
    __conn.close()
    
    # set conf
    cfg['last_update']['capital_increase'] = capital_increase.index[-1]
    config.write(cfg)

#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Daily Quotes ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def update_daily_quotes(force_init=False):
    __conn = db.connect(cfg['path']['db'])
    
    instruments = pd.read_sql('select id, tableu_code from instrument', con=__conn, index_col='id')
    
    def make_arg(id,date):
        if instruments.at[id,'tableu_code'] == '6':
            return id + ',' + str(date) + ',' + '1'
        else:
            return id + ',' + str(date) + ',' + '0'
    
    if force_init:
        date = 0
    else:
        date = cfg['last_update']['daily_quotes']
    
    daily_ohlcv = pd.concat(
        [fetch.instrument_daily_quotes_history_up_to_date(make_arg(id,date)) for id in tqdm(instruments.index)])
    # daily_ohlcv = scrape.get_daily_quotes_async(instruments.index[10:30]) #Async #Later
    
    #write to db
    if date:
        daily_ohlcv.to_sql('daily_ohlcv', con=__conn, if_exists='append', index=False)
    else:
        daily_ohlcv.to_sql('daily_ohlcv', con=__conn, if_exists='replace', index=False)
    
    #set config
    cfg['last_update']['daily_quotes'] = latest_workday
    
    __conn.close()
    config.write(cfg)