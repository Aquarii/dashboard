#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Imports & Setups ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
from datetime import datetime
from random import random
from time import sleep
from more_itertools import chunked, ichunked
import sqlite3 as db
import pandas as pd
from tqdm import tqdm
import config
from . import fetch
from .utils import logger_activity
# import pathlib 

#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Global Vars ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

cfg = config.configs
# for key in cfg['PG_DB'].keys():
#     if not cfg['PG_DB'][key]:      ##### For Postgres
#         cfg['PG_DB'][key] = input(f'Insert Postgres Database "{key.capitalize()}": ')

latest_workday = int(fetch.last_possible_deven())


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ DB Health ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def check_db_integrity(table_names:list): #later
    def check_instrument_last_update():
        pass
    pass


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Update DB Wholly or Selectively ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def update_db(*tables:str, async_=False, force_init=False):
    '''
    No Args= instruments + daily_quotes\n
    Pick Tables from:\n
    \t"instruments": Updates Instruments Catalogue and Share Increase.\n
    \t"daily_quotes": Updates Quotes of Instruments in Table "instruments".\n
    \t"instruments_info_only": Updates Instruments Catalogue in Table "instruments".\n
    \t"types": Updates Instrument Types in Table "types".\n
    '''
    
    ############## force_init -> pathlib.Path.unlink(path to file)
    
    if not len(tables):
        tables = ('instruments', 'identity', 'types')# 'daily_quotes',
    
    for table in tables:
        match table:
            # ---------- instruments and share increase ---------- #
            case 'instruments':
                if cfg['LAST_UPDATE']['INSTRUMENTS'] == latest_workday:
                    print('Instruments Catalogue is already up to date.')
                
                elif cfg['LAST_UPDATE']['INSTRUMENTS'] < latest_workday:
                    get_instruments_and_share_increase()
                    
                    if not cfg['LAST_UPDATE']['INSTRUMENTS'] or force_init:
                        print('Instrument List is Initialized.')
                    else:
                        print('Instrument List is Updated.')
                
                else:
                    print('Error: Invalid Configuration.')
            
            # ---------- Identities ---------- #
            case 'identity':
                if cfg['LAST_UPDATE']['IDENTITIES'] == latest_workday:
                    print('Instruments Identities are already up to date.')
                
                elif cfg['LAST_UPDATE']['IDENTITIES'] < latest_workday:
                    get_instruments_identities(async_=async_)
                    
                    if not cfg['LAST_UPDATE']['IDENTITIES'] or force_init:
                        print('Identities Initialized.')
                    else:
                        print('Identities Updated.')
            
            # ---------- Daily Quotes ---------- #
            case 'daily_quotes':
                get_daily_quotes()
            
            # ---------- instrument types ---------- #
            case 'types':
                if cfg['LAST_UPDATE']['INSTRUMENT_TYPES'].month < datetime.now().month:
                    get_instrument_types()
                
                print('Instrument Type Catalogue Updated.')
            
            # ---------- update instruments info only ---------- #
            case 'instruments_only':
                get_instruments(force_init=force_init)
                get_instruments_identities(force_init=force_init)
            
            # ---------- default ---------- #
            case _:
                print('Input Not Recognized.')


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instruments Category Info ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def get_instruments():
    conn = db.connect(cfg['PATH']['DB'])
    
    latest_workday_parsed = datetime.strptime(str(latest_workday), '%Y%m%d')
    delta = (latest_workday_parsed.weekday() - 2) % 7
    last_wed_date = latest_workday - delta
    
    # ---- All Instruments - Latest Modification
    instruments_all = fetch.instruments(0)
    instruments_all.to_sql(
        'instruments_all', 
        con=conn, 
        if_exists='replace'
    ) # TODO: dtypes later
    
    # ---- Currently Trading Instruments
    valid_instruments = instruments_all[
        instruments_all['mod_date'] > str(last_wed_date)
    ]
    valid_instruments.to_sql(
        'instruments_valid', 
        con=conn, 
        if_exists='replace', 
        index=True, 
        method='multi'
    ) # TODO: dtypes later
    
    # ---- Instruments Added Since the Last Update
    latest_additions = instruments_all[
        instruments_all['mod_date'] > str(cfg['LAST_UPDATE']['INSTRUMENTS'])
    ]
    latest_additions.to_sql(
        'instruments_latest_additions', 
        con=conn, 
        if_exists='replace', 
        index=True, 
        method='multi'
    ) # TODO: dtypes later
    
    logger_activity.info(
        f'{len(latest_additions.index)} new instruments registered at tse db since last update. '
    )
    
    conn.commit()
    conn.close()
    
    # Set Conf
    cfg['LAST_UPDATE']['INSTRUMENTS'] = latest_workday
    config.write(cfg)

#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instrument Types ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def get_instrument_types():
    instrument_types = fetch.instrument_types()
    
    conn = db.connect(cfg['PATH']['DB'])
    
    instrument_types.to_sql(
        'types', 
        con=conn, 
        if_exists='replace', 
        index=True, 
        method='multi') # dtypes later
    
    conn.commit()
    conn.close()
    
    # Set Conf
    cfg['LAST_UPDATE']['INSTRUMENT_TYPES'] = datetime.now().date()
    config.write(cfg)


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instruments Category Info & Share Increase ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def get_instruments_and_share_increase():
    _, share_increase = fetch.instruments_and_share_increase(
        last_fetch_date=0,
        last_record_id=cfg['LAST_UPDATE']['CAPITAL_INCREASE']
    )
    
    latest_workday_parsed = datetime.strptime(str(latest_workday), '%Y%m%d')
    delta = (latest_workday_parsed.weekday() - 2) % 7
    last_wed_date = latest_workday - delta
    
    conn = db.connect(cfg['PATH']['DB'])
    
    # ---- All Instruments - Latest Modification
    instruments_all = fetch.instruments(0)
    instruments_all.to_sql(
        'instruments_all', 
        con=conn, 
        if_exists='replace'
    ) # TODO: dtypes later
    
    # ---- Currently Trading Instruments
    valid_instruments = instruments_all[
        instruments_all['mod_date'] > str(last_wed_date)
    ]
    valid_instruments.to_sql(
        'instruments_valid', 
        con=conn, 
        if_exists='replace', 
        index=True, 
        method='multi'
    ) # TODO: dtypes later
    
    # ---- Instruments Added Since the Last Update
    latest_additions = instruments_all[
        instruments_all['mod_date'] > str(cfg['LAST_UPDATE']['INSTRUMENTS'])
    ]
    latest_additions.to_sql(
        'instruments_latest_additions', 
        con=conn, 
        if_exists='replace', 
        index=True, 
        method='multi'
    ) # TODO: dtypes later
    
    # Set Conf
    cfg['LAST_UPDATE']['INSTRUMENTS'] = latest_workday
    
    logger_activity.info(
        f'{len(latest_additions.index)} new instruments registered at tse db since last update. '
    )
    
    # -------- Shares -------- #
    if share_increase is not None:
        share_increase.to_sql(
            'capital_increase', 
            con=conn, 
            if_exists='append', 
            index=True, 
            method='multi') # TODO: dtypes later
        
        # set conf
        cfg['LAST_UPDATE']['CAPITAL_INCREASE'] = share_increase.index[-1]
        print('New Share Increase(s) Due to Capital Increase.')
    
    conn.commit()
    conn.close()
    config.write(cfg) 


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Daily Quotes ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def get_instruments_identities(async_=True):
    # --- Identities(attrs/شناسه): get updates, overwrite moded, append new, write to db
    conn = db.connect(cfg['PATH']['DB'])
    
    instruments = pd.read_sql(
        f'''
        SELECT id, tableu_code, mod_date 
        FROM instruments_all
        WHERE mod_date > {str(cfg['LAST_UPDATE']['IDENTITIES'])}
        ''', 
        con= conn, 
        index_col= 'id'
    )
    
    ids_to_get_attrs_for = instruments[instruments['tableu_code'] != '6'].index 
    
    if cfg['LAST_UPDATE']['IDENTITIES']:
        identities = pd.read_sql('SELECT * FROM identities', con=conn)
    else: 
        identities = pd.DataFrame()
    
    print('\nGetting Identities of Instruments...')
    
    if async_:
        identities_update = fetch.identities_async(ids_to_get_attrs_for)
        
        identities = pd.concat([identities, identities_update])
        identities = identities.drop_duplicates('کد 12 رقمی شرکت', keep='last')
        
        identities.to_sql(
            'identities', 
            con=conn, 
            if_exists='replace', 
            index=True, 
            method='multi'
        )
        conn.commit()
        
        delay_factor = 7
        sleep(random() * delay_factor)
    
    conn.close()
    
    # Set Conf
    cfg['LAST_UPDATE']['IDENTITIES'] = latest_workday
    config.write(cfg)


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Daily Quotes ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def get_daily_quotes():    
    conn = db.connect(cfg['PATH']['DB'])
    
    table_exists = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='update_tracker'")
    table_exists = table_exists.fetchone()[0]
    
    if table_exists:
        update_tracker = pd.read_sql('SELECT * FROM update_tracker', con=conn, index_col='id')
    else:
        update_tracker = pd.read_sql('SELECT id, tableu_code FROM instruments_all', con=conn, index_col='id')
        update_tracker['daily_quotes_last_update'] = 0
        update_tracker['daily_quotes_last_try'] = 0
        update_tracker['daily_quotes_last_try_result'] = 0
    
    for id in update_tracker.index:
        if update_tracker.at[id,'tableu_code'] == '6':
            update_tracker.loc[id, 'arg'] = id + ',' + str(update_tracker.at[id, 'daily_quotes_last_update']) + ',' + '1'
        else:
            update_tracker.loc[id, 'arg'] = id + ',' + str(update_tracker.at[id, 'daily_quotes_last_update']) + ',' + '0'
    
    required_insts = update_tracker[
        (update_tracker['daily_quotes_last_try'] < latest_workday) | (
            update_tracker['daily_quotes_last_try_result'] == 0)].index
    
    print('\nGetting Daily Quotes...')
    
    # Async Later # Why Freezes without Chunkes?
    for chunk in tqdm(ichunked(tqdm(required_insts), 20)):
        quotes_chunk = []
        for id in chunk:
                inst_quotes = fetch.instrument_daily_quotes_history_up_to_date(update_tracker.at[id, 'arg'])
                
                update_tracker.at[id, 'daily_quotes_last_try'] = latest_workday 
                
                if inst_quotes is None:
                    update_tracker.at[id, 'daily_quotes_last_try_result'] = 0
                
                else:
                    quotes_chunk.append(inst_quotes)
                    update_tracker.at[id, 'daily_quotes_last_update'] = inst_quotes.iloc[-1]['date']
                    update_tracker.at[id, 'daily_quotes_last_try_result'] = 1
        
        daily_quotes = pd.concat(quotes_chunk)
        daily_quotes.drop_duplicates(['instrument_id','date'], keep='last', inplace=True)
        daily_quotes.to_sql('daily_quotes', con=conn, if_exists='append', index=False) # بدون اینکه چک کنه قبلا درج شده باشه اضافه میکنه. وقتی ارور میده چون از ادامش رزوم نمیشه اون اولی هایی ک رایت شده اند رو هم دوباره درج میکنه
        update_tracker.to_sql('update_tracker', con=conn, if_exists='replace', index= True)
    
    conn.commit()
    conn.close()


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ All Instruments Info in One Table ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def instruments_aio():
    conn = db.connect(cfg['PATH']['DB'])
    
    identities = pd.read_sql('SELECT * FROM identities', con=conn)
    insts_db = pd.read_sql('SELECT * FROM instruments_all', con=conn)
    types = pd.read_sql('SELECT * FROM types', con=conn)
    # update_tracker = pd.read_sql('SELECT id, daily_quotes_last_update FROM update_tracker', con=conn)
    
    instruments_aio = insts_db.merge(identities, 'left', on='isin')
    instruments_aio = instruments_aio.merge(types, 'left', left_on='type_code', right_on='code')
    instruments_aio = instruments_aio.drop('code',axis=1).drop_duplicates('isin')
    # instruments_aio = instruments_aio.merge(update_tracker, 'inner', on='id', suffixes=(None, '_r'))
    
    instruments_aio.to_sql('instruments_aio', con=conn, if_exists='replace', index=False)
    
    conn.commit()
    conn.close()