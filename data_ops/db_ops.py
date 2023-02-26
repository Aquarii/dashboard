#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Imports & Setups ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
from datetime import datetime
from more_itertools import ichunked
from sqlalchemy import create_engine, URL, text
import pandas as pd
import config
from . import fetch
from sys import exit
import logging 
from tqdm import tqdm


logging.basicConfig(
    filename='log.log', 
    filemode='a', 
    level=logging.INFO,
    format='%(asctime)s >> %(levelname)s >> %(filename)s >> %(lineno)d >> %(message)s')


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Global Vars ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

latest_workday = int(fetch.last_possible_deven())
cfg = config.configs
db_url = URL.create(
    'postgresql+psycopg2',
    username = cfg['DB']['USERNAME'],
    password = cfg['DB']['PASSWORD'],
    host = cfg['DB']['HOST'],
    database = cfg['DB']['NAME']
)
engine = create_engine(db_url, echo=False)


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ DB Health ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def check_db_integrity(table_names:list): #later
    def check_instrument_last_update():
        pass
    pass


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Update DB Wholly or Selectively ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def update_db(*tables:str, force_init=False):
    '''
    No Args= instruments + daily_quotes\n
    Pick Tables from:\n
    \t"instruments": Updates Instruments Catalogue and Share Increase.\n
    \t"daily_quotes": Updates Quotes of Instruments in "instruments".\n
    \t"instruments_info_only": Updates Instruments Catalogue in "instruments".\n
    \t"instrument_types": Updates Instrument Types in "instrument_types".\n
    '''
    if not len(tables):
        tables = ('instruments', 'daily_quotes')
    
    for table in tables:
        match table:
            # ---------- instruments and share increase ---------- #
            case 'instruments':
                if cfg['LAST_UPDATE']['INSTRUMENTS'] == latest_workday:
                    print('Instruments Catalogue is already up to date.')
                
                elif cfg['LAST_UPDATE']['INSTRUMENTS'] < latest_workday:
                    update_instruments_info_and_share_increase(force_init=force_init)
                    
                    if not cfg['LAST_UPDATE']['INSTRUMENTS']:
                        update_instrument_types()
                        
                        # set conf
                        cfg['LAST_UPDATE']['INSTRUMENT_TYPES'] = datetime.now().date()
                        
                        print('Catalogue Initialized.')
                    
                    else:
                        print('Catalogue updated.')
                    
                    # set conf
                    cfg['LAST_UPDATE']['INSTRUMENTS'] = latest_workday
                
                else:
                    print('Error: Invalid Configuration.')
            
            # ---------- daily quotes ---------- #
            case 'daily_quotes':
                if cfg['LAST_UPDATE']['DAILY_QUOTES'] == latest_workday:
                    print('Daily Quotes are already up to date.')
                
                elif cfg['LAST_UPDATE']['DAILY_QUOTES'] < latest_workday:
                    update_daily_quotes(force_init=force_init)
                    
                    if not cfg['LAST_UPDATE']['DAILY_QUOTES']:
                        print('Daily Quotes Initialized.')
                    
                    else:
                        print('Daily Quotes updated.')
                    
                    #set config
                    cfg['LAST_UPDATE']['DAILY_QUOTES'] = latest_workday
                
                else:
                    print('Error: Invalid Configuration.')
            
            # ---------- instrument types ---------- #
            case 'instrument_types':
                update_instrument_types()
                
                print('Instrument Type Catalogue Updated.')
                
                # set conf
                cfg['LAST_UPDATE']['INSTRUMENT_TYPES'] = datetime.now().date()
            
            # ---------- update instruments info only ---------- #
            case 'instruments_info_only':
                update_instruments_info()
                
                # set conf
                cfg['LAST_UPDATE']['INSTRUMENTS'] = latest_workday
            
            # ---------- default ---------- #
            case _:
                print('Input Not Recognized.')
    
    config.write(cfg)


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instruments Category Info ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def update_instruments_info(force_init=False):
    if force_init:
        confimation = input("NOT gonna take a long time anymore! xp (approx. 45 mins). Say 'yes' if you sure?")
        if confimation == 'yes':
            cfg['LAST_UPDATE']['INSTRUMENTS'] = 0
        else:
            exit(0)
    
    # Instruments: get updates, overwrite moded, append new, save mod history
    all_open_instruments = fetch.instruments(0) 
    instrument_update = fetch.instruments(cfg['LAST_UPDATE']['INSTRUMENTS'])
    
    ids_to_get_attrs_for = instrument_update[instrument_update['tableu_code'] != '6'].index
    
    conn = engine.connect()
    
    if cfg['LAST_UPDATE']['INSTRUMENTS']:
        instrument_prev = pd.read_sql(
            sql=text('SELECT * FROM instruments'), 
            con=conn, 
            index_col='id')
        
        common_filter = instrument_update.index.isin(instrument_prev.index) 
        new_instruments = instrument_update[~common_filter]
        repeated_instruments = instrument_update[common_filter]
        shelved_instruments = instrument_prev[~instrument_prev.index.isin(all_open_instruments.index)] # Usage in History
        
        new_instruments.to_sql('instruments_latest_addition', conn, if_exists='replace', index=True)
        logging.info(
            f'{len(new_instruments.index)} new instruments registered at tse db since last update. ')
        
        for idx in repeated_instruments.index:
            instrument_prev.loc[idx] = repeated_instruments.loc[idx]
        
        instrument_update = pd.concat([instrument_prev, new_instruments])
        
        # region: History of Modifications of All the Instruments #later
        # instruments_mod_history = pd.concat([instrument_prev, instrument_update])
        # instruments_mod_history.drop_duplicates(['isin','mod_date'], inplace=True)
        
        # instruments_mod_history.to_sql(
        #     'instrument_history', 
        #     con=conn, 
        #     if_exists='replace', 
        #     index=True, 
        #     method='multi') # dtypes later
        # endregion
    
    instrument_update.to_sql(
        'instruments', 
        con=conn, 
        if_exists='replace', 
        index=True, 
        method='multi') # dtypes later
    
    # Identities(attrs/شناسه): get updates, overwrite moded, append new, write to db
    if cfg['LAST_UPDATE']['INSTRUMENTS']:
        identities = pd.read_sql(text('SELECT * FROM identity'), con=conn)
    else: 
        identities = pd.DataFrame()
    
    identities_update = fetch.identities_async(ids_to_get_attrs_for)
    
    identities = pd.concat([identities, identities_update])
    identities.drop_duplicates(['isin','attr'], keep='last', inplace=True)
    identities.to_sql(
        'identity', 
        con=conn, 
        if_exists='replace', 
        index=False, 
        method='multi')
    
    conn.commit()
    conn.close()

#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instrument Types ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def update_instrument_types():
    instrument_types = fetch.instrument_types()
    
    conn = engine.connect()
    
    instrument_types.to_sql(
        'instrument_types', 
        con=conn, 
        if_exists='replace', 
        index=True, 
        method='multi') # dtypes later
    
    conn.commit()
    conn.close()


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instruments Category Info & Share Increase ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def update_instruments_info_and_share_increase(force_init=False):
    if force_init:
        confimation = input("Say 'yes' if you're sure? ")
        if confimation == 'yes':
            cfg['LAST_UPDATE']['INSTRUMENTS'] = 0
            cfg['LAST_UPDATE']['CAPITAL_INCREASE'] = 0
        else:
            exit(0)
    
    # Instruments: get updates, overwrite moded, append new, save mod history, write to db
    # all_open_instruments = fetch.instruments(0) # FOR HISTORY
    instrument_update, share_increase = fetch.instruments_and_share_increase(
        last_fetch_date=cfg['LAST_UPDATE']['INSTRUMENTS'],
        last_record_id=cfg['LAST_UPDATE']['CAPITAL_INCREASE']
    )
    
    ids_to_get_attrs_for = instrument_update[instrument_update['tableu_code'] != '6'].index ###########################
    
    conn = engine.connect()
    
    if cfg['LAST_UPDATE']['INSTRUMENTS']:
        instrument_prev = pd.read_sql(
            sql=text('SELECT * FROM instruments'), 
            con=conn, 
            index_col='id'
        )
        
        common_filter = instrument_update.index.isin(instrument_prev.index) 
        new_instruments = instrument_update[~common_filter]
        repeated_instruments = instrument_update[common_filter]
        # shelved_instruments = instrument_prev[~instrument_prev.index.isin(all_open_instruments.index)] # FOR HISTORY
        
        new_instruments.to_sql('instruments_latest_addition', conn, if_exists='replace', index=True)
        logging.info(
            f'{len(new_instruments.index)} new instruments registered at tse db since last update. ')
        
        for idx in repeated_instruments.index:
            instrument_prev.loc[idx] = repeated_instruments.loc[idx]
        
        instrument_update = pd.concat([instrument_prev, new_instruments])
        
        # region: History of Modifications of All the Instruments
        # instruments_mod_history = pd.concat([instrument_prev, instrument_update])
        # instruments_mod_history.drop_duplicates(['isin','mod_date'], inplace=True)
        
        # instruments_mod_history.to_sql(
        #     'instruments_history', 
        #     con=__conn, 
        #     if_exists='replace', 
        #     index=True, 
        #     method='multi') # dtypes later
        # endregion
    
    instrument_update.to_sql(
        'instruments', 
        con=conn, 
        if_exists='replace', 
        index=True, 
        method='multi') # dtypes later
    
    # --- Identities(attrs/شناسه): get updates, overwrite moded, append new, write to db
    if cfg['LAST_UPDATE']['INSTRUMENTS']:
        identities = pd.read_sql(text('SELECT * FROM identity'), con=conn)
    else: 
        identities = pd.DataFrame()
    
    identities_update = fetch.identities_async(ids_to_get_attrs_for)
    
    identities = pd.concat([identities, identities_update])
    identities.drop_duplicates(['isin','attr'], keep='last', inplace=True)
    identities.to_sql(
        'identity', 
        con=conn, 
        if_exists='replace', 
        index=False, 
        method='multi')
    
    if share_increase is not None:
        share_increase.to_sql(
            'capital_increase', 
            con=conn, 
            if_exists='append', 
            index=True, 
            method='multi') # dtypes later
        
        # set conf
        cfg['LAST_UPDATE']['CAPITAL_INCREASE'] = share_increase.index[-1]
        print('New Share Change(s) Due to Capital Increase.')
    
    # clean up the mess. TODO: Replace aqlalchemy with psycopg2.
    conn.commit()
    conn.close()
    config.write(cfg) 


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Daily Quotes ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def update_daily_quotes(force_init=False):    
    conn = engine.connect()
    instruments = pd.read_sql(text('select id, tableu_code from instruments'), con=conn, index_col='id')
    
    if force_init:
        date = 0
    else:
        date = cfg['LAST_UPDATE']['DAILY_QUOTES']
    
    def make_arg(id, date):
        if instruments.at[id,'tableu_code'] == '6':
            return id + ',' + str(date) + ',' + '1'
        else:
            return id + ',' + str(date) + ',' + '0'
    
    print('\nGetting Daily Quotes...')
    
    # Async Later # Freezes without Chunkes
    for chunk in tqdm(ichunked(tqdm(instruments.index), 20)):
        daily_quotes = pd.concat(
            [fetch.instrument_daily_quotes_history_up_to_date(make_arg(id,date)) for id in chunk]
        )
        
        daily_quotes.to_sql('daily_quotes', con=conn, if_exists='append', index=False)
    
    conn.commit()
    conn.close()


