import pandas as pd
from . import api


#=============== Instruments Catalogue =======================#
def Instruments(last_fetch:int):
    instruments = api.instruments(last_fetch)
    if instruments:
        instruments = pd.DataFrame(instrument.split(',') for instrument in instruments.split(';'))
        instruments.columns = ['id',
                               'code',
                               'code_5',
                               'name_latin',
                               'code_4',
                               'ticker',
                               'description',
                               'isin',
                               'mod_date',
                               'market_code',
                               'company_name',
                               'status_code',
                               'group_code',
                               'market_type',
                               'tableu_code',
                               'industry_sector_code',
                               'industry_subsector_code',
                               'type_code']
        instruments.set_index('id', inplace=True)
        instruments['ticker'] = instruments['ticker'].str.replace('ك','ک').str.replace('ي', 'ی').str.replace('\u200c',' ').str.replace('_',' ').str.strip()
        instruments['description'] = instruments['description'].str.replace('ك','ک').str.replace('ي', 'ی').str.replace('\u200c',' ').str.replace('_',' ').str.strip()
        instruments['company_name'] = instruments['company_name'].str.replace('ك','ک').str.replace('ي', 'ی').str.replace('\u200c',' ').str.replace('_',' ').str.strip()
        return instruments


#=============== Update Instruments and Shares ====================#
def instrument_and_capital_increase(last_fetch_date:int, last_record_id):
    instruments, capital_increase = api.instruments_and_shares(last_fetch_date, last_record_id).split('@')

    if instruments:
        instruments = instruments.split(';')
        instruments = pd.DataFrame([instrument.split(',')for instrument in instruments])
        instruments.columns = ['id',
                               'code',
                               'code_5',
                               'name_latin',
                               'code_4',
                               'ticker',
                               'description',
                               'isin',
                               'mod_date',
                               'market_code',
                               'company_name',
                               'status_code',
                               'group_code',
                               'market_type',
                               'tableu_code',
                               'industry_sector_code',
                               'industry_subsector_code',
                               'type_code']
        instruments.set_index('id', inplace=True)
        instruments['ticker'] = instruments['ticker'].str.replace('ك','ک').str.replace('ي', 'ی').str.replace('\u200c',' ').str.replace('_',' ').str.strip()
        instruments['description'] = instruments['description'].str.replace('ك','ک').str.replace('ي', 'ی').str.replace('\u200c',' ').str.replace('_',' ').str.strip()
        instruments['company_name'] = instruments['company_name'].str.replace('ك','ک').str.replace('ي', 'ی').str.replace('\u200c',' ').str.replace('_',' ').str.strip()

    if capital_increase:
        capital_increase = capital_increase.split(';')
        capital_increase = pd.DataFrame([share.split(',') for share in capital_increase])
        capital_increase.columns = ['record_id',
                                    'id',
                                    'date',
                                    'before_raise',
                                    'after_raise']
        #capital_increase['record_id'] = capital_increase['record_id'].astype(int)
        capital_increase.set_index('record_id', inplace=True)
    else:
        capital_increase = None
        
    return instruments, capital_increase


#=============== Update Instrument Types =================#
def instrument_type(matrix):
    return pd.DataFrame(
        matrix, 
        columns=['code', 'category', 'sub_category'],
        index=None).dropna(
            how='all', 
            subset=['category', 'sub_category']
            ).set_index('code')


#=============== Identity ========================#
def identity(matrix):
    return pd.DataFrame(
        matrix, 
        columns= ['attr', 'value'], 
        index= None)

