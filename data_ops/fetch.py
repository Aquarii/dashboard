from random import random
from time import sleep
import zeep
import asyncio
import requests
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import config
from .utils import logger_app, ar_to_fa_series, ar_to_fa


cfg = config.configs

request_headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Referer':'http://main.tsetmc.com/StaticContent/WebServiceHelp'
    }
cookie_jar = {'ASP.NET_SessionId': 'wa40en1alwxzjnqehjntrv5j'}


# ================================================================= #
#                               Utils                               #
# ================================================================= #

#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Convert XML/HTML <table> to Matrix ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def html_table_to_matrix(xml_table):
    """Parses a html segment started with tag <table> followed
    by multiple <tr> (table rows) and inner <td> (table data) tags.
    It returns a list of rows with inner columns.
    Accepts only one <th> (table header/data) in the first row.
    """
    def rowgetDataText(tr, coltag='td'): # td (data) or th (header)
        return [ar_to_fa(td.get_text(strip=True)) for td in tr.find_all(coltag)]
    
    rows = []
    trs = xml_table.find_all('tr')
    headerow = rowgetDataText(trs[0], 'th')
    
    if headerow: # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    
    for tr in trs: # for every table row
        rows.append(rowgetDataText(tr, 'td')) # data row
    return rows


# ================================================================= #
#                               API                                 #
# ================================================================= #

#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instruments (API) ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def instruments(last_fetch):
    '''
    last_fetch:\n\tDate after which Traded Instruments are needed.
    '''
    client = zeep.Client(wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    instruments = client.service.Instrument(last_fetch)
    if instruments:
        instruments = pd.DataFrame(instrument.split(',') for instrument in instruments.split(';'))
        instruments.columns = [
            'id',
            'inst_code_12',
            'inst_code_5',
            'name_latin',
            'company_code_4',
            'ticker',
            'name',
            'isin',
            'mod_date',
            'market_code',
            'company_name',
            'status_code',
            'group_code',
            'market_type_code',
            'tableu_code',
            'industry_sector_code',
            'industry_subsector_code',
            'type_code']
        instruments.set_index('id', inplace=True)
        instruments['ticker'] = ar_to_fa_series(instruments['ticker'])
        instruments['name'] = ar_to_fa_series(instruments['name'])
        instruments['company_name'] = ar_to_fa_series(instruments['company_name'])
        return instruments
    else:
        print('No Response from Endpoint: Instrument')


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Last Deven Possible (API) ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def last_possible_deven():
    client = zeep.Client('http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    last_workday = client.service.LastPossibleDeven()
    return last_workday.split(';')[0] if last_workday.split(';')[1] == last_workday.split(';')[0] else print(last_workday) # for debuging purposes


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instruments and Shares (API) ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def instruments_and_share_increase(last_fetch_date, last_record_id):
    client = zeep.Client(wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    
    instruments, share_increase = client.service.InstrumentAndShare(last_fetch_date, last_record_id).split('@')
    
    if instruments:
        instruments = instruments.split(';')
        instruments = pd.DataFrame([instrument.split(',')for instrument in instruments])
        instruments.columns = [
            'id',
            'inst_code_12',
            'inst_code_5',
            'name_latin',
            'company_code_4',
            'ticker',
            'name',
            'isin',
            'mod_date',
            'market_code',
            'company_name',
            'status_code',
            'group_code',
            'market_type_code',
            'tableu_code',
            'industry_sector_code',
            'industry_subsector_code',
            'type_code']
        instruments.set_index('id', inplace=True)
        instruments['ticker'] = ar_to_fa_series(instruments['ticker'])
        instruments['name'] = ar_to_fa_series(instruments['name'])
        instruments['company_name'] = ar_to_fa_series(instruments['company_name'])
    
    if share_increase:
        share_increase = share_increase.split(';')
        share_increase = pd.DataFrame([share.split(',') for share in share_increase])
        share_increase.columns = [
            'record_id',
            'id',
            'date',
            'before_raise',
            'after_raise']
        #capital_increase['record_id'] = capital_increase['record_id'].astype(int)
        share_increase.set_index('record_id', inplace=True)
    else:
        share_increase = None
        
    return instruments, share_increase


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Decompress And Get Insturments Closing Prices (API) ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def decompress_closing_prices(instruments_compressed:str):
    client = zeep.Client(wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    return client.service.DecompressAndGetInsturmentClosingPrice(instruments_compressed)


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Log Error (API) ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def log_error(error_str):
    client = zeep.Client(wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    return client.service.LogError(error_str)



# ================================================================= #
#                               Scrape                              #
# ================================================================= #

#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Scrape Instrument Types ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def instrument_types():
    resp = requests.get(
        'http://cdn.tsetmc.com/api/StaticData/GetStaticContent/WS-Instrument', 
        headers= request_headers)
    
    repeats = 0
    if resp and resp.text:
        soup = BeautifulSoup(resp.text, 'lxml')
        table = soup.find_all('tbody')[3] 
    elif repeats < 3:
        sleep(2.0)
        repeats += 1
        instrument_types()
    else:
        print('Getting Instrument Types Failed.')
    
    logger_app.info(f'Got Instrument Types After <{repeats}> Retries.')
    
    return pd.DataFrame(
        html_table_to_matrix(table), 
        columns=['code', 'type', 'sub_type'],
        index=None).dropna(
            how='all', 
            subset=['type', 'sub_type']
            ).set_index('code')


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Get Instruments Attrib(Identity) - Synchronous ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def identity(instrument_id):
    resp = requests.get(
        f'http://www.tsetmc.com/Loader.aspx?Partree=15131M&i={instrument_id}', 
        headers= request_headers)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, 'lxml')
    table = soup.find("table", class_='table1')
    matrix = html_table_to_matrix(table)
    inst_id = pd.DataFrame(
        matrix, 
        columns= ['attr', 'value'], 
        index= None
    )
    inst_id['isin'] = matrix[7][1]
    
    return inst_id


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Scrape Instrument Attribs/Identity (شناسه) - Async ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
# region: Scrape Attribs Async
async def get_identity_html(instrument_id: str) -> str:
    # logger_app.info(f"Getting HTML for instrument {instrument_id}")
    
    url = f'http://www.tsetmc.com/Loader.aspx?Partree=15131M&i={instrument_id}'
    
    tcp_connector = aiohttp.TCPConnector(limit_per_host=1)
    # sem = asyncio.Semaphore(25)
    # async with sem:
    async with aiohttp.ClientSession(connector=tcp_connector) as session:
        async with session.get(url, headers=request_headers) as resp:
            resp.raise_for_status()
            
            html = await resp.text()
            return html

# parse HTML and extract Table
def get_table_off_the_page(html: str):
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find("table", class_='table1')
    if not table:
        return "MISSING"
    
    return html_table_to_matrix(table)

# loop through coroutines and get insts attrs(IDs) matrix
async def get_instruments_identity(instrument_ids):
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(get_identity_html(id)) for id in instrument_ids]
    
    delay_factor = 1
    all_identities = []
    for task in tqdm(tasks):
        html = await task
        matrix = get_table_off_the_page(html)
        all_identities.append(matrix)
        await asyncio.sleep(delay_factor * random())
    return all_identities 

# make a clean df out of all the IDs' matrices 
def identities_async(instrument_ids): 
    
    matrices = asyncio.run(get_instruments_identity(instrument_ids))
    
    updated_ids = pd.DataFrame()
    for matrix in matrices:
        inst_id = pd.DataFrame(
            matrix, 
            columns= ['attr', 'value'], 
            index= None)
        inst_id['isin'] = matrix[7][1]
        updated_ids = pd.concat([updated_ids, inst_id]) # make it a generator and yield IDs later and test
    updated_ids = updated_ids.pivot(index='isin', columns='attr', values='value')
    
    return updated_ids # if it keeps track of IDs in case of a break due to "disonnect from server",... 
# endregion 


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Instrument Daily OHLCV Values up to the current date ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#
def instrument_daily_quotes_history_up_to_date(arg):
    resp = requests.get(
        url=cfg['URI']['DAILY_PRICES_HISTROY_TO_DATE'].format(arg), 
        headers=request_headers, cookies=cookie_jar)
    resp.raise_for_status()
    
    logger_app.info(f'Arg: {arg}')
    
    if resp and resp.text:
        return pd.DataFrame(
        [price.split(',') for price in resp.text.split(';')], 
        columns=[
            'instrument_id', 
            'date', 
            'close', 
            'last', 
            'transaction_count', 
            'volume', 
            'value_traded', 
            'low', 
            'high', 
            'dday', 
            'open'])
    else:
        logger_app.info(f'{arg}: No Quote.')
