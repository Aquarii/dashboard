import requests
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging
from .utils import ar_to_fa
from tqdm import tqdm
# from lxml import html
# import json
# import pandas as pd


request_headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Referer':'http://main.tsetmc.com/StaticContent/WebServiceHelp'
    }
cookie_jar = {'ASP.NET_SessionId': 'wa40en1alwxzjnqehjntrv5j'}


#=============== Convert XML/HTML <table> to Matrix =================#
def xml_table_to_matrix(xml_table):
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


#================ Scrape Instrument Types ================#
def instrument_types():
    ws_instrument_text = requests.get(
        'http://cdn.tsetmc.com/api/StaticData/GetStaticContent/WS-Instrument', 
        headers= request_headers).text
    soup = BeautifulSoup(ws_instrument_text, 'lxml')
    table = soup.find_all('tbody')[3]
    return xml_table_to_matrix(table)


#=============== Scrape Instrument Attribs/Identity (شناسه) ===============#
# get identity page
async def get_identity_html(instrument_id: str) -> str:
    logging.info(f"Getting HTML for instrument {instrument_id}")
    
    url = f'http://www.tsetmc.com/Loader.aspx?Partree=15131M&i={instrument_id}'
    
    #sem = asyncio.BoundedSemaphore(25)
    
    #async with sem:
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=25)) as session:
        async with session.get(url, headers=request_headers) as resp:
            resp.raise_for_status()
            
            html = await resp.text()
            return html

# parse HTML and extract Table
def get_table_identity(html: str) -> str:
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find("table", class_='table1')
    if not table:
        return "MISSING"
    
    return xml_table_to_matrix(table)


# get identities
async def get_instruments_identity(instrument_ids):
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(get_identity_html(id)) for id in instrument_ids]
    
    all_identities = []
    for task in tqdm(tasks):
        html = await task
        all_identities.append(get_table_identity(html))
    return all_identities 


def get_identities_async(instrument_ids): 
    return asyncio.run(get_instruments_identity(instrument_ids))



#================= Get Instruments Attrib(Identity) Synchronous way ==============#
def identity(instrument_id):
    resp = requests.get(
        f'http://www.tsetmc.com/Loader.aspx?Partree=15131M&i={instrument_id}', 
        headers= request_headers)
    soup = BeautifulSoup(resp.text, 'lxml')
    table = soup.find("table", class_='table1')
    return xml_table_to_matrix(table)






''' Obsolete
#=============== Get the Last Trading Day (Update Date) ===============#  Obsolete
def last_workday(jalali=False):
    response = req.get('http://tsetmc.com/tsev2/data/TseClient2.aspx?t=LastPossibleDeven')
    last_workday = response.text.split(';')[0]  # Last Finished Workday
    print(f'آخرین روز کاری: {JalaliDate(datetime.strptime(last_workday,"%Y%m%d")).strftime("%x")}')
    print(f'({datetime.strptime(last_workday,"%Y%m%d").date()})')
    if jalali:
        return JalaliDate(last_workday)
    else:
        return last_workday


#=============== Get the List of All Valid Instruments ===================#  Obsolete
def instruments(last_fetch_date:int):
    response = req.get(f'http://tsetmc.com/tsev2/data/TseClient2.aspx?t=Instrument&a={last_fetch_date}')
    if len(response.text):
        tickers = response.text.split(';')
        print('Number of All Valid Instruments: ', len(tickers))
        instruments= pd.DataFrame([ticker.split(',') for ticker in tickers])
        instruments.columns= ['uid',
                            'instrument_id',
                            'code_5',
                            'name_latin',
                            'code_4',
                            'ticker',
                            'description',
                            'isin',
                            'date_x',
                            'market_code',
                            'company_name',
                            'status_code',
                            'instrument_group_code',
                            'market_type',
                            'tableu_code',
                            'industry_sector_code',
                            'industry_subsector_code',
                            'instrument_type_code']
        return instruments
    else:
        print('No New Instruments to Get.')


#=============== Get the List of Added Instruments + Capital Stock Increments ===================#  Obsolete
def instruments_and_shares(last_fetch_date, last_record_id):
    response = req.get(f'http://tsetmc.com/tsev2/data/TseClient2.aspx?t=InstrumentAndShare&a={last_fetch_date}&a2={last_record_id}')
    instruments = response.text.split('@')[0].split(';')
    share_changes = response.text.split('@')[1].split(';')
    print('Number of Instruments: ', len(instruments))
    print('Number of Capital Stocks Change: ', len(share_changes))
    print('dev note: if the number of instruments or shares are < 1 > check wether its empty.')
    instruments = pd.DataFrame([ticker.split(',') for ticker in instruments])
    instruments.columns = ['uid',
                           'instrument_id',
                           'code_5',
                           'name_latin',
                           'code_4',
                           'ticker',
                           'description',
                           'isin',
                           'date_x',
                           'market_code',
                           'company_name',
                           'status_code',
                           'instrument_group_code',
                           'market_type',
                           'tableu_code',
                           'industry_sector_code',
                           'industry_subsector_code',
                           'instrument_type_code']
    share_changes = pd.DataFrame([ticker.split(',') for ticker in share_changes])
    share_changes.columns = ['record_id',
                             'uid',
                             'date',
                             'before_change',
                             'after_change']
    return instruments, share_changes


#=============== DecompressAndGetInsturmentClosingPrice =================#  Obsolete




#=============== Get All the Instruments of IFB.ir ================#  Obsolete
def farabourse_instruments():
    ifb_resp = req.get(url='https://www.ifb.ir/ThirdMarket/AllUnderWrited.aspx',
                        headers={
                           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
                           'accept-language': 'en-US,en;q=0.9,fa;q=0.8'})
    tree = html.fromstring(html=ifb_resp.content)
    instruments = tree.xpath(
        '//select[@id="ContentPlaceHolder1_SymbolCombo"]/option')
    ifb_instruments = {instrument.text.encode('latin1').decode(
        'utf8'): instrument.values() for instrument in instruments}
    del ifb_instruments['همه']
    return pd.DataFrame(ifb_instruments)


'''