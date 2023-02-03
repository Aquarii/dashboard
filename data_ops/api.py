import zeep


#=============== Last Deven Possible (API)=================#
def last_possible_deven():
    client = zeep.Client('http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    last_workday = client.service.LastPossibleDeven()
    return last_workday.split(';')[0] if last_workday.split(';')[1] == last_workday.split(';')[0] else print(last_workday) # for debuging purposes

#=============== Instruments (API) =================#
def instruments(last_workday):
    client = zeep.Client(wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    return client.service.Instrument(last_workday)

#=============== Instruments and Shares (API) =================#
def instruments_and_shares(last_workday, last_record_id):
    client = zeep.Client(wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    return client.service.InstrumentAndShare(last_workday, last_record_id)

#=============== Decompress And Get Insturments Closing Prices (API) =================#
def decompress_closing_prices(instruments_compressed):
    client = zeep.Client(wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    return client.service.DecompressAndGetInsturmentClosingPrice(instruments_compressed)

#=============== Log Error (API) =================#
def log_error(error_str):
    client = zeep.Client(wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl')
    return client.service.LogError(error_str)
