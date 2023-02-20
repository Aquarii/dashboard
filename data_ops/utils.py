
def ar_to_fa(string: str):
    return string.replace('ك','ک').replace('ي', 'ی').replace('\u200c',' ').replace('_',' ').strip()


def fa_to_ar(string: str):
    return string.replace('ک', 'ك').replace('ی', 'ي').replace('\u200c',' ').replace('_',' ').strip()


def ar_to_fa_series(series):
    return series.str.replace('ك','ک').str.replace('ي', 'ی').str.replace('\u200c',' ').str.replace('_',' ').str.strip()


def fa_to_ar_series(series):
    return series.str.replace('ک','ك').str.replace('ی', 'ي').str.replace('\u200c',' ').str.replace('_',' ').str.strip()