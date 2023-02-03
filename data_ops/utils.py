
def ar_to_fa(string: str):
    return string.replace('ك','ک').replace('ي', 'ی').replace('\u200c',' ').replace('_',' ').strip()


def fa_to_ar(string: str):
    return string.replace('ک', 'ك').replace('ی', 'ي').replace('\u200c',' ').replace('_',' ').strip()
