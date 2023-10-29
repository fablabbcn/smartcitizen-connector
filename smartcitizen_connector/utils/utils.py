from pandas import to_datetime
from timezonefinder import TimezoneFinder
from datetime import datetime
from smartcitizen_connector.config import *
from typing import Optional

tf = TimezoneFinder()

# Convertion between SC and Pandas API rollups
rollup_table = {
    "y":   "years",
    "M":   "months",
    "w":   "weeks",
    "d":   "days",
    "h":   "hours",
    "m":   "minutes",
    "s":   "seconds",
    "ms":  "milliseconds"
}

freq_2_rollup_lut = (
    ['A', 'y'],
    ['M', 'M'],
    ['W', 'w'],
    ['D', 'd'],
    ['H', 'h'],
    ['Min', 'm'],
    ['S', 's'],
    ['ms', 'ms']
)

def clean(df, clean_na = None, how = 'all'):
    """
    Helper function for cleaning nan in a pandas.   Parameters
    ----------
        df: pandas.DataFrame to clean
        clean_na: None or string
            type of nan cleaning. If not None, can be 'drop' or 'fill'
        how: 'string'
            Same as how in dropna, fillna. Can be 'any', or 'all'
    Returns
    -------
        Clean dataframe
    """

    if clean_na is not None:
        if clean_na == 'drop':
            df.dropna(axis = 0, how = how, inplace = True)
        elif clean_na == 'fill':
            df = df.fillna(method = 'bfill').fillna(method = 'ffill')
    return df

def convert_freq_to_rollup(freq):
    """
    Helper function for converting a pandas freq into a rollup of SC API's
    ----------
        freq: str freq from pandas
    Returns
    -------
        rollup: str rollup from SC
    """
    # Convert freq from pandas to SC API's
    for index, letter in enumerate(freq):
        try:
            aux = int(letter)
        except:
            index_first = index
            letter_first = letter
            rollup_value = freq[:index_first]
            freq_unit = freq[index_first:]
            break

    for item in freq_2_rollup_lut:
        if item[0] == freq_unit:
            rollup_unit = item[1]
            break

    rollup = rollup_value + rollup_unit
    return rollup

def localise_date(date, timezone, tzaware=True):
    """
    Localises a date if it's tzinfo is None, otherwise converts it to it.
    If the timestamp is tz-aware, converts it as well
    Parameters
    ----------
        date: string or datetime
            Date
        timezone: string
            Timezone string. i.e.: 'Europe/Madrid'
    Returns
    -------
        The date converted to 'UTC' and localised based on the timezone
    """
    if date is not None:
        # Per default, we consider that timestamps are tz-aware or UTC.
        # If not, preprocessing should be done to get there
        result_date = to_datetime(date, utc = tzaware)
        if result_date.tzinfo is not None:
            result_date = result_date.tz_convert(timezone)
        else:
            result_date = result_date.tz_localize(timezone)
    else:
        result_date = None

    return result_date

def std_out(msg: str,
    mtype: Optional[str] = None,
    force: Optional[bool] = False
    ):

    if out_timestamp == True:
        stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    else:
        stamp = ''
    # Output levels:
    # 'QUIET': nothing,
    # 'NORMAL': warn, err
    # 'DEBUG': info, warn, err, success
    if force == True: priority = 2
    elif out_level == 'QUIET': priority = 0
    elif out_level == 'NORMAL': priority = 1
    elif out_level == 'DEBUG': priority = 2

    if mtype is None and priority>1:
        print(f'[{stamp}] - ' + '[INFO] ' + msg)
    elif mtype == 'SUCCESS' and priority>0:
        print(f'[{stamp}] - ' + '[SUCCESS] ' + msg)
    elif mtype == 'WARNING' and priority>0:
        print(f'[{stamp}] - ' + '[WARNING] ' + msg)
    elif mtype == 'ERROR' and priority>0:
        print(f'[{stamp}] - ' + '[ERROR] ' + msg)

import re

''' Directly from
https://www.geeksforgeeks.org/python-check-url-string/
'''

def url_checker(string):
    if string is not None:
        regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
        url = re.findall(regex,string)
        return [x[0] for x in url]
    else:
        return []

def process_headers(headers):
    result = {}
    if 'total' in headers: result['total_pages'] = headers['total']
    if 'per-page' in headers: result['per_page'] = headers['per-page']
    if 'link' in headers:
        for item in headers.get('link').split(','):
            chunk = item.replace(' ', '').split(';')
            if 'rel' in chunk[1]:
                which = chunk[1].replace('"', '').split('=')[1]
                if which == 'next':
                    result['next'] = chunk[0].strip('<').strip('>')
                elif which == 'last':
                    result['last'] = chunk[0].strip('<').strip('>')
                elif which == 'prev':
                    result['prev'] = chunk[0].strip('<').strip('>')
                elif which == 'first':
                    result['first'] = chunk[0].strip('<').strip('>')
    return result