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

rollup_2_freq_lut = (
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
        df: pandas.           The o clean
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

def convert_rollup_to_freq(rollup):
    # Convert frequency from pandas to API's
    for index, letter in enumerate(rollup):
        try:
            aux = int(letter)
        except:
            index_first = index
            letter_first = letter
            frequency_value = rollup[:index_first]
            rollup_unit = rollup[index_first:]
            break

    for item in rollup_2_freq_lut:
        if item[1] == rollup_unit:
            frequency_unit = item[0]
            break

    frequency = frequency_value + frequency_unit
    return frequency

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