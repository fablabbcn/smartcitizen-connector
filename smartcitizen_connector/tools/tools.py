from pandas import to_datetime
from timezonefinder import TimezoneFinder
from datetime import datetime
from smartcitizen_connector._config import config
from typing import Optional
from termcolor import colored
from requests.exceptions import HTTPError
from requests import get
import re
import logging
import sys

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

class CutsomLoggingFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_min = "[%(asctime)s] - %(name)s - %(levelname)s - %(message)s"
    format_deb = "[%(asctime)s] - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format_min + reset,
        logging.INFO: grey + format_min + reset,
        logging.WARNING: yellow + format_min + reset,
        logging.ERROR: red + format_deb + reset,
        logging.CRITICAL: bold_red + format_deb + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logger = logging.getLogger('smartcitizen_connector')
logger.setLevel(config.log_level)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(config.log_level)
ch.setFormatter(CutsomLoggingFormatter())
logger.addHandler(ch)

def set_logger_level(level=logging.DEBUG):
    logger.setLevel(level)

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

def safe_get(url):
    for n in range(config._max_retries):
        try:
            r = get(url)
            r.raise_for_status()
        except HTTPError as exc:
            code = exc.response.status_code

            if code in config._retry_codes:
                time.sleep(config._retry_interval)
                continue

            raise
    return r

def get_alphasense(slot, sensor_id):
    result = list()

    # Alphasense type - AAN 803-04
    as_type = config._as_sensor_codes[sensor_id[0:3]]
    channel = as_type[as_type.index('_')+1:]
    metric = channel
    if channel == 'OX':
        metric = 'O3'

    # Get working and auxiliary electrode names
    wen = f"ADC_{slot.strip('AS_')[:slot.index('_')]}_{slot.strip('AS_')[slot.index('_')+1]}"
    aen = f"ADC_{slot.strip('AS_')[:slot.index('_')]}_{slot.strip('AS_')[slot.index('_')+2]}"

    # Simply fill it up
    logger.info(f'{metric} found in blueprint metrics, filling up with hardware info')

    result.append({metric: {
        'kwargs': {
            'we': wen,
            'ae': aen,
            't': 'EC_SENSOR_TEMP',
            'alphasense_id': str(sensor_id)}}})

    # Add channel name for traceability
    result.append({f'{channel}_WE': {'kwargs': {'channel': wen}}})
    result.append({f'{channel}_AE': {'kwargs': {'channel': aen}}})

    return result

def get_pt_temp(slot, sensor_id):
    result = list()

    # Get working and auxiliary electrode names
    pt1000plus = f"ADC_{slot.strip('PT_')[:slot.index('_')]}_{slot.strip('PT_')[slot.index('_')+1]}"
    pt1000minus = f"ADC_{slot.strip('PT_')[:slot.index('_')]}_{slot.strip('PT_')[slot.index('_')+2]}"

    metric = 'ASPT1000'

    # Simply fill it up
    logger.info(f'{metric} found in blueprint metrics, filling up with hardware info')

    result.append({metric: {
        'kwargs': {
            'pt1000plus': pt1000plus,
            'pt1000minus': pt1000minus,
            'afe_id': str(sensor_id)}}})

    # Add channel name for traceability
    result.append({f'PT1000_POS': {'kwargs': {'channel': pt1000plus}}})

    return result

def find_by_field(models, value, field):
    try:
        item = next(model for _, model in enumerate(models) if model.__getattribute__(field) == value)
    except StopIteration:
        logger.exception(f'Column {field} not in models')
        pass
    else:
        return item
    return None

def dict_fmerge(base_dct, merge_dct, add_keys=True):
    """
    Recursive dict merge.
    From: https://gist.github.com/CMeza99/5eae3af0776bef32f945f34428669437
    Parameters
    ----------
        base_dct: dict
            Dict onto which the merge is executed
        merge_dct: dict
            Dict merged into base_dct
        add_keys: bool
            True
            Whether to add new keys
    Returns
    -------
        Updated dict
    """
    rtn_dct = base_dct.copy()
    if add_keys is False:
        merge_dct = {key: merge_dct[key] for key in set(rtn_dct).intersection(set(merge_dct))}

    rtn_dct.update({
        key: dict_fmerge(rtn_dct[key], merge_dct[key], add_keys=add_keys)
        if isinstance(rtn_dct.get(key), dict) and isinstance(merge_dct[key], dict)
        else merge_dct[key]
        for key in merge_dct.keys()
    })

    return rtn_dct
