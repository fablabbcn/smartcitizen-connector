from smartcitizen_connector._config import config
from smartcitizen_connector.tools import *
from typing import Optional
from pandas import DataFrame
from os import environ
from requests import get

def global_search(value: Optional[str] = None) -> DataFrame:
    """
    Gets devices from Smart Citizen API based on basic search query values,
    searching both Users and Devices at the same time.
    Global search documentation: https://developer.smartcitizen.me/#global-search
    Parameters
    ----------
        value: string
            None
            Query to fit
            For null, not_null values, use 'null' or 'not_null'
    Returns
    -------
        A list of kit IDs that comply with the requirements, or the full df, depending on full.
    """
    if 'SC_ADMIN_BEARER' in environ:
        logger.info('Admin Bearer found, using it')
        headers = {'Authorization':'Bearer ' + environ['SC_ADMIN_BEARER']}
    else:
        headers = None
        logger.info('Admin Bearer not found')

    # Value check
    if value is None: logger.error(f'Value needs a value, {value} supplied'); return None

    url = config.API_SEARCH_URL  + f'{value}'

    df = DataFrame()
    isn = True
    while isn:
        r = get(url, headers = headers)
        r.raise_for_status()
        # If status code OK, retrieve data
        h = process_headers(r.headers)
        df = df.combine_first(DataFrame(r.json()).set_index('id'))

        if 'next' in h:
            if h['next'] == url: isn = False
            elif h['next'] != url: url = h['next']
        else:
            isn = False

    return df

def search_by_query(endpoint: Optional[str] = 'devices',
    key: Optional[str] = '',
    search_matcher: Optional[str] = '',
    value: Optional[str] = None) -> DataFrame:
    """
    Gets devices from Smart Citizen API based on ransack parameters
    Basic query documentation: https://developer.smartcitizen.me/#basic-searching
    Parameters
    ----------
        endpoint: string
            'devices'
            Endpoint to perform the query at (see docs)
        key: string
            ''
            Query key according to the basic query documentation.
        search_matcher: string
            ''
            Ransack search_matcher:
            https://activerecord-hackery.github.io/ransack/getting-started/search-matches/
        value: string
            None
            Query to fit
            For null, not_null values, use 'null' or 'not_null'. In this case ignores search_matcher
    Returns
    -------
        DataFrame with devices
    """

    if 'SC_ADMIN_BEARER' in environ:
        logger.info('Admin Bearer found, using it')
        headers = {'Authorization':'Bearer ' + environ['SC_ADMIN_BEARER']}
    else:
        headers = None
        logger.info('Admin Bearer not found')

    # Value check
    if value is None:
        logger.error(f'Value needs a value, {value} supplied')
        return None

    if value == 'null' or value == 'not_null':
         url = f'{config.API_URL}{endpoint}/?q[{key}_{value}]=1'
    else:
         url = f'{config.API_URL}{endpoint}/?q[{key}_{search_matcher}]={value}'

    df = DataFrame()
    isn = True
    logger.info(f'Getting: {url}')
    while isn:
        r = get(url, headers = headers)
        r.raise_for_status()
        # If status code OK, retrieve data
        h = process_headers(r.headers)
        if r.json() == []: return None
        df = df.combine_first(DataFrame(r.json()).set_index('id'))

        if 'next' in h:
            if h['next'] == url: isn = False
            elif h['next'] != url: url = h['next']
        else:
            isn = False

    return df