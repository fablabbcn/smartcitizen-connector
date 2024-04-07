from smartcitizen_connector.models import (Device, HardwarePostprocessing, Metric, Postprocessing)
from smartcitizen_connector._config import config
from smartcitizen_connector.tools import logger, safe_get, tf, \
    convert_freq_to_rollup, clean, localise_date, url_checker, process_headers, get_alphasense, \
    get_pt_temp, find_by_field, dict_fmerge
from typing import Optional, List, Dict
from requests import get, post, patch
from aiohttp_retry import RetryClient, ExponentialRetry
from pandas import DataFrame, to_datetime
from datetime import datetime
from os import environ
from pydantic import TypeAdapter
import sys
import json
from math import isnan
from tqdm import trange
from json import dumps, JSONEncoder, loads
import aiohttp
import asyncio
import time

# numpy to json encoder to avoid convertion issues. borrowed from
# https://stackoverflow.com/questions/50916422/python-typeerror-object-of-type-int64-is-not-json-serializable#50916741
class NpEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, integer):
            return int(obj)
        if isinstance(obj, floating):
            return float(obj)
        if isinstance(obj, ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def check_blueprint(blueprint_url):
    if blueprint_url is None or not blueprint_url:
        logger.info('No blueprint url')
        return None
    if url_checker(blueprint_url):
        _blueprint = safe_get(blueprint_url).json()
    else:
        logger.info(f'No valid blueprint in url')
        _blueprint = None
    return _blueprint

def check_postprocessing(postprocessing):
    # Postprocessing should be dict or Postprocessing Model
    if postprocessing is None:
        return None, None, False

    if type(postprocessing) == dict:
        _postprocessing = TypeAdapter(Postprocessing).validate_python(postprocessing)
    else:
        _postprocessing = postprocessing

    # Check the url in hardware
    urls = url_checker(_postprocessing.hardware_url)
    # If URL is empty, try prepending base url from config
    if not urls:
        tentative_url = f"{config.BASE_POSTPROCESSING_URL}hardware/{_postprocessing.hardware_url}.json"
    else:
        if len(urls)>1: logger('URLs for postprocessing recipe are more than one, trying first', 'WARNING')
        tentative_url = urls[0]

    logger.info(f'Device has postprocessing information:\n{_postprocessing}')

    _ok = True
    # Make hardware postprocessing
    if url_checker(tentative_url):
        try:
            r = safe_get(tentative_url)
        except:
            _ok = False
            _hardware_postprocessing = None
            pass
    else:
        _hardware_postprocessing = None
        _ok = False

    if _ok:
        _hardware_url = tentative_url
        try:
            _hardware_postprocessing = TypeAdapter(HardwarePostprocessing).validate_python(r.json())
        except:
            _ok = False
            _hardware_postprocessing = None
            pass
    else:
        _hardware_url = ''

    return _hardware_url, _hardware_postprocessing, _ok

class SCDevice:
    id: int
    url: str
    page: str
    timezone: str
    json: Device
    data: DataFrame

    def __init__(self, id, check_postprocessing=True):
        self.id = id
        self.url = f'{config.DEVICES_URL}{self.id}'
        self.page = f'{config.FRONTEND_URL}{self.id}'
        self.method = 'async'
        self._metrics: List[Metric] = []
        self.__load__()
        self.__get_timezone__()
        if check_postprocessing:
            self.__check_postprocessing__()
            self._filled_properties = list()
            self._properties = dict()
            if self.__check_blueprint__():
                if self.__get_metrics__():
                    # TODO Improve how this happens automatically
                    self._filled_properties.append('metrics')
                self.__make_properties__()
        else:
            self._metrics = []
        logger.info(f'Device {self.json.id} initialized')

    def __load__(self):
        r = safe_get(self.url)
        # TODO assess if one day SCDevice can inherit directly from Device
        self.json = TypeAdapter(Device).validate_python(r.json())

    def __get_timezone__(self) -> str:

        if self.json.location.latitude is not None and self.json.location.longitude is not None:
            self.timezone = tf.timezone_at(lng=self.json.location.longitude, lat=self.json.location.latitude)

        logger.info('Device {} timezone is {}'.format(self.id, self.timezone))

        return self.timezone

    def __check_blueprint__(self):
        self._blueprint = check_blueprint(self.blueprint_url)

        return self._blueprint is not None

    def __check_postprocessing__(self):
        logger.info(f'Checking postprocessing of {self.id}')
        if self.json.postprocessing is not None:
            self.json.postprocessing.hardware_url, self._hardware_postprocessing, valid = check_postprocessing(self.json.postprocessing)
        else:
            self._hardware_postprocessing = None
            logger.warning('No postprocessing information')

    def __get_metrics__(self):
        self._metrics = TypeAdapter(List[Metric]).validate_python([y for y in self._blueprint['metrics']])

        # Convert that to metrics now
        if self._hardware_postprocessing is not None:
            for version in self._hardware_postprocessing.versions:
                if version.from_date is not None:
                    if version.from_date > self.last_reading_at:
                        logger.warning('Postprocessing from_date is later than device last_reading_at. Skipping')
                        continue

                for slot in version.ids:
                    metrics = None
                    if slot.startswith('AS'):
                        metric = get_alphasense(slot, version.ids[slot])
                    elif slot.startswith('PT'):
                        metric = get_pt_temp(slot, version.ids[slot])
                    for m in metric:
                        for key, value in m.items():
                            item = find_by_field(self._metrics, key, 'name')
                            if item is None:
                                logger.warning(f'Item not found, {item[0]}')
                                continue
                            item.kwargs = dict_fmerge(item.kwargs, value['kwargs'])
            return True

    def __make_properties__(self):
        for item, value in self._blueprint.items():
            if item in self._filled_properties:
                self._properties[item] = self.__getattribute__(item)
            else:
                self._properties[item] = value

    async def get_datum(self, semaphore, session, url, headers, sensor_id, resample, frequency, rename)->Dict:
        async with semaphore:
            retry_client = RetryClient(client_session=session)
            async with retry_client.get(url, headers = headers) as resp:

                datum = await resp.json()
                sensor_name = find_by_field(self.json.data.sensors, sensor_id, 'id').name

                if 'readings' not in datum:
                    logger.warning(f"Device: {self.json.id}- No readings in request for sensor: {sensor_id}: {sensor_name}")
                    logger.warning(f"Response code: {resp}")
                    return None

                if datum['readings'] == []:
                    logger.warning(f"Device: {self.json.id} - No data in request for sensor: {sensor_id}: {sensor_name}")
                    return None

                logger.info(f"Device: {self.json.id} - Got readings for sensor: {sensor_id}: {sensor_name}")

                # Make a Dataframe
                # Set index
                df_sensor = DataFrame(datum['readings']).set_index(0)
                # Set columns
                if rename:
                    df_sensor.columns = [sensor_name]
                else:
                    df_sensor.columns = [str(sensor_id)]
                # Localise index
                df_sensor.index = localise_date(df_sensor.index, self.timezone)
                # Sort it just in case
                df_sensor.sort_index(inplace=True)
                # Remove duplicates
                df_sensor = df_sensor[~df_sensor.index.duplicated(keep='first')]
                # Drop unnecessary columns
                df_sensor.drop([i for i in df_sensor.columns if 'Unnamed' in i], axis=1, inplace=True)
                # Check for weird things in the data
                df_sensor = df_sensor.astype(float, errors='ignore')
                # Resample
                if (resample):
                    df_sensor = df_sensor.resample(frequency).mean()

                return df_sensor

    async def get_data(self,
        min_date: Optional[datetime] = None,
        max_date: Optional[datetime] = None,
        frequency: Optional[str] = '1Min',
        clean_na: Optional[str] = None,
        resample: Optional[bool] = False,
        rename: Optional[bool] = True)->DataFrame:

        if 'SC_ADMIN_BEARER' in environ:
            logger.info('Admin Bearer found, using it')
            headers = {'Authorization':'Bearer ' + environ['SC_ADMIN_BEARER']}
        else:
            logger.warning('Admin Bearer not found')
            headers = None

        logger.info(f'Make sure we are up to date')
        self.__load__()
        logger.info(f'Requesting data from SC API')
        logger.info(f'Device ID: {self.id}')
        rollup = convert_freq_to_rollup(frequency)
        logger.info(f'Using rollup: {rollup}')

        if self.timezone is None:
            logger.warning('Device does not have timezone set, skipping')
            return self.data

        # Check start date and end date
        # Converting to UTC by passing None
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.tz_convert.html
        if min_date is not None:
            min_date = localise_date(to_datetime(min_date), 'UTC')
            logger.info(f'Min Date: {min_date}')
        else:
            min_date = localise_date(to_datetime('2001-01-01'), 'UTC')
            logger.info("No min_date specified")

        if max_date is not None:
            max_date = localise_date(to_datetime(max_date), 'UTC')
            logger.info(f'Max Date: {max_date}')

        # Trim based on actual data available
        if min_date is not None and self.json.last_reading_at is not None:
            if min_date > self.json.last_reading_at:
                logger.warning('Device request would yield empty data (min_date). Returning')
                return self.data

        if max_date is not None and self.json.created_at is not None:
            if max_date < self.json.created_at:
                logger.warning('Device request would yield empty data (max_date). Returning')
                return self.data

        if max_date is not None and self.json.last_reading_at is not None:
            if max_date > self.json.last_reading_at:
                logger.warning('Trimming max_date to last reading')
                max_date = self.json.last_reading_at

        if self.json.kit is not None:
            logger.info(f'Kit ID: {self.json.kit.id}')
        logger.info(f'Device timezone: {self.timezone}')

        if not self.json.data.sensors:
            logger.info('Device is empty')
            return self.data
        else: logger.info(f"Sensor IDs: {[f'{sensor.name}: {sensor.id}' for sensor in self.json.data.sensors]}")

        df = DataFrame()
        logger.info(f'Requesting from {min_date} to {max_date}')

        semaphore = asyncio.Semaphore(config._max_concurrent_requests)
        async with aiohttp.ClientSession() as session:

            tasks = []
            for sensor in self.json.data.sensors:

                # Request sensor per ID
                url = self.url + '/readings?'

                if min_date is not None: url += f'from={min_date}'
                if max_date is not None: url += f'&to={max_date}'

                url += f'&rollup={rollup}'
                url += f'&sensor_id={sensor.id}'
                url += '&function=avg'

                tasks.append(asyncio.ensure_future(self.get_datum(semaphore, session, url, headers, sensor.id, resample, frequency, rename)))

            dfs_sensor = await asyncio.gather(*tasks)

        # Process received data
        for df_sensor in dfs_sensor:
            if df_sensor is None: continue
            # Combine in the main df
            df = df.combine_first(df_sensor)

        try:
            df = df.reindex(df.index.rename('TIME'))
            df = clean(df, clean_na, how = 'all')
            self.data = df
        except:
            logger.error('Problem closing up the API dataframe')
            pass

        logger.info(f'Device {self.id} loaded successfully from API')
        return self.data

    async def post_data(self, columns = 'sensors', rename = None, clean_na = 'drop', chunk_size = 500, dry_run = False, max_retries = 2, delay = None):
        '''
            POST self.data in the SmartCitizen API
            Parameters
            ----------
                columns: List or string
                    'sensors'
                    If string, either 'sensors' or 'metrics. Empty string is 'sensors' + 'metrics'
                    If list, list containing column names.
                clean_na: string, optional
                    'drop'
                    'drop', 'fill'
                chunk_size: integer
                    chunk size to split resulting pandas DataFrame for posting readings
                dry_run: boolean
                    False
                    Post the payload to the API or just return it
                max_retries: int
                    2
                    Maximum number of retries per chunk
            Returns
            -------
                True if the data was posted succesfully
        '''

        if self.data is None:
            logger.error('No data to post, ignoring')
            return False

        if 'SC_BEARER' not in environ:
            logger.error('Cannot post without Auth Bearer')
            return False

        if 'SC_ADMIN_BEARER' in environ:
            logger.info('Using admin Bearer')
            bearer = environ['SC_ADMIN_BEARER']
        else:
            bearer = environ['SC_BEARER']

        headers = {'Authorization':'Bearer ' + bearer, 'Content-type': 'application/json'}
        post_ok = True

        if columns == 'sensors':
            _columns = self.json.data.sensors
            # TODO - when a device has been processed, data will be there for metrics
        elif columns == 'metrics':
            _columns = self._metrics
        elif type(columns) is list:
            _columns = list()
            for column in columns:
                item = find_by_field(self.json.data.sensors + self._metrics, column, 'name')
                if item is not None:
                    _columns.append(item)
        else:
            _columns = self.json.data.sensors + self._metrics

        if rename is None:
            logger.info('Renaming not required')
            _rename = dict()
            for column in _columns:
                _rename[column.name] = column.name
        else:
            _rename = rename

        async with aiohttp.ClientSession() as session:

            tasks = []
            for column in _columns:
                if _rename[column.name] not in self.data:
                    logger.warning(f'{_rename[column.name]} not in data')
                    continue
                if column.id is None:
                    logger.warning(f'{column.name} has no id')
                    continue
                # Get only post data
                df = DataFrame(self.data[_rename[column.name]]).copy()
                # Rename to ID to be able to post
                logger.info(f'Adding {_rename[column.name]} ({column.id}) to post list')
                df.rename(columns={_rename[column.name]: column.id}, inplace = True)
                url = f'{self.url}/readings'
                # Append task
                tasks.append(asyncio.ensure_future(self.post_datum(session, headers, url, df,
                    clean_na = clean_na, chunk_size = chunk_size, dry_run = dry_run,
                    max_retries = max_retries, delay=delay)))

            posts_ok = await asyncio.gather(*tasks)

        return not(False in posts_ok)

    async def post_datum(self, session, headers, url, df, clean_na = 'drop', chunk_size = 500, dry_run = False, max_retries = 2, delay = None):
        '''
            POST external pandas.DataFrame to the SmartCitizen API
            Parameters
            ----------
                session: aiohttp.ClientSession
                headers: dict
                    Auth headers
                df: pandas DataFrame
                    Contains data in a DataFrame format.
                    Data is posted using the column names of the dataframe
                    Data is posted in UTC TZ so dataframe needs to have located
                    timestamp
                clean_na: string, optional
                    'drop'
                    'drop', 'fill'
                chunk_size: integer
                    chunk size to split resulting pandas DataFrame for posting readings
                dry_run: boolean
                    False
                    Post the payload to the API or just return it
                max_retries: int
                    2
                    Maximum number of retries per chunk
            Returns
            -------
                True if the data was posted succesfully
        '''
        # Clean df of nans
        df = clean(df, clean_na, how = 'all')
        logger.info(f'Posting to {url}')
        logger.info(f'Sensor ID: {list(df.columns)[0]}')
        df.index.name = 'recorded_at'

        # Split the dataframe in chunks
        chunked_dfs = [df[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]
        if len(chunked_dfs) > 1: logger.info(f'Splitting post in chunks of size {chunk_size}')

        for i in trange(len(chunked_dfs), file=sys.stdout,
                        desc=f"Posting data for {self.id}..."):

            chunk = chunked_dfs[i].copy()

            # Prepare json post
            payload = {"data":[]}
            for item in chunk.index:
                payload["data"].append(
                    {
                        "recorded_at": localise_date(item, 'UTC').strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "sensors": [{
                            "id": column,
                            "value": chunk.loc[item, column]
                        } for column in chunk.columns if not isnan(chunk.loc[item, column])]
                    }
                )

            if dry_run:
                logger.info(f'Dry run request to: {self.url}/readings for chunk ({i+1}/{len(chunked_dfs)})')
                jsd = dumps(payload, indent = 2, cls = NpEncoder)
                logger.debug(jsd)
                return jsd

            post_ok = False
            retries = 0

            while post_ok == False and retries < max_retries:
                if delay is not None: time.sleep(delay)
                response = post(url, data = dumps(payload, cls = NpEncoder), headers = headers)

                if response.status_code == 200 or response.status_code == 201:
                    post_ok = True
                    break
                else:
                    retries += 1
                    logger.warning (f'Chunk ({i+1}/{len(chunked_dfs)}) post failed. \
                            API responded {response.status_code}.\
                            Retrying ({retries}/{max_retries}')

            if (not post_ok) or (retries == max_retries):
                logger.error (f'Chunk ({i+1}/{len(chunked_dfs)}) post failed. \
                        API responded {response.status_code}.\
                        Reached max_retries')
                return False

        return True

    def patch_postprocessing(self, dry_run = False):
        '''
            PATCH postprocessing info into the device in the SmartCitizen API
            Updates all the post info. Changes need to be made info the keys of the postprocessing outside of here

            # Example postprocessing:
            # {
            #   "blueprint_url": "https://github.com/fablabbcn/smartcitizen-data/blob/master/blueprints/sc_21_station_module.json",
            #   "hardware_url": "https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/hardware/SCAS210001.json",
            #   "latest_postprocessing": "2020-10-29T08:35:23Z"
            # }
        '''

        if 'SC_ADMIN_BEARER' not in environ:
            logger.error('Cannot post without Admin Auth Bearer')
            return

        headers = {'Authorization':'Bearer ' + environ['SC_ADMIN_BEARER'],
                   'Content-type': 'application/json'}

        post = {"postprocessing_attributes": loads(self.json.postprocessing.model_dump_json())}

        if dry_run:
            logger.info(f'Dry run request to: {self.url}/')
            return dumps(post)

        logger.info(f'Posting postprocessing_attributes:\n {post} to {self.url}')
        response = patch(f'{self.url}/',
                         data = dumps(post), headers = headers)

        if response.status_code == 200 or response.status_code == 201:
            logger.info(f"Postprocessing posted")
            return True
        else:
            logger.info(f"API responded with {response.status_code}")

        return False

    @property
    def blueprint_url(self):
        if self.json.postprocessing is None:
            return None
        if url_checker(self.json.postprocessing.blueprint_url):
            return self.json.postprocessing.blueprint_url
        elif url_checker(self.json.postprocessing.hardware_url):
            return self._hardware_postprocessing.blueprint_url
        else:
            return None

    @property
    def blueprint(self):
        return self._blueprint

    @property
    def properties(self):
        return self._properties

    @property
    def metrics(self):
        return [metric.model_dump() for metric in self._metrics]

    @property
    def sensors(self):
        return [sensor.model_dump() for sensor in self.json.data.sensors]

    @property
    def hardware_postprocessing(self):
        if self._hardware_postprocessing is None:
            return None
        return self._hardware_postprocessing.model_dump()

    @property
    def postprocessing(self):
        if self.json.postprocessing is None:
            return None
        return self.json.postprocessing.model_dump()

    @property
    def latest_postprocessing(self):
        return self.json.postprocessing.latest_postprocessing

    def update_latest_postprocessing(self, date):
        if self.json.postprocessing.id is not None:
            # Add latest postprocessing rounded up with
            # frequency so that we don't end up in
            # and endless loop processing only the latest data line
            # (minute vs. second precission of the data)
            try:
                self.json.postprocessing.latest_postprocessing = date.to_pydatetime()
            except:
                return False
            else:
                logger.info(f"Updated latest_postprocessing to: {self.latest_postprocessing}")
                return True
        logger.info('Nothing to update')
        return True

    @property
    def last_reading_at(self):
        return self.json.last_reading_at

def get_devices():
    isn = True
    result = list()
    url = config.DEVICES_URL
    while isn:
        r = get(url)
        r.raise_for_status()
        # If status code OK, retrieve data
        h = process_headers(r.headers)
        result += TypeAdapter(List[Device]).validate_python(r.json())

        if 'next' in h:
            if h['next'] == url: isn = False
            elif h['next'] != url: url = h['next']
        else:
            isn = False
    return result
