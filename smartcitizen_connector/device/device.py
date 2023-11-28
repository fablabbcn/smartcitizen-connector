from smartcitizen_connector.models import (Sensor, Kit, Owner, Location, Data, Device, HardwarePostprocessing, Measurement, Postprocessing, HardwareInfo)
from smartcitizen_connector.config import *
from smartcitizen_connector.utils import *
from typing import Optional, List, Dict
from requests import get, post, patch
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

class SCDevice:
    id: int
    url: str
    page: str
    timezone: str
    json: Device
    data: DataFrame

    def __init__(self, id):
        self.id = id
        self.url = f'{DEVICES_URL}{self.id}'
        self.page = f'{FRONTEND_URL}{self.id}'
        self.method = 'async'
        self._metrics = list()
        r = self.__safe_get__(self.url)
        self.json = TypeAdapter(Device).validate_python(r.json())
        self.__get_timezone__()
        self.__check_postprocessing__()

    def __safe_get__(self, url):
        r = get(url)
        r.raise_for_status()

        return r

    def __get_timezone__(self) -> str:

        if self.json.data.location.latitude is not None and self.json.data.location.longitude is not None:
            self.timezone = tf.timezone_at(lng=self.json.data.location.longitude, lat=self.json.data.location.latitude)

        std_out ('Device {} timezone is {}'.format(self.id, self.timezone))

        return self.timezone

    def __check_postprocessing__(self) -> dict:

        if self.json.postprocessing is not None:
            # Check the url in hardware
            urls = url_checker(self.json.postprocessing.hardware_url)
            # If URL is empty, try prepending base url from config
            if not urls:
                tentative_url = f"{BASE_POSTPROCESSING_URL}hardware/{self.json.postprocessing.hardware_url}.json"
            else:
                if len(urls)>1: std_out('URLs for postprocessing recipe are more than one, trying first', 'WARNING')
                tentative_url = urls[0]

            self.json.postprocessing.hardware_url = tentative_url

            std_out (f'Device {self.id} has postprocessing information:\n{self.json.postprocessing}')

            # Make hardware postprocessing
            if url_checker(self.json.postprocessing.hardware_url):
                r = self.__safe_get__(self.json.postprocessing.hardware_url)
                self._hardware_postprocessing = TypeAdapter(HardwarePostprocessing).validate_python(r.json())

            # Convert that to metrics now
            if self._hardware_postprocessing is not None:
                for version in self._hardware_postprocessing.versions:
                    if version.from_date is not None:
                        if version.from_date > self.last_reading_at:
                            std_out('Postprocessing from_date is later than device last_reading_at. Skipping', 'ERROR')
                            continue

                    for slot in version.ids:
                        metrics = None
                        if slot.startswith('AS'):
                            metric = get_alphasense(slot, version.ids[slot])
                        elif slot.startswith('PT'):
                            metric = get_pt_temp(slot, version.ids[slot])
                        [self._metrics.append(m) for m in metric]
        else:
            std_out (f'Device {self.id} has no postprocessing information')

    async def get_datum(self, session, url, headers, sensor_id)->Dict:
        async with session.get(url, headers = headers) as resp:
            data = await resp.json()

            if data['readings'] == []:
                std_out(f'No data in request for sensor: {sensor_id}', 'WARNING')
                return None

            return {sensor_id: data}

    async def get_data(self,
        min_date: Optional[datetime] = None,
        max_date: Optional[datetime] = None,
        freq: Optional[str] = '1Min',
        clean_na: Optional[str] = None,
        resample: Optional[bool] = False,
        only_unprocessed: Optional[bool] = False)->DataFrame:

        if 'SC_ADMIN_BEARER' in environ:
            std_out('Admin Bearer found, using it', 'SUCCESS')

            headers = {'Authorization':'Bearer ' + environ['SC_ADMIN_BEARER']}
        else:
            headers = None
            std_out('Admin Bearer not found', 'WARNING')

        std_out(f'Requesting data from SC API')
        std_out(f'Device ID: {self.id}')
        rollup = convert_freq_to_rollup(freq)
        std_out(f'Using rollup: {rollup}')

        if self.timezone is None:
            std_out('Device does not have timezone set, skipping', 'WARNING')
            return None

        # Check start date and end date
        # Converting to UTC by passing None
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.tz_convert.html
        if min_date is not None:
            min_date = localise_date(to_datetime(min_date), 'UTC')
            std_out (f'Min Date: {min_date}')
        else:
            min_date = localise_date(to_datetime('2001-01-01'), 'UTC')
            std_out(f"No min_date specified")

        if max_date is not None:
            max_date = localise_date(to_datetime(max_date), 'UTC')
            std_out (f'Max Date: {max_date}')

        # Trim based on actual data available
        if min_date is not None and self.json.last_reading_at is not None:
            if min_date > self.json.last_reading_at:
                std_out(f'Device request would yield empty data (min_date). Returning', 'WARNING')
                return None

        if max_date is not None and self.json.created_at is not None:
            if max_date < self.json.created_at:
                std_out(f'Device request would yield empty data (max_date). Returning', 'WARNING')
                return None

        if max_date is not None and self.json.last_reading_at is not None:
            if max_date > self.json.last_reading_at:
                std_out('Trimming max_date to last reading', 'WARNING')
                max_date = self.json.last_reading_at

        if self.json.kit is not None:
            std_out('Kit ID: {}'.format(self.json.kit.id))
        std_out(f'Device timezone: {self.timezone}')

        if not self.json.data.sensors:
            std_out(f'Device is empty')
            return None
        else: std_out(f"Sensor IDs: {[(sensor.id, sensor.name) for sensor in self.json.data.sensors]}")

        df = DataFrame()
        std_out(f'Requesting from {min_date} to {max_date}')

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

                tasks.append(asyncio.ensure_future(self.get_datum(session, url, headers, sensor.id)))

            data = await asyncio.gather(*tasks)
            sensors = self.json.data.sensors

            # Process received data
            for datum in data:
                if datum is None: continue
                sensor_id = list(datum.keys())[0]
                sensor_name = ''
                # Find the id of the sensor
                for sensor in sensors:
                    if sensor.id == sensor_id:
                        sensor_name = sensor.name
                        break

                # Set index
                df_sensor = DataFrame(datum[sensor_id]['readings']).set_index(0)
                # Set columns
                df_sensor.columns = [sensor_name]
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
                    df_sensor = df_sensor.resample(freq).mean()
                # Combine in the main df
                df = df.combine_first(df_sensor)

            try:
                df = df.reindex(df.index.rename('TIME'))
                df = clean(df, clean_na, how = 'all')
                self.data = df
            except:
                std_out('Problem closing up the API dataframe', 'ERROR')
                pass
                return None

            std_out(f'Device {self.id} loaded successfully from API', 'SUCCESS')
            return self.data

    async def post_data(self, clean_na = 'drop', chunk_size = 500, dry_run = False, max_retries = 2):
        '''
            POST self.data in the SmartCitizen API
            Parameters
            ----------
                clean_na: string, optional
                    'drop'
                    'drop', 'fill'
                chunk_size: integer
                    chunk size to split resulting pandas DataFrame for posting readings
            Returns
            -------
                True if the data was posted succesfully
        '''
        if self.data is None:
            std_out('No data to post, ignoring', 'ERROR')
            return False

        if 'SC_BEARER' not in environ:
            std_out('Cannot post without Auth Bearer', 'ERROR')
            return False

        if 'SC_ADMIN_BEARER' in environ:
            std_out('Using admin Bearer')
            bearer = environ['SC_ADMIN_BEARER']
        else:
            bearer = environ['SC_BEARER']

        headers = {'Authorization':'Bearer ' + bearer, 'Content-type': 'application/json'}
        post_ok = True

        async with aiohttp.ClientSession() as session:

            tasks = []
            for sensor in self.json.data.sensors:
                if sensor.name not in self.data:
                    std_out(f'No data in {sensor.id}', 'WARNING')
                    continue
                # Get only post data
                df = DataFrame(self.data[sensor.name]).copy()
                # Rename to ID to be able to post
                df.rename(columns={sensor.name: sensor.id}, inplace = True)
                url = f'{DEVICES_URL}{self.id}/readings'
                # Append task
                tasks.append(asyncio.ensure_future(self.post_datum(session, headers, url, df,
                    clean_na = clean_na, chunk_size = chunk_size, dry_run = dry_run,
                    max_retries = max_retries)))

            posts_ok = await asyncio.gather(*tasks)

        return not(False in posts_ok)

    async def post_datum(self, session, headers, url, df, clean_na = 'drop', chunk_size = 500, dry_run = False, max_retries = 2):
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
        std_out(f'Posting columns to {url}')
        std_out(f'Sensor ID: {list(df.columns)[0]}')
        df.index.name = 'recorded_at'

        # Split the dataframe in chunks
        chunked_dfs = [df[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]
        if len(chunked_dfs) > 1: std_out(f'Splitting post in chunks of size {chunk_size}')

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
                std_out(f'Dry run request to: {DEVICES_URL}{self.id}/readings for chunk ({i+1}/{len(chunked_dfs)})')
                return dumps(payload, indent = 2, cls = NpEncoder)

            post_ok = False
            retries = 0

            while post_ok == False and retries < max_retries:
                response = post(url, data = dumps(payload, cls = NpEncoder), headers = headers)

                if response.status_code == 200 or response.status_code == 201:
                    post_ok = True
                    break
                else:
                    retries += 1
                    std_out (f'Chunk ({i+1}/{len(chunked_dfs)}) post failed. \
                            API responded {response.status_code}.\
                            Retrying ({retries}/{max_retries}', 'WARNING')

            if (not post_ok) or (retries == max_retries):
                std_out (f'Chunk ({i+1}/{len(chunked_dfs)}) post failed. \
                        API responded {response.status_code}.\
                        Reached max_retries', 'ERROR')
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
            std_out('Cannot post without Admin Auth Bearer', 'ERROR')
            return

        headers = {'Authorization':'Bearer ' + environ['SC_ADMIN_BEARER'],
                   'Content-type': 'application/json'}

        post = {"postprocessing_attributes": loads(self.json.postprocessing.model_dump_json())}

        if dry_run:
            std_out(f'Dry run request to: {DEVICES_URL}{self.id}/')
            return dumps(post)

        std_out(f'Posting postprocessing_attributes:\n {post} to {DEVICES_URL}{self.id}')
        response = patch(f'{DEVICES_URL}{self.id}/',
                         data = dumps(post), headers = headers)

        if response.status_code == 200 or response.status_code == 201:
            std_out(f"Postprocessing posted", "SUCCESS")
            return True
        else:
            std_out(f"API responded with {response.status_code}")

        return False

    @property
    def blueprint_url(self):
        if url_checker(self.json.postprocessing.blueprint_url):
            return self.json.postprocessing.blueprint_url
        elif url_checker(self.json.postprocessing.hardware_url):
            return self.hardware_postprocessing.blueprint_url

    @property
    def hardware_postprocessing(self):
        return self._hardware_postprocessing

    @property
    def postprocessing(self):
        return self.json.postprocessing

    @property
    def latest_postprocessing(self):
        return self.postprocessing.latest_postprocessing

    @property
    def last_reading_at(self):
        return self.json.last_reading_at

    # TODO Rethink and make into a model?
    @property
    def metrics(self):
        return self._metrics

    # @staticmethod
    # def get_devices(
    #     owner_username: Optional[str] = None,
    #     kit_id: Optional[int] = None,
    #     city: Optional[str] = None,
    #     tags: Optional[list] = None,
    #     tag_method: Optional[str] = 'any',
    #     full: Optional[bool] = False,
    #     ) -> List[DeviceSummary]:
    #     """
    #     Gets devices from Smart Citizen API with certain requirements
    #     Parameters
    #     ----------
    #         user: string
    #             None
    #             Username
    #         kit_id: integer
    #             None
    #             Kit ID
    #         city: string, optional
    #             Empty string
    #             City
    #         tags: list of strings
    #             None
    #             Tags for the device (system or user). Default system wide are: indoor, outdoor, online, and offline
    #         tag_method: string
    #             'any'
    #             'any' or 'all'. Checks if 'all' the tags are to be included in the tags or it could be any
    #         full: bool
    #             False
    #             Returns a list with if False, or the whole dataframe if True
    #     Returns
    #     -------
    #         A list of kit IDs that comply with the requirements, or the full df, depending on full.
    #         If no requirements are set, returns all of them
    #     """

    #     world_map = get(API_URL + 'devices/world_map')
    #     df = DataFrame(world_map.json())
    #     df = df.dropna(axis=0, how='any')
    #     df['kit_id'] = df['kit_id'].astype(int)

    #     # Location
    #     if owner_username is not None: df=df[(df['owner_username']==owner_username)]
    #     if kit_id is not None: df=df[(df['kit_id']==kit_id)]
    #     if city is not None: df=df[(df['city']==city)]

    #     # Tags
    #     if tags is not None:
    #         if tag_method == 'any':
    #             df['has_tags'] = df.apply(lambda x: any(tag in x['system_tags']+x['user_tags'] for tag in tags), axis=1)
    #         elif tag_method == 'all':
    #             df['has_tags'] = df.apply(lambda x: all(tag in x['system_tags']+x['user_tags'] for tag in tags), axis=1)
    #         df=df[(df['has_tags']==True)]

    #     return [DeviceSummary(**d) for d in df.to_dict(orient='records')]
