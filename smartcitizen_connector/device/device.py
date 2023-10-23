from smartcitizen_connector.models import (Sensor, Kit, Owner, Location, Data, Device,
    Measurement, Postprocessing, HardwareInfo)
from smartcitizen_connector.config import *
from smartcitizen_connector.utils import *
from typing import Optional, List
from requests import get
from pandas import DataFrame, to_datetime
from datetime import datetime
from os import environ
from pydantic import TypeAdapter
import aiohttp
import asyncio

class SCDevice:
    id: int
    url: str
    timezone: str
    json: Device
    data: DataFrame

    def __init__(self, id):
        self.id = id
        self.url = f'{DEVICES_URL}{self.id}'
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
        else:
            std_out (f'Device {self.id} has no postprocessing information')

        return self.json.postprocessing

    async def get_datum(self, session, url, headers, sensor_id):
        async with session.get(url, headers = headers) as resp:
            data = await resp.json()

            if data['readings'] == []:
                std_out(f'No data in request for sensor: {sensor_id}', 'WARNING')
                return None

            return {sensor_id: data}

    async def get_data(self,
        min_date: Optional[datetime] = None,
        max_date: Optional[datetime] = None,
        rollup: Optional[str] = '1h',
        clean_na: Optional[str] = None,
        resample: Optional[bool] = False)->DataFrame:

        if 'SC_ADMIN_BEARER' in environ:
            std_out('Admin Bearer found, using it', 'SUCCESS')

            headers = {'Authorization':'Bearer ' + environ['SC_ADMIN_BEARER']}
        else:
            headers = None
            std_out('Admin Bearer not found', 'WARNING')

        std_out(f'Requesting data from SC API')
        std_out(f'Device ID: {self.id}')
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
                print (sensor)

                # Request sensor per ID
                url = self.url + '/readings?'

                if min_date is not None: url += f'from={min_date}'
                if max_date is not None: url += f'&to={max_date}'

                url += f'&rollup={rollup}'
                url += f'&sensor_id={sensor.id}'
                url += '&function=avg'

                tasks.append(asyncio.ensure_future(self.get_datum(session, url, headers, sensor.id)))

            data = await asyncio.gather(*tasks)

            for datum in data:
                if datum is None: continue
                sensors = self.json.data.sensors
                sensor_id = list(datum.keys())[0]
                sensor_name = ''
                # Find the id of the sensor
                for sensor in sensors:
                    if sensor.id == sensor_id:
                        sensor_name = sensor.name
                        break

                df_sensor = DataFrame(datum[sensor_id]['readings']).set_index(0)
                df_sensor.columns = [sensor_name]
                df_sensor.index = localise_date(df_sensor.index, self.timezone)
                df_sensor.sort_index(inplace=True)
                df_sensor = df_sensor[~df_sensor.index.duplicated(keep='first')]

                # Drop unnecessary columns
                df_sensor.drop([i for i in df_sensor.columns if 'Unnamed' in i], axis=1, inplace=True)
                # Check for weird things in the data
                df_sensor = df_sensor.astype(float, errors='ignore')
                # Resample
                if (resample):
                    df_sensor = df_sensor.resample(frequency).mean()

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

    # @staticmethod
    # def global_search(value: Optional[str] = None) -> DataFrame:
    #     """
    #     Gets devices from Smart Citizen API based on basic search query values,
    #     searching both Users and Devices at the same time.
    #     Global search documentation: https://developer.smartcitizen.me/#global-search
    #     Parameters
    #     ----------
    #         value: string
    #             None
    #             Query to fit
    #             For null, not_null values, use 'null' or 'not_null'
    #     Returns
    #     -------
    #         A list of kit IDs that comply with the requirements, or the full df, depending on full.
    #     """

    #     API_SEARCH_URL = API_URL + "search?q="

    #     # Value check
    #     if value is None: std_out(f'Value needs a value, {value} supplied', 'ERROR'); return None

    #     url = API_SEARCH_URL  + f'{value}'

    #     df = DataFrame()
    #     isn = True
    #     while isn:
    #         try:
    #             r = get(url)
    #             # If status code OK, retrieve data
    #             if r.status_code == 200 or r.status_code == 201:
    #                 h = process_headers(r.headers)
    #                 df = df.combine_first(DataFrame(r.json()).set_index('id'))
    #             else:
    #                 std_out('API reported {}'.format(r.status_code), 'ERROR')
    #         except:
    #             std_out('Failed request. Probably no connection', 'ERROR')
    #             pass

    #         if 'next' in h:
    #             if h['next'] == url: isn = False
    #             elif h['next'] != url: url = h['next']
    #         else:
    #             isn = False

    #     return df

    # @staticmethod
    # def search_by_query(key: Optional[str] = '', value: Optional[str] = None) -> DataFrame:
    #     """
    #     Gets devices from Smart Citizen API based on ransack parameters
    #     Basic query documentation: https://developer.smartcitizen.me/#basic-searching
    #     Parameters
    #     ----------
    #         key: string
    #             ''
    #             Query key according to the basic query documentation. Some (not all) parameters are:
    #             ['id', 'owner_id', 'name', 'description', 'mac_address', 'created_at',
    #             'updated_at', 'kit_id', 'geohash', 'last_recorded_at', 'uuid', 'state',
    #             'postprocessing_id', 'hardware_info']
    #         value: string
    #             None
    #             Query to fit
    #             For null, not_null values, use 'null' or 'not_null'
    #     Returns
    #     -------
    #         A list of kit IDs that comply with the requirements, or the full df, depending on full.
    #     """

    #     API_BASE_URL= API_URL + 'devices/'

    #     # Value check
    #     if value is None: std_out(f'Value needs a value, {value} supplied', 'ERROR'); return None

    #     if value == 'null' or value == 'not_null':
    #          url = API_BASE_URL  + f'?q[{key}_{value}]=1'
    #     else:
    #          url = API_BASE_URL  + f'?q[{key}]={value}'

    #     df = DataFrame()
    #     isn = True
    #     while isn:
    #         try:
    #             r = get(url)
    #             # If status code OK, retrieve data
    #             if r.status_code == 200 or r.status_code == 201:
    #                 h = process_headers(r.headers)
    #                 df = df.combine_first(DataFrame(r.json()).set_index('id'))
    #             else:
    #                 std_out('API reported {}'.format(r.status_code), 'ERROR')
    #         except:
    #             std_out('Failed request. Probably no connection', 'ERROR')
    #             pass

    #         if 'next' in h:
    #             if h['next'] == url: isn = False
    #             elif h['next'] != url: url = h['next']
    #         else:
    #             isn = False
    #     return df
