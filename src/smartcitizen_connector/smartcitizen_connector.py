#!/usr/bin/env python3
from .models import Sensor, Kit, Owner, Location, Data, Device, DeviceSummary
from typing import Optional, List
from requests import get
import urllib3
from pandas import DataFrame, to_datetime
from tzwhere import tzwhere
from datetime import datetime

tz_where = tzwhere.tzwhere(forceTZ=True)

urllib3.disable_warnings()

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

# Output config
out_level = 'NORMAL'
out_timestamp = True

def clean(df, clean_na = None, how = 'all'):
    """
    Helper function for cleaning nan in a pandas.DataFrame
    Parameters
    ----------
        df: pandas.DataFrame
            The dataframe to clean
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

# Base URL for all methods
API_URL = 'https://api.smartcitizen.me/v0/'

class ScApiDevice:

    API_BASE_URL= API_URL + 'devices/'

    def __init__ (self, device_id: int):

        self.id = device_id # the number after https://smartcitizen.me/kits/######

        # Additional device stuff
        self.kit_id = None # the number that defines the type of blueprint
        self.mac = None
        self.last_reading_at = None
        self.added_at = None
        self.timezone = None
        self.lat = None
        self.long = None
        self.alt = None
        self.data = None
        self.sensors = None
        self.devicejson = None
        self.postprocessing = None
        self._url = f'https://smartcitizen.me/kits/{self.id}'
        self._api_url = f'{self.API_BASE_URL}{self.id}'

    @property
    def url(self) -> str:
        return self._url

    @property
    def api_url(self) -> str:
        return self._api_url

    @staticmethod
    def get_kits() -> list[Kit]:
        kits = get(API_URL + 'kits/?per_page=200')

        if kits.status_code == 429:
            std_out('API reported {}. Retrying once'.format(kits.status_code),
                    'WARNING')
            return None

        if kits.status_code == 200 or kits.status_code == 201:
            result = [Kit(**kit) for kit in kits.json()]
            return result
        else:
            std_out('API reported {}'.format(kits.status_code), 'ERROR')
            return None

    @staticmethod
    def get_device_info(id:int) -> DeviceSummary:
        device = get(API_URL + 'devices/{}/'.format(id))

        if device.status_code == 429:
            std_out('API reported {}. Retrying once'.format(device.status_code),
                    'WARNING')
            return None

        if device.status_code == 200 or device.status_code == 201:
            dj = device.json()

            dj['owner_id'] = int(dj['owner']['id'])
            dj['owner_username'] = dj['owner']['username']
            dj['latitude'] = float(dj['data']['location']['latitude'])
            dj['longitude'] = float(dj['data']['location']['longitude'])
            dj['city'] = dj['data']['location']['city']
            dj['country_code'] = dj['data']['location']['country_code']
            dj['kit_id'] = int(dj['kit']['id'])

            result = DeviceSummary(**dj)
            return result
        else:
            std_out('API reported {}'.format(device.status_code), 'ERROR')
            return None

    @staticmethod
    def get_devices(
        owner_username: Optional[str] = None,
        kit_id: Optional[int] = None,
        city: Optional[str] = None,
        tags: Optional[list] = None,
        tag_method: Optional[str] = 'any',
        full: Optional[bool] = False,
        ) -> list[DeviceSummary]:
        """
        Gets devices from Smart Citizen API with certain requirements
        Parameters
        ----------
            user: string
                None
                Username
            kit_id: integer
                None
                Kit ID
            city: string, optional
                Empty string
                City
            tags: list of strings
                None
                Tags for the device (system or user). Default system wide are: indoor, outdoor, online, and offline
            tag_method: string
                'any'
                'any' or 'all'. Checks if 'all' the tags are to be included in the tags or it could be any
            full: bool
                False
                Returns a list with if False, or the whole dataframe if True
        Returns
        -------
            A list of kit IDs that comply with the requirements, or the full df, depending on full.
            If no requirements are set, returns all of them
        """

        world_map = get(API_URL + 'devices/world_map')
        df = DataFrame(world_map.json())
        df = df.dropna(axis=0, how='any')
        df['kit_id'] = df['kit_id'].astype(int)

        # Location
        if owner_username is not None: df=df[(df['owner_username']==owner_username)]
        if kit_id is not None: df=df[(df['kit_id']==kit_id)]
        if city is not None: df=df[(df['city']==city)]

        # Tags
        if tags is not None:
            if tag_method == 'any':
                df['has_tags'] = df.apply(lambda x: any(tag in x['system_tags']+x['user_tags'] for tag in tags), axis=1)
            elif tag_method == 'all':
                df['has_tags'] = df.apply(lambda x: all(tag in x['system_tags']+x['user_tags'] for tag in tags), axis=1)
            df=df[(df['has_tags']==True)]

        return [DeviceSummary(**d) for d in df.to_dict(orient='records')]

    @staticmethod
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

        API_BASE_URL = "https://api.smartcitizen.me/v0/search?q="

        # Value check
        if value is None: std_out(f'Value needs a value, {value} supplied', 'ERROR'); return None

        url = API_BASE_URL  + f'{value}'

        df = DataFrame()
        isn = True
        while isn:
            try:
                r = get(url)
                # If status code OK, retrieve data
                if r.status_code == 200 or r.status_code == 201:
                    h = process_headers(r.headers)
                    df = df.combine_first(DataFrame(r.json()).set_index('id'))
                else:
                    std_out('API reported {}'.format(r.status_code), 'ERROR')
            except:
                std_out('Failed request. Probably no connection', 'ERROR')
                pass

            if 'next' in h:
                if h['next'] == url: isn = False
                elif h['next'] != url: url = h['next']
            else:
                isn = False

        return df

    @staticmethod
    def search_by_query(key: Optional[str] = '', value: Optional[str] = None) -> DataFrame:
        """
        Gets devices from Smart Citizen API based on ransack parameters
        Basic query documentation: https://developer.smartcitizen.me/#basic-searching
        Parameters
        ----------
            key: string
                ''
                Query key according to the basic query documentation. Some (not all) parameters are:
                ['id', 'owner_id', 'name', 'description', 'mac_address', 'created_at',
                'updated_at', 'kit_id', 'geohash', 'last_recorded_at', 'uuid', 'state',
                'postprocessing_id', 'hardware_info']
            value: string
                None
                Query to fit
                For null, not_null values, use 'null' or 'not_null'
        Returns
        -------
            A list of kit IDs that comply with the requirements, or the full df, depending on full.
        """

        API_BASE_URL = "https://api.smartcitizen.me/v0/devices/"

        # Value check
        if value is None: std_out(f'Value needs a value, {value} supplied', 'ERROR'); return None

        if value == 'null' or value == 'not_null':
             url = API_BASE_URL  + f'?q[{key}_{value}]=1'
        else:
             url = API_BASE_URL  + f'?q[{key}]={value}'

        df = DataFrame()
        isn = True
        while isn:
            try:
                r = get(url)
                # If status code OK, retrieve data
                if r.status_code == 200 or r.status_code == 201:
                    h = process_headers(r.headers)
                    df = df.combine_first(DataFrame(r.json()).set_index('id'))
                else:
                    std_out('API reported {}'.format(r.status_code), 'ERROR')
            except:
                std_out('Failed request. Probably no connection', 'ERROR')
                pass

            if 'next' in h:
                if h['next'] == url: isn = False
                elif h['next'] != url: url = h['next']
            else:
                isn = False
        return df

    def get_mac(self, update:  Optional[bool] = None) -> str:
        if self.mac is None or update:
            std_out(f'Requesting MAC from API for device {self.id}')
            # Get device
            try:
                deviceR = get(self.API_BASE_URL + '{}/'.format(self.id))

                # If status code OK, retrieve data
                if deviceR.status_code == 200 or deviceR.status_code == 201:
                    if 'hardware_info' in deviceR.json().keys(): self.mac = deviceR.json()['hardware_info']['mac']
                    std_out ('Device {} is has this MAC {}'.format(self.id, self.mac))
                else:
                    std_out('API reported {}'.format(deviceR.status_code), 'ERROR')
            except:
                std_out('Failed request. Probably no connection', 'ERROR')
                pass

        return self.mac

    def get_device_json(self, update:  Optional[bool] = None) -> dict:
        if self.devicejson is None or update:
            try:
                deviceR = get(self.API_BASE_URL + '{}/'.format(self.id))
                if deviceR.status_code == 429:
                    std_out('API reported {}. Retrying once'.format(deviceR.status_code),
                            'WARNING')
                    sleep(30)
                    deviceR = get(self.API_BASE_URL + '{}/'.format(self.id))

                if deviceR.status_code == 200 or deviceR.status_code == 201:
                    self.devicejson = deviceR.json()
                else:
                    std_out('API reported {}'.format(deviceR.status_code), 'ERROR')
            except:
                std_out('Failed request. Probably no connection', 'ERROR')
                pass
        return self.devicejson

    def get_device_description(self, update:  Optional[bool] = None) -> str:
        if self.get_device_json(update) is not None:
            return self.get_device_json()['kit']['description']
        return None

    def get_kit_ID(self, update:  Optional[bool] = None) -> int:

        if self.kit_id is None or update:
            if self.get_device_json(update) is not None:
                self.kit_id = self.devicejson['kit']['id']

        return self.kit_id

    def get_device_last_reading(self, update:  Optional[bool] = None) -> datetime:

        if self.last_reading_at is None or update:
            if self.get_device_json(update) is not None and self.get_device_json(update)['state'] != 'never_published':
                self.last_reading_at = localise_date(self.devicejson['last_reading_at'], 'UTC').strftime('%Y-%m-%dT%H:%M:%SZ')

        std_out ('Device {} has last reading at {}'.format(self.id, self.last_reading_at))

        return self.last_reading_at

    def get_device_added_at(self, update:  Optional[bool] = None) -> datetime:

        if self.added_at is None or update:
            if self.get_device_json(update) is not None:
                self.added_at = localise_date(self.devicejson['added_at'], 'UTC').strftime('%Y-%m-%dT%H:%M:%SZ')

        std_out ('Device {} was added at {}'.format(self.id, self.added_at))

        return self.added_at

    def get_device_postprocessing(self, update:  Optional[bool] = None) -> dict:

        if self.postprocessing is None or update:
            if self.get_device_json(update) is not None:
                self.postprocessing = self.devicejson['postprocessing']

                if self.postprocessing is not None:
                    # Check the url in hardware
                    if 'hardware_url' in self.postprocessing:
                        urls = url_checker(self.postprocessing['hardware_url'])
                        # If URL is empty, try prepending base url from config
                        if not urls:
                            tentative_url = f"{config._base_postprocessing_url}hardware/{self.postprocessing['hardware_url']}.{config._default_file_type}"
                        else:
                            if len(urls)>1: std_out('URLs for postprocessing recipe are more than one, trying first', 'WARNING')
                            tentative_url = urls[0]

                        self.postprocessing['hardware_url'] = tentative_url

                    std_out ('Device {} has postprocessing information:\n{}'.format(self.id, self.postprocessing))
                else:
                    std_out (f'Device {self.id} has no postprocessing information')

        return self.postprocessing

    def get_device_timezone(self, update:  Optional[bool] = None) -> str:

        if self.timezone is None or update:
            latitude, longitude = self.get_device_lat_long(update)
            # Localize it

            if latitude is not None and longitude is not None:
                self.timezone = tz_where.tzNameAt(latitude, longitude, forceTZ=True)

        std_out ('Device {} timezone is {}'.format(self.id, self.timezone))

        return self.timezone

    def get_device_lat_long(self, update:  Optional[bool] = None) -> tuple:

        if self.lat is None or self.long is None or update:
            if self.get_device_json(update) is not None:
                latidude = longitude = None
                if 'location' in self.devicejson.keys():
                    latitude, longitude = self.devicejson['location']['latitude'], self.devicejson['location']['longitude']
                elif 'data' in self.devicejson.keys():
                    if 'location' in self.devicejson['data'].keys():
                        latitude, longitude = self.devicejson['data']['location']['latitude'], self.devicejson['data']['location']['longitude']

                self.lat = latitude
                self.long = longitude

        std_out ('Device {} is located at {}, {}'.format(self.id, self.lat, self.long))

        return (self.lat, self.long)

    def get_device_alt(self, update:  Optional[bool] = None) -> float:

        if self.lat is None or self.long is None:
            self.get_device_lat_long(update)

        if self.alt is None or update:
            self.alt = get_elevation(_lat = self.lat, _long = self.long)

        std_out ('Device {} altitude is {}m'.format(self.id, self.alt))

        return self.alt

    def get_device_sensors(self, update:  Optional[bool] = None) -> dict:

        if self.sensors is None or update:
            if self.get_device_json(update) is not None:
                # Get available sensors in platform
                sensors = self.devicejson['data']['sensors']

                # Put the ids and the names in lists
                self.sensors = dict()
                for sensor in sensors:
                    self.sensors[sensor['id']] = sensor['name']

        return self.sensors

    def get_device_data(self,
        min_date: Optional[datetime] = None,
        max_date: Optional[datetime] = None,
        rollup: Optional[str] = '1h',
        clean_na: Optional[str] = None,
        resample: Optional[bool] = False)->DataFrame:

        std_out(f'Requesting data from SC API')
        std_out(f'Device ID: {self.id}')

        std_out(f'Using rollup: {rollup}')

        # Make sure we have the everything we need beforehand
        self.get_device_sensors()
        self.get_device_timezone()
        self.get_device_last_reading()
        self.get_device_added_at()
        self.get_kit_ID()

        if self.timezone is None:
            std_out('Device does not have timezone set, skipping', 'WARNING')
            return None

        # Check start date and end date
        # Converting to UTC by passing None
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.tz_convert.html
        if min_date is not None:
            min_date = localise_date(to_datetime(min_date), 'UTC').strftime('%Y-%m-%dT%H:%M:%S')
            std_out (f'Min Date: {min_date}')
        else:
            min_date = localise_date(to_datetime('2001-01-01'), 'UTC').strftime('%Y-%m-%dT%H:%M:%S')
            std_out(f"No min_date specified")

        if max_date is not None:
            max_date = localise_date(to_datetime(max_date), 'UTC').strftime('%Y-%m-%dT%H:%M:%S')
            std_out (f'Max Date: {max_date}')

        # Trim based on actual data available
        if min_date is not None and self.last_reading_at is not None:
            if min_date > self.last_reading_at:
                std_out(f'Device request would yield empty data (min_date). Returning', 'WARNING')
                return None

        if max_date is not None and self.added_at is not None:
            if max_date < self.added_at:
                std_out(f'Device request would yield empty data (max_date). Returning', 'WARNING')
                return None

        if max_date is not None and self.last_reading_at is not None:
            if max_date > self.last_reading_at:
                std_out('Trimming max_date to last reading', 'WARNING')
                max_date = self.last_reading_at

        # Print stuff
        std_out('Kit ID: {}'.format(self.kit_id))
        std_out(f'Device timezone: {self.timezone}')
        if not self.sensors.keys():
            std_out(f'Device is empty')
            return None
        else: std_out(f'Sensor IDs: {list(self.sensors.keys())}')

        df = DataFrame()
        std_out(f'Requesting from {min_date} to {max_date}')

        # Get devices in the sensor first
        for sensor_id in self.sensors.keys():

            # Request sensor per ID
            request = self.API_BASE_URL + '{}/readings?'.format(self.id)

            if min_date is not None: request += f'from={min_date}'
            if max_date is not None: request += f'&to={max_date}'

            request += f'&rollup={rollup}'
            request += f'&sensor_id={sensor_id}'
            request += '&function=avg'

            # Make request
            headers = {'Content-type': 'application/json'}
            response = get(request, headers = headers)

            # Retry once in case of 429 after 30s
            if response.status_code == 429:
                std_out('Too many requests, waiting for 1 more retry', 'WARNING')
                sleep (30)
                response = get(request, headers = headers)

            flag_error = False
            try:
                sensorjson = response.json()
            except:
                std_out(f'Problem with json data from API, {response.status_code}', 'ERROR')
                flag_error = True
                pass
                continue

            if 'readings' not in sensorjson.keys():
                std_out(f'No readings key in request for sensor: {sensor_id} ({self.sensors[sensor_id]})', 'ERROR')
                flag_error = True
                continue

            elif sensorjson['readings'] == []:
                std_out(f'No data in request for sensor: {sensor_id} ({self.sensors[sensor_id]})', 'WARNING')
                flag_error = True
                continue

            if flag_error: continue

            try:
                dfsensor = DataFrame(sensorjson['readings']).set_index(0)
                dfsensor.columns = [self.sensors[sensor_id]]
                # dfsensor.index = to_datetime(dfsensor.index).tz_localize('UTC').tz_convert(self.timezone)
                dfsensor.index = localise_date(dfsensor.index, self.timezone)
                dfsensor.sort_index(inplace=True)
                dfsensor = dfsensor[~dfsensor.index.duplicated(keep='first')]

                # Drop unnecessary columns
                dfsensor.drop([i for i in dfsensor.columns if 'Unnamed' in i], axis=1, inplace=True)
                # Check for weird things in the data
                dfsensor = dfsensor.astype(float, errors='ignore')
                # dfsensor = dfsensor.apply(to_numeric, errors='coerce')
                # Resample
                if (resample):
                    dfsensor = dfsensor.resample(convert_rollup_to_freq(rollup)).mean()
                df = df.combine_first(dfsensor)
            except:
                print_exc()
                std_out('Problem with sensor data from API', 'ERROR')
                flag_error = True
                pass
                continue

            try:
                df = df.reindex(df.index.rename('TIME'))
                df = clean(df, clean_na, how = 'all')
                self.data = df

            except:
                std_out('Problem closing up the API dataframe', 'ERROR')
                pass
                return None

        if flag_error == False: std_out(f'Device {self.id} loaded successfully from API', 'SUCCESS')
        return self.data


# def get_device_data(self, min_date = None, max_date = None, frequency = '1Min', clean_na = None, resample = True):

#         if 'SC_ADMIN_BEARER' in environ:
#             std_out('Admin Bearer found, using it', 'SUCCESS')

#             headers = {'Authorization':'Bearer ' + environ['SC_ADMIN_BEARER']}
#         else:
#             headers = None
#             std_out('Admin Bearer not found', 'WARNING')

#         std_out(f'Requesting data from SC API')
#         std_out(f'Device ID: {self.id}')

#         rollup = self.convert_rollup(frequency)
#         std_out(f'Using rollup: {rollup}')

#         # Make sure we have the everything we need beforehand
#         self.get_device_sensors()
#         self.get_device_timezone()
#         self.get_device_last_reading()
#         self.get_device_added_at()
#         self.get_kit_ID()

#         if self.timezone is None:
#             std_out('Device does not have timezone set, skipping', 'WARNING')
#             return None

#         # Check start date and end date
#         # Converting to UTC by passing None
#         # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.tz_convert.html
#         if min_date is not None:
#             min_date = localise_date(to_datetime(min_date), 'UTC').strftime('%Y-%m-%dT%H:%M:%S')
#             std_out (f'Min Date: {min_date}')
#         else:
#             min_date = localise_date(to_datetime('2001-01-01'), 'UTC').strftime('%Y-%m-%dT%H:%M:%S')
#             std_out(f"No min_date specified")

#         if max_date is not None:
#             max_date = localise_date(to_datetime(max_date), 'UTC').strftime('%Y-%m-%dT%H:%M:%S')
#             std_out (f'Max Date: {max_date}')

#         # Trim based on actual data available
#         if min_date is not None and self.last_reading_at is not None:
#             if min_date > self.last_reading_at:
#                 std_out(f'Device request would yield empty data (min_date). Returning', 'WARNING')
#                 return None

#         if max_date is not None and self.added_at is not None:
#             if max_date < self.added_at:
#                 std_out(f'Device request would yield empty data (max_date). Returning', 'WARNING')
#                 return None

#         if max_date is not None and self.last_reading_at is not None:
#             if max_date > self.last_reading_at:
#                 std_out('Trimming max_date to last reading', 'WARNING')
#                 max_date = self.last_reading_at

#         # Print stuff
#         std_out('Kit ID: {}'.format(self.kit_id))
#         std_out(f'Device timezone: {self.timezone}')
#         if not self.sensors.keys():
#             std_out(f'Device is empty')
#             return None
#         else: std_out(f'Sensor IDs: {list(self.sensors.keys())}')

#         df = DataFrame()
#         std_out(f'Requesting from {min_date} to {max_date}')

#         # Get devices in the sensor first
#         for sensor_id in self.sensors.keys():

#             # Request sensor per ID
#             request = self.API_BASE_URL + '{}/readings?'.format(self.id)

#             if min_date is not None: request += f'from={min_date}'
#             if max_date is not None: request += f'&to={max_date}'

#             request += f'&rollup={rollup}'
#             request += f'&sensor_id={sensor_id}'
#             request += '&function=avg'

#             # Make request
#             response = get(request, headers = headers)

#             # Retry once in case of 429 after 30s
#             if response.status_code == 429:
#                 std_out('Too many requests, waiting for 1 more retry', 'WARNING')
#                 sleep (30)
#                 response = get(request, headers = headers)

#             flag_error = False
#             try:
#                 sensorjson = response.json()
#             except:
#                 std_out(f'Problem with json data from API, {response.status_code}', 'ERROR')
#                 flag_error = True
#                 pass
#                 continue

#             if 'readings' not in sensorjson.keys():
#                 std_out(f'No readings key in request for sensor: {sensor_id} ({self.sensors[sensor_id]})', 'ERROR')
#                 flag_error = True
#                 continue

#             elif sensorjson['readings'] == []:
#                 std_out(f'No data in request for sensor: {sensor_id} ({self.sensors[sensor_id]})', 'WARNING')
#                 flag_error = True
#                 continue

#             if flag_error: continue

#             try:
#                 dfsensor = DataFrame(sensorjson['readings']).set_index(0)
#                 dfsensor.columns = [self.sensors[sensor_id]]
#                 # dfsensor.index = to_datetime(dfsensor.index).tz_localize('UTC').tz_convert(self.timezone)
#                 dfsensor.index = localise_date(dfsensor.index, self.timezone)
#                 dfsensor.sort_index(inplace=True)
#                 dfsensor = dfsensor[~dfsensor.index.duplicated(keep='first')]

#                 # Drop unnecessary columns
#                 dfsensor.drop([i for i in dfsensor.columns if 'Unnamed' in i], axis=1, inplace=True)
#                 # Check for weird things in the data
#                 dfsensor = dfsensor.astype(float, errors='ignore')
#                 # dfsensor = dfsensor.apply(to_numeric, errors='coerce')
#                 # Resample
#                 if (resample):
#                     dfsensor = dfsensor.resample(frequency).mean()
#                 df = df.combine_first(dfsensor)
#             except:
#                 print_exc()
#                 std_out('Problem with sensor data from API', 'ERROR')
#                 flag_error = True
#                 pass
#                 continue

#             try:
#                 df = df.reindex(df.index.rename('TIME'))
#                 df = clean(df, clean_na, how = 'all')
#                 self.data = df

#             except:
#                 std_out('Problem closing up the API dataframe', 'ERROR')
#                 pass
#                 return None

#         if flag_error == False: std_out(f'Device {self.id} loaded successfully from API', 'SUCCESS')
#         return self.data
