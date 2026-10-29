[![Python application](https://github.com/fablabbcn/smartcitizen-connector/actions/workflows/python-app.yml/badge.svg)](https://github.com/fablabbcn/smartcitizen-connector/actions/workflows/python-app.yml)

# Smart Citizen Connector

This is a connector written in `python` to get and post data from the Smart Citizen API. It can be used in conjunction with other scripts such as [scdata](https://github.com/fablabbcn/smartcitizen-data).

## Installation

Simply do:

```
pip install smartcitizen-connector
```

### Development

Clone the repo and install in editable mode:

```
git clone git@github.com:fablabbcn/smartcitizen-connector.git
cd smartcitizen-connector
pip install -e .
```

## Usage

See [docs](/docs) for examples. a basic usage is shown below:

- Device (create and get data):

```
from smartcitizen_connector import SCDevice
import asyncio

d = SCDevice(16549)
print (d.json.name)
print (d.json.owner)

await d.get_data(freq = '1Min') # returns pandas dataframe
print (d.data)
```

- Hardware and blueprints

A device with a `hardware_url` in its postprocessing is processed with the blueprints of its hardware, read from `BASE_POSTPROCESSING_URL` ([Smart Citizen Flows](https://flows.smartcitizen.me/api/v1/) by default). A hardware lists its blueprints by name, at most one of each kind: `process`, `long` and `backup`. `blueprint_kind` selects the one the device uses, which also defines the sensors to load. Without it, the `process` blueprint, else the `long` one. A hardware with only a `backup` blueprint has no channels to process. The `blueprint_url` of the device postprocessing is not used.

```
d = SCDevice(16549, blueprint_kind='long')
print (d.blueprints)        # {'process': 'sc_air', 'long': 'sc_air_baseline'}
print (d.blueprint_name)    # sc_air_baseline
```

- Search (see [docs](https://developer.smartcitizen.me/#basic-searching))

```
from smartcitizen_connector import search_by_query
# Users whose username contains "osc"
search_by_query(endpoint = 'users', key="username", search_matcher="cont", value="osc")
# Devices in which
search_by_query(endpoint = 'devices', key="name", search_matcher="cont", value="air")
# Devices created after (date greater than) "2023-08-11"
search_by_query(endpoint = 'devices', key="created_at", search_matcher="gt", value="2023-08-11")
```

- Authentication

Set the following environment variable with your Smart Citizen API token:

```
SC_BEARER=xxxxx
```
