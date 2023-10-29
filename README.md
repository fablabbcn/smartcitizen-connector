# Smart Citizen Connector

This is a simple connector written in `python` to get data from the Smart Citizen API. It is only providing this functionality, to be used in conjunction with other scripts, and it does not aim to replace [scdata](https://github.com/fablabbcn/smartcitizen-data), which is a much more advanced data analysis framework.

## Installation

Simply do:

```
pip install smartcitizen-connector
```

### Development

Or clone the repo and install in editable mode:

```
git clone git@github.com:fablabbcn/smartcitizen-connector.git
cd smartcitizen-connector
pip install -e .
```

## Usage

Device (create and get data - blazingly fast!):

```
from smartcitizen_connector import SCDevice
import asyncio

d = SCDevice(16549)
print (d.json.name)
print (d.json.owner)

await d.get_data(freq = '1Min') # returns pandas dataframe
...
```

Search (see [docs](https://developer.smartcitizen.me/#basic-searching))

```
from smartcitizen_connector import search_by_query
# Users whose username contains "osc"
search_by_query(endpoint = 'users', key="username", search_matcher="cont", value="osc")
# Devices in which
search_by_query(endpoint = 'devices', key="name", search_matcher="cont", value="air")
# Devices created after (date greater than) "2023-08-11"
search_by_query(endpoint = 'devices', key="created_at", search_matcher="gt", value="2023-08-11")
```