from smartcitizen_connector import SCDevice
from smartcitizen_connector.tools import set_logger_level
import logging
import asyncio

set_logger_level(logging.WARNING)

# Get all data from a device
device = SCDevice(17018)
await device.get_data()
print (device.data)

# Change logger level if you want
print ('Changing logger level')
set_logger_level(logging.INFO)

# Specify some restrictions
await device.get_data(min_date='2024-01-30', max_date='2024-01-31', frequency='10Min')
print (device.data)

# More options, test the query below - This will round up the timestamps, also clean NaNs
await device.get_data(min_date='2024-01-30', max_date='2024-01-31', frequency='10Min', clean_na = True, resample = True)
print (device.data)
