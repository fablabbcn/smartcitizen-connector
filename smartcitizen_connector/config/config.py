# Output config
out_level = 'DEBUG'
out_timestamp = True
# Base URL for all methods
API_URL = 'https://api.smartcitizen.me/v0/'
DEVICES_URL =  API_URL + 'devices/'
FRONTEND_URL = 'https://smartcitizen.me/kits/'
BASE_POSTPROCESSING_URL='https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/'
API_SEARCH_URL = API_URL + "search?q="

# Alphasense sensor codes
as_sensor_codes =  {
    '132':  'ASA4_CO',
    '133':  'ASA4_H2S',
    '130':  'ASA4_NO',
    '212':  'ASA4_NO2',
    '214':  'ASA4_OX',
    '134':  'ASA4_SO2',
    '162':  'ASB4_CO',
    '133':  'ASB4_H2S',#
    '130':  'ASB4_NO', #
    '202':  'ASB4_NO2',
    '204':  'ASB4_OX',
    '164':  'ASB4_SO2'
}