import os

#-----------data_loader settins start-----------#

# change HISTORICAL_LOAD flag to True for historical load
# If you want to download the limited file then set the SAMPLE_FILE , Else set to None if you want to download all files
INCREMENTAL_LOAD=False
SAMPLE_FILE=100

# base url for the wheather data
# change the year to get data for a particular year
BASE_URL="https://www1.ncdc.noaa.gov/pub/data/gsod/"
FILE_NAME=None #'010010-99999-2023.op.gz'
FILE_OUTPUT_PATH = os.path.join(os.getcwd(), "data/raw")

# station name file settings
STATION_NAME_URL='https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv'
STATION_FILE_OUTPUT_PATH=os.path.join(os.getcwd(), "data/resources")
STATION_FILE_NAME='station_names.csv'

# country details file
COUNTRY_FILE_NAME='country.txt'
COUNTRY_URL='https://www1.ncdc.noaa.gov/pub/data/noaa/country-list.txt'
COUNTRY_FILE_OUTPUT_PATH=os.path.join(os.getcwd(), "data/resources")

# cloud settings
SAVE_TO_CLOUD=False
CLOUD_NAME='Azure'
CONTAINER_NAME='raw'
BLOB_NAME='data'
PROCESSED_BLOB_NAME='processed'
CONNECTION_STRING='DefaultEndpointsProtocol=https;AccountName=adfstorage1140;AccountKey=546RzkYi4oCkyT6TtZzbtieke5ksF12cMMCYlMcufpducNYja69BI9z19zDzDaVG+xbA6InpCbjE+AStXbdNqA==;EndpointSuffix=core.windows.net'


#-----------data_loader settins end-----------#

# data_cleaner settings
EXTENSION='op'
DATE_COLUMN='DATE'
KEY_COLUMNS=['STATION']


# data_transformer settings
SELECTED_COLUMNS=['DATE','Year','STN','STATION NAME','COUNTRY NAME','LAT', 
                  'LON','TEMP', 'DEWP', 'SLP', 'STP', 'VISIB',
                  'WDSP', 'MXSPD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP', 'FRSHTT',
                  'Day', 'IsWeekend','Hemisphere', 'Season', 'TempChange','WindCategory',
                  'VisibilityCategory','Fog' ,'Rain','Snow','Hail','Thunder','Tornado']

DATAFRAME_OUTPUT_PATH = os.path.join(os.getcwd(), "data/processed")
DATAFRAME_OUTPUT_FILE_NAME='WEATHER_PROCESSED.csv'
TO_TABLE=False

# data_analyzer settings
AGGREGATED_FILE_PATH=os.path.join(os.getcwd(), "data/aggregated")