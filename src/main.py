
# import all required libraries
import logging
import pandas as pd
import config
import os

from data_loader import extract_data
from data_cleaner import clean_data
from data_transformer import add_features,save_dataframe

# Configure the default behaviour logging system
# logging.INFO--> log info or higher log like error or critical
# use the format --> time - log level - message
logFileName='data_pipeline.log'
logFolder='log'
logFilePath=os.path.join(os.getcwd(),logFolder,logFileName)
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',filename=logFilePath,filemode='w')

def run_pipeline():
    """
    Execute the data processing pipeline.

    This function performs the following steps:
    1. Extracts data from a specified URL and saves it to a local file or cloud storage.
    2. Processes the extracted data by cleaning and transforming it.
    3. Adds additional features to the processed DataFrame.
    4. Saves the processed DataFrame to a specified output file.

    The configuration parameters for data extraction, processing, and saving are provided in the config module.

    Raises:
        Exception: If there is an error during any step of the pipeline, an exception is raised and logged.

    Returns:
        None: The function executes the entire data processing pipeline.
    """
    logging.info('Pipeline started.')
    try:
        # extract the data from url
        # set up the variables
        url=config.BASE_URL
        connectionString=config.CONNECTION_STRING
        cloudName=config.CLOUD_NAME
        fileOutputPath=config.FILE_OUTPUT_PATH
        saveToCloud=config.SAVE_TO_CLOUD
        fileName=config.FILE_NAME
        isComparison=config.COMPARISON
        sampleFile=config.SAMPLE_FILE
        extract_data(url,connectionString,cloudName,fileOutputPath,saveToCloud,fileName,isComparison,sampleFile)

        # # process the data
        extension=config.EXTENSION
        dateColumn=config.DATE_COLUMN
        keyColumns=config.KEY_COLUMNS
        weatherDf,stationDf,countryDf= clean_data(fileOutputPath,extension)

        # # transform the data
        processed_df=add_features(weatherDataframe=weatherDf,stationDataframe=stationDf,countryDataframe=countryDf,dateColumn=dateColumn)

        # # save processed data
        fileName=config.DATAFRAME_OUTPUT_FILE_NAME
        filePath=config.DATAFRAME_OUTPUT_PATH
        result=save_dataframe(dataframe=processed_df,fileName=fileName,filePath=filePath)
        if result:
            logging.info('Pipeline ran successfully.')
    except Exception as e:
        logging.error(f'Error in pipeline.{e}')

if __name__=='__main__':
    try:
        run_pipeline()
    except Exception as e:
        raise e