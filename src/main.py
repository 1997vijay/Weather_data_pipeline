"""
Main file to process the GSOD weather data.

Dependencies:
    - pandas: A Python library for analyzing the data.
    - os: An operating system library for interacting with the system.
    - logging: A library for logging information.

Usage:
    1. Ensure that you have the required dependencies installed.
    3. Run the code to download, extract, and process the files.

    Author Information:
    Name: Vijay Kumar
    Date: 4th Jan 2024

Abstract/Description:
The primary script perform the following tasks: downloading GSOD files, saving them to local/cloud storage, conducting data cleaning, performing data transformation, 
and ultimately creating and saving a processed dataframe in a CSV file. This processed dataframe serves as a valuable resource for data scientists engaged in exploratory data analysis (EDA)
Change Log:
    - 4th Jan 2024: Initial creation.
"""

# import all required libraries
import logging
import pandas as pd
import config
import os

from data_loader import extract_data,save_file
from data_cleaner import clean_data,read_data
from data_transformer import add_features,save_dataframe
from cloudStorageSaver import CloudStorageSaver

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
        isIncrementalLoad=config.INCREMENTAL_LOAD
        if isIncrementalLoad:
            logging.info('Load Type: Incremental Load.')
        else:
            logging.info('Load Type: Historical Load.')

        # extract the data from url
        # set up the variables
        url=config.BASE_URL
        connectionString=config.CONNECTION_STRING
        cloudName=config.CLOUD_NAME
        fileOutputPath=config.FILE_OUTPUT_PATH
        saveToCloud=config.SAVE_TO_CLOUD
        fileName=config.FILE_NAME
        sampleFile=config.SAMPLE_FILE
        rawBlobName=config.BLOB_NAME
        listOfYears=extract_data(url,connectionString,cloudName,fileOutputPath,saveToCloud,fileName,isIncrementalLoad,sampleFile,rawBlobName)
        logging.info('extract_data finished.')

        # # process the data
        extension=config.EXTENSION
        dateColumn=config.DATE_COLUMN
        keyColumns=config.KEY_COLUMNS
        if isIncrementalLoad:
            listOfFilePath=[fileOutputPath+f'/{year}' for year in listOfYears]
            logging.info(listOfFilePath)
            weatherDf,stationDf,countryDf= clean_data(listOfFilePath,extension)

        else:
             weatherDf,stationDf,countryDf= clean_data([fileOutputPath],extension)
        logging.info('clean_data finished.')

        # # transform the data
        processed_df=add_features(weatherDataframe=weatherDf,stationDataframe=stationDf,countryDataframe=countryDf,dateColumn=dateColumn)
        logging.info('feature addition finished.')

        # # save processed data
        fileName=config.DATAFRAME_OUTPUT_FILE_NAME
        filePath=config.DATAFRAME_OUTPUT_PATH
        blobName=config.PROCESSED_BLOB_NAME

        if isIncrementalLoad:
            # For incremental load, read the processed file which already have historical data
            logging.info('Reading the already processed data.')
            historical_df=read_data(filePath=filePath+'/'+fileName)

            # removing data for the given listOfYears as data for these year will be concatenated with already processed data 
            historical_df=historical_df[~historical_df['Year'].isin(listOfYears)]
            print(historical_df[historical_df['Year'].isin(listOfYears)].head())

            # concate the processed_df and historical_df
            processed_df=pd.concat([historical_df,processed_df],ignore_index=True)
        print(processed_df.head())
        if saveToCloud:
            client=CloudStorageSaver(cloudName=cloudName,connection_string=connectionString)
            save_file(fileResponse=processed_df,fileName=fileName,outputPath=filePath,blobName=blobName,client=client)
        else:
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