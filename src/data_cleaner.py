"""
Perform the data quality check on weather dataframe before anaylyzing further.

Dependencies:
    - pandas: A Python library for data analysis.
    - os: An operating system library for interacting with the system.
    - logging: A library for logging information.
    - glob: It is used to search for files that match a specific file pattern or name. It can be used to search CSV files and for text in files.

Usage:
    1. Ensure that you have the required dependencies installed.
    2. Confirm the correctness of the file path.
    3. Run the code to perform data cleaning operation on dataframe

    Author Information:
    Name: Vijay Kumar
    Date: 25 Dec 2023

Abstract/Description:
This script reads all files from the specified directory and perform data cleaing on dataframe.

Change Log:
    - 25 Dec 2023: Initial creation.
"""

# import required libraries
import pandas as pd
import numpy as np
import config
import logging
import os
import glob


from dataQuality import DataQualityCheck

def read_data(filePath,header=0):
    """
    Read data from a file based on its format (csv, xlsx, json).
    Args:
        file_path (str): The path to the file to be read.
    Returns:
        pd.DataFrame: The DataFrame containing the data from the file.
    Raises:
        ValueError: If the file format is not supported (not csv, xlsx, or json).
        FileNotFoundError: If the specified file is not found.
    """
    try:
        extension=filePath.split("/")[-1].split(".")[-1]
        if extension=='csv':
            return pd.read_csv(filePath,header=header)
        elif extension=='xlsx':
            return pd.read_excel(filePath,header=header)
        elif extension=='json':
            return pd.read_json(filePath,header=header)
        elif extension=='op':
            return pd.read_fwf(filePath,header=header)
        elif extension=='txt':
            return pd.read_fwf(filePath,header=header)
        else:
            raise ValueError('Not a valid file format.')
    except Exception as e:
        raise FileNotFoundError(f'File not found.{e}')

def read_data_files(filePath, extension):
    """  
    This function reads data from file(s) in the specified directory or from a specific file. 
    It supports filtering files based on the provided file extension. The data from each file 
    is read using the `read_data` function and appended to a list. The final result is a pandas 
    DataFrame obtained by concatenating the data from all the files.

    Args:
        filePath (List): List containing the path to the directory or file.
        extension (str): The file extension to filter files.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the concatenated data.

    Raises:
        Exception: If there is an error during the reading process, an exception is raised.
    """
    logging.info('read_data_files started.')
    try:
        data = []
        
        if not isinstance(filePath, list) or not filePath or len(filePath)==0:
            logging.error('Invalid file path. Please provide a non-empty list of file paths.')
            raise ValueError('Invalid file path. Please provide a non-empty list of file paths.')
        
        for path in filePath:
            if os.path.isdir(path):
                # Read all files with the given extension from the specified directory and its subdirectories
                for root, dirs, files in os.walk(path):
                    for file in files:
                        if file.endswith(f".{extension}"):
                            fileName = os.path.join(root, file)
                            df = read_data(fileName)
                            data.append(df)
            else:
                # Read a single file
                df = read_data(filePath)
                data.append(df)
        df = pd.concat(data, axis=0, ignore_index=True)
        return df

    except Exception as e:
        logging.error(f'Error while reading the file data.{e}')
        raise e
    

def clean_columnName(dataframe):
    """
    Renames columns containing "---" by removing the "---" from the column names.
    Args:
        dataframe (pd.DataFrame): The input DataFrame with column names containing "---".

    Returns:
        pd.DataFrame: The DataFrame with updated column names.
    """
    # Rename columns with "---"
    try:
        for column in dataframe.columns:
            if '---' in column:
                new_column = column.replace('---', '')
                dataframe = dataframe.rename(columns={column: new_column})
        return dataframe
    except Exception as e:
        logging.error(f'Error in clean_columnName.{e}')
        raise e

def fahrenheit_to_celsius(fahrenheit):
    """
    Convert temperature from Fahrenheit to Celsius.
    Args:
        fahrenheit (float): Temperature in Fahrenheit.

    Returns:
        float: Temperature in Celsius.
    """
    celsius = (fahrenheit - 32) / 1.8
    return round(celsius,2)

    
def clean_data(filePath,extension):
    """
    Function will read all the weather files from given directory, it will also read station details and country details as well.
    Then it will perform data cleaning operation on the dataframe.
    Args:
        file_path (List): List of path to the directory or file.
        extension (str): The file extension to filter files.

    Returns:
        pd.DataFrame or list: Pandas dataframe for further processing.

    Raises:
        ValueError: If the specified file path is not valid or no files found with the given extension.
        FileNotFoundError: If the specified file is not found.
        Exception: If an error occurs during the process.
    """
    logging.info('Data quality check started.')
    try:
        #-----------------Weather data cleaning Start-----------------#
        # read single or all files
        dataframe=read_data_files(filePath=filePath,extension=extension)

        # Remove "---" from column name
        dataframe=clean_columnName(dataframe)

        # Replaces missing values indicated by 999.9 in the given dataset with NaN.
        dataframe=dataframe.replace(999.9,np.nan)

        # Convert string format to date format
        dataframe['DATE']=pd.to_datetime(dataframe['YEARMODA'], format='%Y%m%d')
        dataframe['Year']=dataframe['DATE'].dt.year
        

        # Columns to float type after removing '*'
        try:
            dataframe['MAX']=dataframe['MAX'].astype(str).str.replace('*','').astype(float)
            dataframe['MIN']=dataframe['MIN'].astype(str).str.replace('*','').astype(float)
        except:
            dataframe['MAX']=dataframe['MAX'].replace('*','').astype(float)
            dataframe['MIN']=dataframe['MIN'].replace('*','').astype(float)  

        # convert fahrenheit to celsius
        dataframe['TEMP']=dataframe['TEMP'].apply(fahrenheit_to_celsius)
        dataframe['MAX']=dataframe['MAX'].apply(fahrenheit_to_celsius)
        dataframe['MIN']=dataframe['MIN'].apply(fahrenheit_to_celsius)

        # Remove unknown and unnecessary columns
        dataframe=dataframe[[column for column in dataframe.columns.tolist() if 'Unnamed' not in column]]
        dataframe=dataframe.drop('YEARMODA',axis=1)
        #-----------------Weather data cleaning End-----------------#

        # read and concate station and country data with weather data
        # read station details and country details
        station_df=read_data(filePath=config.STATION_FILE_OUTPUT_PATH+'/'+config.STATION_FILE_NAME)
        country_df=read_data(filePath=config.COUNTRY_FILE_OUTPUT_PATH+'/'+config.COUNTRY_FILE_NAME)

        # convert data type for USAF from string to int by filtering out string values
        station_df=station_df[station_df['USAF'].str.isnumeric()]
        station_df['USAF']=station_df['USAF'].astype(int)

        return dataframe,station_df,country_df
    except FileNotFoundError as fe:
        raise FileNotFoundError(f'File not found: {filePath}. {fe}')
    except ValueError as ve:
        raise ValueError(f'Error in input parameters: {ve}')
    except Exception as e:
        raise Exception(f'Error during data processing: {e}')
    
# call the main function
if __name__ == '__main__':
    filePath=config.FILE_OUTPUT_PATH
    extension=config.EXTENSION
    dateColumn=config.DATE_COLUMN
    keyColumns=config.KEY_COLUMNS
    clean_data(filePath,extension)