
"""
Add new feature to the dataframe.

Dependencies:
    - pandas: A Python library for data analysis and manuplation.
    - numpy: NumPy is a Python library used for working with arrays
    - logging: A library for logging information.

Usage:
    1. Ensure that you have the required dependencies installed.
    3. Run the code to add new features column.

    Author Information:
    Name: Vijay Kumar
    Date: 25 Dec 2023

Abstract/Description:
This script new feature columns like Day, IsWeekend, TempChange, WindCategory and Season.

Change Log:
    - 25 Dec 2023: Initial creation.
"""

#import required libraries
import pandas as pd
import numpy as np
import logging
import config

from pandas.errors import EmptyDataError

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
        elif extension=='txt':
            return pd.read_fwf(filePath,header=header)
        else:
            raise ValueError('Not a valid file format.')
    except Exception as e:
        raise FileNotFoundError(f'File not found.{e}')


def check_columns(dataframe,listColumns):
    """
    function will check if the provides columns are present in the dataframe or not
    Arguments:
        dataframe -- Pandas dataframe
        listColumns -- list of columns
    """
    try:
        if len(listColumns)!=0:
            missingColumns=[column for column in listColumns if column not in dataframe.columns.tolist()]
            if len(missingColumns)!=0:
                logging.error(f'Missing columns are {missingColumns}')
            else:
                return True
        else:
            logging.error('Empty columns list')
    except Exception as e:
        logging.error(f'Error: while checking column existant,{e}')
        raise e

def add_day_name(dataframe,dateColumn):
    """
    Add a new column 'Day' representing the day of the week.
    Args:
        dataframe (pd.DataFrame): Pandas dataframe.
        dateColumn (str): Name of the date column.

    Returns:
        Pandas Dataframe: pandas dataframe DataFrame by adding the 'Day' column.
    
    Raises:
        ValueError: If the specified date column is not found.
    """
    try:
        if dateColumn in dataframe.columns.tolist():
            dataframe['Day']=dataframe[dateColumn].dt.day_name()
            return dataframe
        else:
            raise ValueError(f'Column {dateColumn} not found.')
    except Exception as e:
        logging.error(f'Error in add_day_name. {e}')
        raise e

def add_weekend(dataframe,dateColumn):
    """
    Add a new column 'IsWeekend' representing whether the day is a weekend.
    Args:
        dataframe (pd.DataFrame): Pandas dataframe.
        dateColumn (str): Name of the date column.

    Returns:
        Pandas Dataframe: pandas dataframe DataFrame by adding the 'IsWeekend' column.

    Raises:
        ValueError: If the specified date column is not found.
    """
    try:
        #creates a boolean mask that is True for Saturday and Sunday and False for other days of the week
        dataframe['IsWeekend']= dataframe[dateColumn].dt.dayofweek // 5 == 1
        return dataframe
    except Exception as e:
        logging.error(f'Error in add_weekend. {e}')
        raise e

def add_hemisphere(dataframe):
    """
    Adds a 'Hemisphere' column to the provided DataFrame based on the latitude values.
    Parameters:
    - dataframe (pandas.DataFrame): The DataFrame containing the 'LAT' column.
    Raises:
    - Exception: Raises an exception if there is an issue during the execution.
    Returns:
    - None: Modifies the input DataFrame in place by adding the 'Hemisphere' column.
    """
    try:
        # In the Northern Hemisphere, latitudes are positive, while in the Southern Hemisphere, latitudes are negative.
        dataframe['Hemisphere'] = dataframe['LAT'].apply(lambda x: 'Northern' if x >= 0 else 'Southern')
        return dataframe
    except Exception as e:
        raise e


def add_seasons(date,hemisphere):
    """
    Add a new column 'Season' and representing the season based on the month.
    Args:
        dataframe (pd.DataFrame): Pandas dataframe.
        dateColumn (str): Name of the date column.

    Returns:
        Pandas Dataframe: pandas dataframe DataFrame by adding the 'Season' column.

    Raises:
        ValueError: If the specified date column is not found.
    """
    try:
        month = date.month
        if hemisphere == 'Southern':
            season_month_south = {
                12: 'Summer', 1: 'Summer', 2: 'Summer',
                3: 'Autumn', 4: 'Autumn', 5: 'Autumn',
                6: 'Winter', 7: 'Winter', 8: 'Winter',
                9: 'Spring', 10: 'Spring', 11: 'Spring'}
            return season_month_south.get(month)

        elif hemisphere == 'Northern':
            season_month_north = {
                12: 'Winter', 1: 'Winter', 2: 'Winter',
                3: 'Spring', 4: 'Spring', 5: 'Spring',
                6: 'Summer', 7: 'Summer', 8: 'Summer',
                9: 'Autumn', 10: 'Autumn', 11: 'Autumn'}
            return season_month_north.get(month)

        else:
            raise ValueError('Invalid selection. Please select a hemisphere and try again')
    except Exception as e:
        logging.error(f'Error in add_seasons. {e}')
        raise e

def add_wind_category(dataframe,windColumn='WDSP'):
    """
    Create a new categorical column 'WindCategory' based on wind speed levels.
    Args:
        dataframe (pd.DataFrame): Pandas dataframe.
        windColumn (str): Name of the wind speed column. Default is 'WSDP'.

    Returns:
        Pandas Dataframe: pandas dataframe DataFrame by adding the 'WindCategory' column.

    Raises:
        ValueError: If the specified wind column is not found.
    """
    try:
        #create new categorical column that represents the wind speed levels as 'Low', 'Medium', or 'High' based on the specified bin edges and labels. 
        #This can be useful for creating categories or ranges for analysis or visualization purposes
        dataframe['WindCategory'] = pd.cut(dataframe[windColumn], bins=[0, 5, 10, float('inf')], labels=['Low', 'Medium', 'High'])
        return dataframe
    except Exception as e:
        logging.error(f'Error in add_wind_category. {e}')
        raise e
    
def add_temp_change(dataframe):
    """
    Calculate and add the temperature change to the provided DataFrame.
    The function calculates the temperature change by subtracting the minimum temperature
    from the maximum temperature and adds the result to a new column called 'TempChange'.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing temperature data.

    Raises:
        Exception: If there is an error during the calculation or assignment, an exception is raised.

    Returns:
        Pandas Dataframe: pandas dataframe DataFrame by adding the 'TempChange' column.
    """
    try:
        # Temperature Change
        dataframe['TempChange'] = round((dataframe['MAX'] - dataframe['MIN']),2)
        return dataframe
    except Exception as e:
        logging.error(f'Error in add_temp_change. {e}')
        raise e
    
def save_dataframe(dataframe,fileName,filePath):
    """
    Save a DataFrame to a specified file path.

    Args:
        dataframe (pd.DataFrame): The DataFrame to be saved.
        fileName (str): The name of the file to be created or overwritten.
        filePath (str): The path to the folder where the file will be saved.

    Raises:
        EmptyDataError: If the input DataFrame is empty.
        Exception: If there is an error during the save operation, an exception is raised.

    Returns:
        None: Flag indictaing success or failure
    """
    try:
        # define the full path
        fullPath=filePath+'/'+fileName

        # check for empty dataframe, if not then save the dataframe
        flag=False
        if dataframe.empty:
            raise EmptyDataError('Empty Dataframe.')
        else:
            dataframe.to_csv(fullPath,index=False)
            flag=True
            logging.info(f'File {fullPath} saved successfully.')
        return flag
    except Exception as e:
        logging.error(f'Error in save_dataframe. {e}')
        raise e
    
def merge_dataframe(leftDataframe,rightDataframe,leftOnKey,rightOnKey,joinType):
    """
    Merge two DataFrames based on specified keys and join type.

    Parameters:
    - leftDataframe (pandas.DataFrame): The left DataFrame to be merged.
    - rightDataframe (pandas.DataFrame): The right DataFrame to be merged.
    - leftOnKey (str or list of str): The key(s) in the left DataFrame for the merge.
    - rightOnKey (str or list of str): The key(s) in the right DataFrame for the merge.
    - joinType (str): The type of join to be performed ('inner', 'outer', 'left', 'right').

    Returns:
    pandas.DataFrame: The merged DataFrame.

    Raises:
    - EmptyDataError: If any of the given DataFrames is empty.
    - Exception: If an error occurs during the merge process.
    """
    logging.info('Merging satrted.')
    try:
        if leftDataframe.empty or rightDataframe.empty:
            raise EmptyDataError('Any of the given dataframe is empty.')
        if joinType in ['inner', 'outer', 'left', 'right']:
            final_df=leftDataframe.merge(rightDataframe,right_on=rightOnKey,left_on=leftOnKey,how=joinType)
            return final_df
        else:
            raise ValueError('Not a valid join type.')
    except Exception as e:
        logging.error(f'Error in merge_dataframe.{e}')
        raise e

def add_visibilityCategory(dataframe):
    """
    Categorizes visibility in miles into different categories: 'Very Low', 'Low', 'Moderate', 'High', 'Very High', 'Excellent'.
    Args:
        dataframe (pd.DataFrame): The input DataFrame containing the 'VISIB' column representing visibility in miles.

    Returns:
        pd.DataFrame: The DataFrame with an additional column 'VisibilityCategory' indicating the visibility category.
    """
    logging.info('add_visibilityCategory started.')
    try:
        # defined the bins for the category ['Very Low' < 'Low' < 'Moderate' < 'High' < 'Very High' < 'Excellent']
        bins = [-float('inf'), 5, 10, 20, 30, 40, float('inf')]
        labels = ['Very Low', 'Low', 'Moderate', 'High', 'Very High', 'Excellent']
        dataframe['VisibilityCategory'] = pd.cut(dataframe['VISIB'], bins=bins, labels=labels)
        return dataframe
    except Exception as e:
        logging.error(f'Error in add_visibilityCategory.{e}')
        raise e

def add_weatherType(dataframe):
    """
    Adds new columns for weather types based on the 'FRSHTT' column, where each indicator represents
    the occurrence of specific weather events (Fog, Rain, Snow, Hail, Thunder, Tornado).

    Indicators (1 = yes, 0 = no/not reported) for the occurrence during the day of:
    Fog ('F' - 1st digit).
    Rain or Drizzle ('R' - 2nd digit).
    Snow or Ice Pellets ('S' - 3rd digit).
    Hail ('H' - 4th digit).
    Thunder ('T' - 5th digit).
    Tornado or Funnel Cloud ('T' - 6th       
    digit).

    Args:
        dataframe (pd.DataFrame): The input DataFrame containing the 'FRSHTT' column with weather type indicators.

    Returns:
        pd.DataFrame: The DataFrame with new columns representing individual weather types.
    """
    logging.info('add_weatherType started.')
    try:
        # Add new columns based on 'FRSHTT'
        dataframe['FRSHTT']=dataframe['FRSHTT'].astype(str)
        dataframe['Fog'] = dataframe['FRSHTT'].str[0]
        dataframe['Rain'] = dataframe['FRSHTT'].str[1]
        dataframe['Snow'] = dataframe['FRSHTT'].str[2].replace('.',np.nan)
        dataframe['Hail'] = dataframe['FRSHTT'].str[3].replace('.',np.nan)
        dataframe['Thunder'] = dataframe['FRSHTT'].str[4].replace('.',np.nan)
        dataframe['Tornado'] = dataframe['FRSHTT'].str[5].replace('.',np.nan)
        return dataframe
    except Exception as e:
        logging.error(f'Error in add_weatherType.{e}')
        raise e

def add_features(weatherDataframe,stationDataframe,countryDataframe,dateColumn):
    """
    Perform various data transformations and feature additions to the provided DataFrame.
    The function checks for the existence of required columns, adds a 'Day' column with day names,
    identifies weekends with an 'IsWeekend' column, adds a 'Season' column based on the 'Month' column,
    calculates temperature change with a 'TempChange' column, ,categorizes wind speed into 'WindCategory',
    categorizes FRSHTT  into 'weather category' and categorizes VISIB  into 'visible categories'

    Args:
        weatherDataframe (pd.DataFrame): The input DataFrame containing weather data.
        stationDataframe (pd.DataFrame): The input DataFrame containing station details data.
        countryDataframe (pd.DataFrame): The input DataFrame containing country name data.
        dateColumn (str): The name of the column representing dates.

    Raises:
        EmptyDataError: If the input DataFrame is empty.
        Exception: If there is an error during any data transformation, an exception is raised.

    Returns:
        pd.DataFrame: The modified DataFrame with additional features.
    """
    logging.info('add_features started.')
    try:
        if weatherDataframe.empty or stationDataframe.empty or countryDataframe.empty:
            raise EmptyDataError('Any of the given dataframe is empty.')
        
        # check for column existent
        checkColumn=check_columns(dataframe=weatherDataframe,listColumns=[dateColumn])
        if checkColumn:

            # add day
            df=add_day_name(weatherDataframe,dateColumn)

            # add weekend
            df=add_weekend(df,dateColumn)

            # add Tempprature change
            df=add_temp_change(df)

            # add wind category
            df=add_wind_category(df,'WDSP')

            # add visibility categories
            df=add_visibilityCategory(df)

            # add weather categories
            df=add_weatherType(df)
            print(df.head())
            # merge station and country dataframe
            merged_df=merge_dataframe(leftDataframe=stationDataframe,rightDataframe=countryDataframe,rightOnKey='FIPS',leftOnKey='CTRY',joinType='left')
        

            # merge with weather dataframe
            final_df=merge_dataframe(leftDataframe=df,rightDataframe=merged_df,rightOnKey=['USAF','WBAN'],leftOnKey=['STN','WBAN'],joinType='left')

            # add Hemisphere (Northern and Southern)
            final_df=add_hemisphere(final_df)
            # add seasons based on Hemisphere
            final_df['Season'] = final_df.apply(lambda row: add_seasons(row['DATE'], row['Hemisphere']), axis=1)

            final_df=final_df[config.SELECTED_COLUMNS]
            if final_df.empty:
                raise EmptyDataError('Processed dataframe is empty.')
            return final_df
        else:
            raise ValueError('Not all column present in dataframe.')
    except Exception as e:
        logging.error(f'Error in add_features.{e}')
        raise e

