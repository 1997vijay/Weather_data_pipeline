"""
Perform some data quality checks on the dataframe.

Dependencies:
    - pandas: A python library for data analysis.
    - scipy: SciPy is a powerful tool for scientific computing in Python.

Usage:
    1. Ensure that you have the required dependencies installed.
    2. Run the code to perform data quality checks on dataframe.

    Author Information:
    Name: Vijay Kumar
    Date: 24 Dec 2023

Abstract/Description:
Below code is used to perform the data quality checks on pandas dataframe.
It can be used to check and remove duplicates, convert to datetime format, check for outliers etc.

Change Log:
    - 24 Dec 2023: Initial creation.
"""

import pandas as pd
import logging
from scipy.stats import zscore

import warnings
warnings.filterwarnings('ignore')

class DataQualityCheck:
    """
    A class for processing and analyzing tabular data.
    Attributes:
        data (pd.DataFrame): The input DataFrame.
    Methods:
        convert_to_dateformat(column_name, date_format='%Y-%m-%d'): Convert a column to a specified date format.
        remove_duplicates_keep_latest(key_columns, date_column): Remove duplicates based on key columns, keeping the latest record.
        check_outliers_zscore(numeric_column, zscore_threshold=3): Check for outliers using Z-score.
    """
    def __init__(self, data):
        """
        Initialize the DataProcessor with a DataFrame.
        Args:
            data (pd.DataFrame): The input DataFrame.
        """
        self._data = data

    def convert_to_dateformat(self, column_name, date_format='%Y-%m-%d'):
        """
        Convert a column to a specified date format.
        Args:
            column_name (str): The name of the column to be converted.
            date_format (str, optional): The desired date format. Default is '%Y-%m-%d'.
        """
        try:
            if column_name in self._data.columns.tolist():
                self._data[column_name] = pd.to_datetime(self._data[column_name], format=date_format)
                return self._data
            else:
                raise ValueError(f'Column {column_name} not present.')
        except Exception as e:
            raise ValueError(e)

    def remove_duplicates(self, key_columns, date_column):
        """
        Remove duplicates based on key columns, keeping the latest record.
        Args:
            key_columns (list): List of column names used as key for identifying duplicates.
            date_column (str): The column representing dates for determining the latest record.
        """
        try:
            if all(column in self._data.columns.tolist() for column in key_columns) and date_column in self._data.columns.tolist():
                # Count duplicates before removal
                duplicates_before = self._data.duplicated(subset=key_columns).sum()

                # Sort the DataFrame by date_column in descending order
                self._data = self._data.sort_values(by=[date_column], ascending=False)

                # Drop duplicates based on key_columns, keeping the first occurrence (latest record)
                self._data = self._data.drop_duplicates(subset=key_columns, keep='first')

                # Count duplicates after removal
                duplicates_after = duplicates_before - len(self._data)

                logging.info(f"Number of duplicates removed: {duplicates_after}")

                return self._data
            else:
                raise ValueError('One or more key columns or date column not found in DataFrame.')
        except Exception as e:
            raise e
    
    def change_data_type(self,dataType,column):
        """
        Change the data type of a specified column in the DataFrame.

        Parameters:
        - dataType (str): The target data type to which the column should be converted (e.g., 'int', 'float', 'str').
        - column (str): The name of the column in the DataFrame.

        Returns:
        pandas.DataFrame: The DataFrame with the specified column converted to the target data type.

        Raises:
        - ValueError: If the specified column is not present in the DataFrame.
        - Exception: If an error occurs during the data type conversion process.
        """
        if column in self._data.columns.tolist():
            try:
                self._data[column]=self._data[column].astype(dataType)
                return self._data
            except Exception as e:
                logging.error(f'Error in change_data_type.{e}')
                raise e
        else:
            raise ValueError('Column Not resent')


    def check_outliers(self, numeric_column, zscore_threshold=3):
        """
        Check for outliers using Z-score.
        Args:
            numeric_column (str): The name of the numeric column to check for outliers.
            zscore_threshold (float, optional): Z-score threshold for identifying outliers. Default is 3.

        Returns:
            pd.DataFrame: DataFrame containing outliers.
        """
        try:
            z_scores = zscore(self._data[numeric_column])
            outliers = self._data[abs(z_scores) > zscore_threshold]
            return outliers
        except Exception as e:
            raise e

