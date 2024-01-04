"""
Performs Change Data Capture on the provided DataFrames using the specified key columns.

Dependencies:
    - pandas: A powerful data manipulation library.
    - logging: A library to log the info
    - hashlib: Provides the SHA-224, SHA-256, SHA-384, SHA-512 hash algorithms. Can be used to generate the unique hash value for each record. 

Usage:
    1. Ensure that you have the required dependencies installed.
    2. Run the code to the pandas dataframe with CDC.

    Author Information:
    Name: Vijay Kumar
    Date: 14 Dec 2023

Abstract/Description:
Performs Change Data Capture (CDC) on source and history files and outputs a Pandas DataFrame
with a 'CDC_Flag' column denoting the type of change ('I' for insert, 'U' for update, 'D' for delete).

Change Log:
    - 14 Dec 2023: Initial creation.
"""

import logging
import pandas as pd
import hashlib

class CDCGenerator:
    """
    Class for generating a Pandas DataFrame containing change data information
    based on a specified key columns.
    Attributes:
        keyColumn (list): List of key columns used for Change Data Capture (CDC).
        sourceDataframe: Source file dataframe
        previousDataframe: History file dataframe
    """

    def __init__(self, keyColumn,sourceDataframe,previousDataframe) -> None:
        """
        Constructor for initializing the CDCGenerator instance.
        Args:
            keyColumn (str): The key column name from the DataFrame.
            sourceDataframe: Source file dataframe
            previousDataframe: History file dataframe
        """
        self._keyColumn=keyColumn
        self._sourceDataframe=sourceDataframe
        self._previousDataframe=previousDataframe
    
        # check for key column
        if len(self._keyColumn)!=0:
            missingColumns=[column for column in self._keyColumn if column not in self._sourceDataframe.columns.tolist()]
            if len(missingColumns)!=0:
                return logging.error(f'One or more key columns not found in the source DataFrame. {missingColumns}')
        else:
            return logging.error('Empty columns list')
        
        # check for empty dataframe
        if self._sourceDataframe.empty or self._previousDataframe.empty:
            return logging.error('Empty Dataframe')
        
        # generate hash value. axis=1 means that the function will be applied to each row
        self._sourceDataframe['HashValue'] = self._sourceDataframe.apply(self._generate_hash, axis=1)
        self._previousDataframe['HashValue'] = self._previousDataframe.apply(self._generate_hash, axis=1)

    def _generate_hash(self,row):
        """
        Generate a unique hash value for each row
        Arguments:
            row -- dataframe row
        Returns:
            Hash value for each dataframe
        """
        # Convert each value to bytes and calculate the hash
        hash_object = hashlib.md5()
        for value in row.values:
            hash_object.update(str(value).encode('utf-8'))
        return hash_object.hexdigest()

    def _insert_cdc(self, row, previousDF):
        """
        Perform Change Data Capture (CDC) based on the provided row and previous DataFrame.
        Args:
            row (pd.Series): The current row from the DataFrame to check for changes.
            previousDF (pd.DataFrame): The DataFrame containing previous data for comparison.
        Returns:
            str: A CDC flag indicating the type of change ('I' for insert, 'N' for no change).
        """
        current_product_id = row[self._keyColumn]
        
        # Check if any row in previousDF matches the conditions
        for _, prev_row in previousDF.iterrows():
            if all(current_product_id == prev_row[self._keyColumn]):
                return 'N'  # Return 'N' that means no new row added
        
        # If no match is found, return 'I', indicating new row
        return 'I'
        

    def _delete_cdc(self,row, previousDF):
        """
        Perform Change Data Capture (CDC) based on the provided row and previous DataFrame.
        Args:
            row (pd.Series): The current row from the DataFrame to check for changes.
            previousDF (pd.DataFrame): The DataFrame containing previous data for comparison.
        Returns:
            str: A CDC flag indicating the type of change ('D' for delete, 'N' for no change).
        """
        current_product_id = row[self._keyColumn]
        
        # Check if any row in previousDF matches the conditions
        for _, prev_row in previousDF.iterrows():
            if all(current_product_id == prev_row[self._keyColumn]):
                return 'N'  # Return 'N' as soon as a match is found
        
        # If no match is found, return 'D',indicating 'delete'
        return 'D'

    def _update_cdc(self, row, previousDF):
        """
        Perform Change Data Capture (CDC) for an update operation based on the provided row and previous DataFrame.
        Args:
            row (pd.Series): The current row from the DataFrame to check for updates.
            previousDF (pd.DataFrame): The DataFrame containing previous data for comparison.
        Returns:
            str: A CDC flag indicating the type of change ('U' for update, 'N' for no change).
        """
        current_product_id = row[self._keyColumn]
        current_hash_value = row['HashValue']
        
        # Check if any row in previousDF matches the conditions
        for _, prev_row in previousDF.iterrows():
            if (current_product_id == prev_row[self._keyColumn]).all() and current_hash_value != prev_row['HashValue']:
                return 'U'  # Return 'U' as soon as a match is found, indicating update
        
        # If no match is found, return 'N'
        return 'N'
    
    def perform_cdc(self):
        """
        Perform Change Data Capture (CDC) operation on the given source DataFrame and history DataFrame.
        Returns:
            pd.DataFrame: A DataFrame with a CDC flag indicating the type of change ('I' for insert,
                        'U' for update, 'D' for delete).
        """
        # copy the dataframe before performing CDC
        insert_df=self._sourceDataframe.copy()
        delete_df=self._previousDataframe.copy()
        update_df=self._sourceDataframe.copy()

        try:
            # call the function for each dataframe
            insert_df['CDC_Flag'] = insert_df.apply(self._insert_cdc, axis=1, previousDF=self._previousDataframe)
            delete_df['CDC_Flag'] = delete_df.apply(self._delete_cdc, axis=1, previousDF=self._sourceDataframe)
            update_df['CDC_Flag'] = update_df.apply(self._update_cdc, axis=1, previousDF=self._previousDataframe)

            # concat all dataframe into final dataframe.
            # filter records with flag I ,D and U. we dont want any records with flag N which indicates No change.
            insert_df = insert_df[insert_df['CDC_Flag']=='I'].drop('HashValue', axis=1).reset_index(drop=True)
            delete_df = delete_df[delete_df['CDC_Flag']=='D'].drop('HashValue', axis=1).reset_index(drop=True)
            update_df = update_df[update_df['CDC_Flag']=='U'].drop('HashValue', axis=1).reset_index(drop=True)

            # combine all dataframe
            cdcDataframe = pd.concat([insert_df, update_df, delete_df]).reset_index(drop=True)
            if cdcDataframe.empty:
                return logging.error('No changed data.')
            else:
                return cdcDataframe
        except Exception as e:
            return logging.error(f'Error while performing CDC. {e}')
