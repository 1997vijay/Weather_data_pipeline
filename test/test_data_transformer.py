"""
Test cases for data_transformer file.

Dependencies:
    - requests: A Python library for interacting with APIs.
    - os: An operating system library for interacting with the system.
    - pytest: A library testing the code.
Change Log:
    - 5th Jan 2024: Initial creation.
"""

import pandas as pd
import logging
import numpy as np
import pytest
from src.data_transformer import *

# Fixture for a sample DataFrame with date column
@pytest.fixture
def sample_dataframe_with_date():
    data = {'date_column': pd.to_datetime(['2022-01-01', '2022-02-01', '2022-03-01']),
            'LAT': [10, -20, 30],
            'WDSP': [4, 8, 12],
            'MAX': [25, 30, 22],
            'MIN': [15, 20, 10]}
    df = pd.DataFrame(data)
    return df

def test_add_day_name(sample_dataframe_with_date):
    # Act
    result = add_day_name(sample_dataframe_with_date, dateColumn='date_column')

    # Assert
    assert 'Day' in result.columns
    assert result['Day'].tolist() == ['Saturday', 'Tuesday', 'Tuesday']

def test_add_weekend(sample_dataframe_with_date):
    # Act
    result = add_weekend(sample_dataframe_with_date, dateColumn='date_column')

    # Assert
    assert 'IsWeekend' in result.columns
    assert result['IsWeekend'].tolist() == [True, False, False]

def test_add_hemisphere(sample_dataframe_with_date):
    # Act
    result = add_hemisphere(sample_dataframe_with_date)

    # Assert
    assert 'Hemisphere' in result.columns
    assert result['Hemisphere'].tolist() == ['Northern', 'Southern', 'Northern']

def test_add_seasons():
    # Act
    result_northern = add_seasons(pd.to_datetime('2022-06-01'), hemisphere='Northern')
    result_southern = add_seasons(pd.to_datetime('2022-06-01'), hemisphere='Southern')

    # Assert
    assert result_northern == 'Summer'
    assert result_southern == 'Winter'

def test_add_wind_category(sample_dataframe_with_date):
    # Act
    result = add_wind_category(sample_dataframe_with_date, windColumn='WDSP')

    # Assert
    assert 'WindCategory' in result.columns
    assert result['WindCategory'].tolist() == ['Low', 'Medium', 'High']

def test_add_temp_change(sample_dataframe_with_date):
    # Act
    result = add_temp_change(sample_dataframe_with_date)

    # Assert
    assert 'TempChange' in result.columns
    assert result['TempChange'].tolist() == [10, 10, 12]


@pytest.fixture
def sample_dataframe_weather():
    data = {'VISIB': [3, 8, 15, 25, 35, 50],
            'FRSHTT': ['10000', '01000', '00100', '00010', '00001', '11111']}
    df = pd.DataFrame(data)
    return df

def test_add_visibilityCategory(sample_dataframe_weather):
    # Act
    result = add_visibilityCategory(sample_dataframe_weather)

    # Assert
    assert 'VisibilityCategory' in result.columns
    assert result['VisibilityCategory'].tolist() == ['Very Low', 'Low', 'Moderate', 'High', 'Very High', 'Excellent']

def test_add_weatherType(sample_dataframe_weather):
    # Act
    result = add_weatherType(sample_dataframe_weather)

    # Assert
    assert 'Fog' in result.columns
    assert 'Rain' in result.columns
    assert 'Snow' in result.columns
    assert 'Hail' in result.columns
    assert 'Thunder' in result.columns
    assert 'Tornado' in result.columns

    assert result['Fog'].tolist() == ['1', '0', '0', '0', '0', '1']
    assert result['Rain'].tolist() == ['0', '1', '0', '0', '0', '1']
    assert result['Snow'].tolist() == [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
    assert result['Hail'].tolist() == [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
    assert result['Thunder'].tolist() == [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
    assert result['Tornado'].tolist() == [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]



def sample_dataframes():
    left_data = {'ID': [1, 2, 3], 'Name': ['Alice', 'Bob', 'Charlie']}
    right_data = {'ID': [1, 2, 4], 'Age': [25, 30, 35]}
    left_df = pd.DataFrame(left_data)
    right_df = pd.DataFrame(right_data)
    return left_df, right_df

def test_merge_dataframe_inner(sample_dataframes):
    # Arrange
    left_df, right_df = sample_dataframes

    # Act
    result = merge_dataframe(left_df, right_df, 'ID', 'ID', 'inner')

    # Assert
    assert 'Name' in result.columns
    assert 'Age' in result.columns
    assert result.shape == (2, 3)

def test_merge_dataframe_outer(sample_dataframes):
    # Arrange
    left_df, right_df = sample_dataframes

    # Act
    result = merge_dataframe(left_df, right_df, 'ID', 'ID', 'outer')

    # Assert
    assert 'Name' in result.columns
    assert 'Age' in result.columns
    assert result.shape == (3, 3)

def test_merge_dataframe_invalid_join_type(sample_dataframes):
    # Arrange
    left_df, right_df = sample_dataframes

    # Act and Assert
    with pytest.raises(ValueError, match='Not a valid join type.'):
        merge_dataframe(left_df, right_df, 'ID', 'ID', 'invalid_join')