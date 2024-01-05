"""
Test cases for data_cleaner file.

Dependencies:
    - requests: A Python library for interacting with APIs.
    - os: An operating system library for interacting with the system.
    - pytest: A library testing the code.
Change Log:
    - 5th Jan 2024: Initial creation.
"""

import os
import pandas as pd
import pytest
from src.data_cleaner import read_data_files,read_data ,clean_columnName

# Fixture for a valid directory path containing files
@pytest.fixture
def valid_directory():
    return 'data/raw'

# Fixture for multiple valid directory paths containing files
@pytest.fixture
def valid_directories():
    return ['data/raw/2023', 'data/raw/2024']


# Fixture for an empty list of file paths
@pytest.fixture
def empty_file_paths():
    return []

# Fixture for a non-existing directory path
@pytest.fixture
def non_existing_directory():
    return '/data/raw/2023/non/existing/directory'

# Test case for reading files from a valid directory
def test_read_data_files_valid_extension(valid_directory):
    result_df = read_data_files(valid_directory, 'csv')
    assert result_df is not None
    assert isinstance(result_df, pd.DataFrame)


# Test case for attempting to read files from an empty list of file paths
def test_read_data_files_empty_file_paths(empty_file_paths):
    with pytest.raises(ValueError):
        read_data_files(empty_file_paths, 'csv')

# Test case for attempting to read files from a non-existing directory
def test_read_data_files_non_existing_directory(non_existing_directory):
    with pytest.raises(ValueError):
        read_data_files(non_existing_directory, 'csv')

# Test case for attempting to read files with an invalid file extension
def test_read_data_files_invalid_extension(valid_directory):
    with pytest.raises(ValueError):
        read_data_files(valid_directory, 'invalid_extension')


@pytest.fixture
def valid_csv_file(tmpdir):
    file_path = tmpdir.join("test_valid.csv")
    data = "ID,Name\n1,Alice\n2,Bob\n3,Charlie"
    file_path.write(data)
    return str(file_path)

# Fixture for an invalid file format (e.g., a text file)
@pytest.fixture
def invalid_file_format(tmpdir):
    file_path = tmpdir.join("test_invalid.txt")
    data = "ID,Name\n1,Alice\n2,Bob\n3,Charlie"
    file_path.write(data)
    return str(file_path)

def test_read_data_valid_file(valid_csv_file):
    result = read_data(valid_csv_file)
    assert isinstance(result, pd.DataFrame)
    assert result.shape == (3, 2)

def test_read_data_invalid_file_format(invalid_file_format):
    with pytest.raises(ValueError, match="Not a valid file format."):
        read_data(invalid_file_format)


# Fixture for a DataFrame with columns containing "---"
@pytest.fixture
def dataframe_with_dash_columns():
    data = {'col1---': [1, 2, 3], 'col2': [4, 5, 6], 'col3---': [7, 8, 9]}
    df = pd.DataFrame(data)
    return df

# Fixture for a DataFrame without columns containing "---"
@pytest.fixture
def dataframe_without_dash_columns():
    data = {'col1': [1, 2, 3], 'col2': [4, 5, 6], 'col3': [7, 8, 9]}
    df = pd.DataFrame(data)
    return df

def test_clean_columnName_with_dash_columns(dataframe_with_dash_columns):
    result = clean_columnName(dataframe_with_dash_columns)
    assert '---' not in result.columns
    assert result.columns.tolist() == ['col1', 'col2', 'col3']

def test_clean_columnName_without_dash_columns(dataframe_without_dash_columns):
    result = clean_columnName(dataframe_without_dash_columns)
    assert result.equals(dataframe_without_dash_columns)