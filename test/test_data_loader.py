"""
Test cases for data_loader file.

Dependencies:
    - requests: A Python library for interacting with APIs.
    - os: An operating system library for interacting with the system.
    - pytest: A library testing the code.
Change Log:
    - 31st Dec 2023: Initial creation.
"""
import pytest
import os
import requests
import logging
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup
from src.data_loader import get_listOfYears ,save_file

# Mock responses for testing HTML content
@pytest.fixture
def mock_html_content():
    BASE_URL="https://www1.ncdc.noaa.gov/pub/data/gsod/"
    response=requests.get(BASE_URL)
    return response

# Test case for extracting years
def test_get_listOfYears_extract_years(mock_html_content):
    response_mock = type("Response", (object,), {"text": mock_html_content})
    result = get_listOfYears(response_mock, currentTimestamp=datetime.now(), isIncrementalLoad=False)
    assert len(result) == 96

# Test case for extracting years with incremental load
def test_get_listOfYears_incremental_load(mock_html_content):
    response_mock = type("Response", (object,), {"text": mock_html_content})
    result = get_listOfYears(response_mock, currentTimestamp=datetime.now(), isIncrementalLoad=True)
    assert result == ['2023', '2024']

# Test case for handling an exception during extraction
def test_get_listOfYears_exception_handling():
    with pytest.raises(Exception):
        get_listOfYears(response=None, currentTimestamp=datetime.now(), isIncrementalLoad=False)


# url response
@pytest.fixture
def file_response():
    url='https://www1.ncdc.noaa.gov/pub/data/gsod/2023/010010-99999-2023.op.gz'
    response=requests.get(url).content
    return response

# pandas dataframe
@pytest.fixture
def pandas_dataframe():
    data = {'FIPS ID': ['AA', 'AC', 'AF'], 'Name': ['ARUBA', 'ANTIGUA AND BARBUDA', 'AFGHANISTAN']}
    df = pd.DataFrame(data)
    return df

def test_save_file_url_response(tmpdir, caplog, file_response):
    # Arrange
    output_path = 'data/resources'
    file_name = 'test_file.csv'
    blob_name = 'test_blob_name'
    caplog.set_level(logging.INFO)

    # Act
    save_file(file_response, file_name, output_path, blob_name)

    # Assert
    assert "Saving to local storage." in caplog.text
    assert os.path.exists(os.path.join(output_path, file_name.split("/")[-1]))

def test_save_file_dataframe(tmpdir, caplog, pandas_dataframe):
    # Arrange
    output_path = 'data/resources'
    file_name = 'test_file.csv'
    blob_name = 'test_blob_name'
    caplog.set_level(logging.INFO)

    # Act
    save_file(pandas_dataframe, file_name, output_path, blob_name)

    # Assert
    assert "Saving to local storage." in caplog.text
    assert os.path.exists(os.path.join(output_path, file_name.split("/")[-1]))