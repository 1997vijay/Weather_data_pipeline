"""
Download data from a specified URL, extract the contents, and save the data in the 'raw' folder.

Dependencies:
    - requests: A Python library for interacting with APIs.
    - os: An operating system library for interacting with the system.
    - logging: A library for logging information.

Usage:
    1. Ensure that you have the required dependencies installed.
    2. Confirm the correctness of the file path.
    3. Run the code to download, extract, and save the file.

    Author Information:
    Name: Vijay Kumar
    Date: 23 Dec 2023

Abstract/Description:
This script downloads files from a specified URL, extracts their contents, and saves them in the 'raw' folder.

Change Log:
    - 23 Dec 2023: Initial creation.
"""

# import all required libraries
from io import BytesIO
from bs4 import BeautifulSoup
from datetime import datetime,timedelta

from cloudStorageSaver import CloudStorageSaver

import requests
import pandas as pd
import os
import logging
import config
import gzip
import shutil

#set the yesterday timestamp for file timestamp comparison
currentTimestamp=datetime.now()-timedelta(days=2)

logFileName='data_pipeline.log'
logFolder='log'
logFilePath=os.path.join(os.getcwd(),logFolder,logFileName)
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',filename=logFilePath,filemode='w')

def save_file(fileResponse,fileName,outputPath,blobName,client=None):
    """
        Save the contents of a file received in a response to a specified output path.
        Args:
            fileResponse (Response): The response object containing the file content.
            fileName (str): The name of the file to be saved.
            outputPath (str): The path to the output folder where the file will be saved.
        Returns:
            None: The function saves the file to the specified output path.
        Raises:
            Exception: If there is an error while saving the file, an exception is raised and logged.
        """
    try:
        # Convert DataFrame to CSV in-memory otherwise try to replce the file extension and store it in memory
        try:
            file_content = BytesIO()
            fileResponse.to_csv(file_content, index=False)
            file_content.seek(0)
        except:
            fileName=fileName.replace("op.gz","op")
            output_path = os.path.join(outputPath,fileName.split("/")[-1])
            file_content = BytesIO(fileResponse)

        # Extract and Save the content to the output path
        if config.SAVE_TO_CLOUD:
                logging.info(f'Saving to {config.CLOUD_NAME} storage.')
                client.upload_file(containerName=config.CONTAINER_NAME,blobName=blobName,data=file_content,fileName=fileName)
        else:
            logging.info('Saving to local storage.')
            # Extract and Save the content to the output path
            with gzip.open(file_content, 'rb') as fileIn:
                with open(output_path, "wb") as file:
                    shutil.copyfileobj(fileIn, file)
    except Exception as e:
        logging.error(f'Error while saving the file!.{e}')

def download_other_file(url,path,fileName):
    """
    Download a file from the specified URL and save it to the given path with the specified filename.

    Parameters:
    - url (str): The URL of the file to be downloaded.
    - path (str): The path where the downloaded file should be saved.
    - fileName (str): The name of the file to be saved.
    Raises:
    - Exception: If an error occurs during the download or file saving process.
    """
    logging.info('download_other_file started.')
    try:
        logging.info(f'File url --> {url}')
        output_path = os.path.join(path,fileName)
        # Make a request to get the content of the URL
        response = requests.get(url)
        if response.status_code == 200:
            with open(output_path, "wb") as file:
                file.write(response.content)
    except Exception as e:
        logging.error(f'Error in download_station_file. {e}')
        raise e

   
def get_listOfYears(response, currentTimestamp,isIncrementalLoad):
    """
    Extracts and compares file timestamps from the HTML content of a response.
    The function uses BeautifulSoup to parse the HTML content and selects
    the second <td> element in each row using 'nth-of-type' selector.
    It then attempts to convert the timestamp string to a datetime object
    and compares it with the provided current timestamp.

    Args:
        response (Response): The response object containing HTML content.
        isIncrementalLoad (bool, optional): If True, only modified files will be downloaded if False all files will be downloaded.

    Returns:
        list : Returns a list of years.

    Raises:
        Exception: If there is an error during the extraction or comparison process,
                   an exception is raised and logged.
    """
    logging.info('get_listOfYears started.')
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    listYear=[]
    try:
        # Use 'nth-of-type' selector to select only the second <td> element
        years = soup.find_all('a')[5:]
        fileTimestamps=soup.select('td:nth-of-type(2)')[1:]
        for year,fileTime in zip(years,fileTimestamps) :
            # Convert the string to a datetime object
            timestamp_dt = datetime.strptime(fileTime.get_text().strip(), '%Y-%m-%d %H:%M')
            year=year.get_text().split("/")[0]
            
            if isIncrementalLoad:
              if timestamp_dt >= currentTimestamp:
                if year.isnumeric():
                  print(f"Modified Directory: {fileTime.get_text().strip()}-->{year}")
                  listYear.append(year)
            else:
              if year.isnumeric():
                listYear.append(year)
        return listYear
    except Exception as e:
        logging.error(f'Error in get_listOfYears. {e}')
        raise e

def download_files(url,connectionString,cloudName,fileOutputPath,saveToCloud,fileName,isIncrementalLoad,sampleFile,blobName):
    """
    Download files from a specified URL and save them to the output path.
    
    If a specific `fileName` is provided, the function downloads only that file. If `fileName` is None,
    the function parses the HTML content of the URL to find links to files, iterates through each link,
    and downloads the corresponding files.
    
    Args:
        url (str): The URL from which files will be downloaded.
        connectionString (str): Connection string required for cloud storage, if saveToCloud is True.
        cloudName (str): Name of the cloud storage service.
        fileOutputPath (str): The local path where the files will be saved.
        isIncrementalLoad (bool): A flag indicating whether to download the historic files.
        sampleFile (int): The number of sample files to download.
        saveToCloud (bool, optional): If True, files will be saved to cloud storage; if False, files will be saved locally. Defaults to False.
        fileName (str, optional): The name of the specific file to download. If None, download all files. Defaults to None.
        blobName : Azure blob name
    
    Returns:
        None: The function downloads and saves the file(s) to the specified output path.
    
    Raises:
        Exception: If there is an error during the download or saving process, an exception is raised and logged.
    """
    logging.info('download_files started')
    try:
        listOfYears=[]
        if fileName!=None:
            fullUrl=url+'/'+fileName
        else:
            fullUrl=url
        logging.info(f'File url --> {fullUrl}')

        # if saveToCloud True --> create cloud storage instance
        if saveToCloud:
                client=CloudStorageSaver(cloudName=cloudName,connection_string=connectionString)
        else:
            client=None

        # Make a request to get the content of the URL
        response = requests.get(fullUrl)
        if response.status_code == 200:
            if fileName is None:
                listOfYears=get_listOfYears(response,currentTimestamp,isIncrementalLoad)
                for year in listOfYears:
                    logging.info(f'downloading file for year {year}')

                    fullUrl=url+f'{year}'
                    new_fileOutputPath=fileOutputPath

                    # check if dir is present. if not create a directory with the given year
                    new_fileOutputPath=new_fileOutputPath+f'/{year}'
                    if not os.path.exists(new_fileOutputPath):
                        os.makedirs(new_fileOutputPath)

                    # Parse the HTML content to find links to files
                    html_content = requests.get(fullUrl).text
                    fileLinks = [(fullUrl+'/'+BeautifulSoup(line, 'html.parser').find('a').text)for line in html_content.split('\n') if ".op.gz" in line]
                    logging.info(f'Downloading {sampleFile} files.')

                    fileLinks=list(set(fileLinks))
                    # iterate through each link and download the file
                    for link in fileLinks[:sampleFile]:
                        # Extract the file name from the URL
                        fileName = link.split("/")[-1]

                        # Send a GET request to the URL
                        # if saveToCloud True then save file data to cloud storage else save in local
                        response = requests.get(link)
                        if response.status_code==200:
                            save_file(fileResponse=response.content,fileName=year+'/'+fileName,outputPath=new_fileOutputPath,blobName=blobName,client=client)
                        else:
                            logging.error('Error while downlaod file.')
            else:
                # Extract the file name from the URL
                fileName = fullUrl.split("/")[-1]
                logging.info(f'File name -->{fileName}')
                save_file(fileResponse=response,fileName=fileName,outputPath=fileOutputPath,client=client)
                logging.info('File downloaded successfully.')
        return listOfYears
    except Exception as e:
        logging.error(f'Error while downloading the file.{e}')
        raise e

def extract_data(url,connectionString,cloudName,fileOutputPath,saveToCloud,fileName,isIncrementalLoad,sampleFile,blobName):
    """
    Main function to run the data pipeline.
    Args:
        fileName (str, optional): The name of the specific file to download. If None, download all files.Defaults to None.
    Returns:
        None: The function runs the data pipeline to download and save the file(s).
    Raises:
        Exception: If there is an error during the pipeline execution, an exception is raised and logged.
    """
    logging.info(f'extract_data function started.')
    try:
        # first download station details file 
        download_other_file(config.STATION_NAME_URL,config.STATION_FILE_OUTPUT_PATH,config.STATION_FILE_NAME)

        # download country name file
        download_other_file(config.COUNTRY_URL,config.COUNTRY_FILE_OUTPUT_PATH,config.COUNTRY_FILE_NAME)

        # download the weather data and return the list of years
        listOfYears=download_files(url,connectionString,cloudName,fileOutputPath,saveToCloud,fileName,isIncrementalLoad,sampleFile,blobName)

        logging.info('extract_data pipeline finshed.')
        return listOfYears
    except Exception as e:
        logging.error(f'Error in extract_data.{e}')

# call the main function
if __name__ == '__main__':

    # set up the variables
    url=config.BASE_URL
    connectionString=config.CONNECTION_STRING
    cloudName=config.CLOUD_NAME
    fileOutputPath=config.FILE_OUTPUT_PATH
    saveToCloud=config.SAVE_TO_CLOUD
    fileName=config.FILE_NAME
    isIncrementalLoad=config.INCREMENTAL_LOAD
    sampleFile=config.SAMPLE_FILE
    blobName=config.BLOB_NAME
    try:
        listOfYears=extract_data(url,connectionString,cloudName,fileOutputPath,saveToCloud,fileName,isIncrementalLoad,sampleFile,blobName)
    except Exception as e:
        raise e