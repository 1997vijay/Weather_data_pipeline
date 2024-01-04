"""
Save the file to any of the cloud provider.

Dependencies:
    - azure-storage-blob==12.19.0: A Azure sdk python library for intreacting with Azur storage.
    - boto3==1.34.7: A AWS sdk python library for intreacting with AWS s3 bucket.
    - google-cloud-storage: A GCP sdk python library for intreacting with GCP storage.

Usage:
    1. Ensure that you have the required dependencies installed.
    2. Run the code to upload the file content to cloud storage.

    Author Information:
    Name: Vijay Kumar
    Date: 23 Dec 2023

Abstract/Description:
Below code is used to upload the file content directly to any cloud storage without the need of downloading the file.

Change Log:
    - 23 Dec 2023: Initial creation.
"""

# import required libraries
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ServiceRequestError
from io import BytesIO

import logging



# class to intreact with AWS storage
class AwsStorageUtils:
    def __init__(self) -> None:
        """Initialize an instance of AwsStorageUtils.

        Raises:
            NotImplementedError: This class is not implemented. It serves as a placeholder for future implementation.
        """
    def upload_file(self,containerName,blobName,response,fileName:str=None):
            """
            Upload a file to Azure Blob Storage.
            Args:
                container_name (str): Container name.
                blob_name (str): Blob name.
                file_name (str, optional): File name. Default is None.
            Raises:
                Exception: If there is an error during the file upload process.
            """
            raise NotImplementedError

# class to intreact with GCP storage
class GCPStorageUtils:
    def __init__(self) -> None:
        """Initialize an instance of GCPStorageUtils.

        Raises:
            NotImplementedError: This class is not implemented. It serves as a placeholder for future implementation.
        """
    def upload_file(self,containerName,blobName,response,fileName:str=None):
            """
            Upload a file to Azure Blob Storage.
            Args:
                container_name (str): Container name.
                blob_name (str): Blob name.
                file_name (str, optional): File name. Default is None.
            Raises:
                Exception: If there is an error during the file upload process.
            """
            raise NotImplementedError

# class to intreact with Azure storage
class AzureStorageUtils:
    def __init__(self,connection_string) -> None:
        """
        Initialize the Azure Storage Utilities object with an Azure Storage connection string.
        Args:
            connection_string (str): Azure storage account connection string.
        Raises:
            ValueError: If the provided connection string is empty or None.
            ServiceRequestError: If there is an error while connecting to the Azure Blob Storage.
        """
        self.__connection_string=connection_string

        if self.__connection_string!='' or self.__connection_string is not None:
            try:
                self._client=BlobServiceClient.from_connection_string(conn_str=self.__connection_string)
            except ServiceRequestError as e:
                raise ServiceRequestError("Error while connecting to blob!!. {e}")
        else:
            raise ValueError('Invalid connection string!!')

    def upload_file(self,containerName,blobName,data,fileName:str=None):
            """
            Upload a file to Azure Blob Storage.
            Args:
                container_name (str): Container name.
                blob_name (str): Blob name.
                file_name (str, optional): File name. Default is None.
            Raises:
                Exception: If there is an error during the file upload process.
            """
            try:
                blob_client=self._client.get_blob_client(container=containerName,blob=f"{blobName}/{fileName}")
                result=blob_client.upload_blob(data,overwrite=True)
                if result['request_id']:
                    logging.info(f'{fileName} uploaded to container: {containerName} successfully')
            except Exception as e:
                logging.error(f'Error while uploading the file.{e}')
                raise e


# import required libraries
class CloudStorageSaver:
    def __init__(self,cloudName,connection_string):
        """
        Initialize an instance of CloudStorageSaver.
        This class is designed to handle file storage across different cloud providers.

        Args:
            cloudName (str): Name of the cloud storage provider ('Azure', 'AWS', 'GCP').
            response: The response containing the file content.
            connection_string (str): Connection string for cloud storage.
            toLocal (bool, optional): Flag indicating whether to save the file locally. Defaults to False.

        Raises:
            NotImplementedError: If the initialization is attempted without providing a concrete implementation.
        """
        self._cloudName=cloudName
        self._connection_string=connection_string
    
        if self._cloudName.capitalize()=='Azure':
            logging.info('Creating instance of Azure storage')
            self._client=AzureStorageUtils(connection_string=self._connection_string)
        elif self._cloudName.upper()=='AWS':
            logging.info('Creating instance of AWS bucket')
            self._client=AwsStorageUtils()
        elif self._cloudName.upper()=='GCP':
            logging.info('Creating instance of GCP storage')
            self._client=GCPStorageUtils()
        else:
            raise ValueError('Not a valid cloud provider name!!')
    
    def upload_file(self,containerName,blobName,data,fileName=None):
         result=self._client.upload_file(containerName,blobName,data,fileName)
         return result