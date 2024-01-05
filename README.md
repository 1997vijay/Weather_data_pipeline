
# GSOD Weather Data Pipeline

The project revolves around the efficient processing of weather data obtained from GSOD (Global Summary of the Day). It encompasses tasks such as historical data loading and incremental data updates. The implementation leverages various Python libraries, including requests for interacting with the HTTP URL 'https://www1.ncdc.noaa.gov/pub/data/gsod/' and BeautifulSoup for extracting files and folders from the provided URL. The project aims to streamline the retrieval and processing of weather-related information through these essential Python tools.




## Run Locally

Clone the project

```bash
  git clone https://github.com/1997vijay/Weather_data_pipeline
```

Go to the project directory

```bash
  cd Weather_data_pipeline
```
Create data directory
```bash
  data
    --> aggregated
    --> processed
    --> raw
    --> resources
```

Install dependencies

```python
  pip install -r requirements.txt
```

Run the file

```python
  python .\src\main.py
```

## Configuration File
The config.py file in this Python project serves as a centralized hub for configuration settings, allowing easy customization of various aspects. Below are some of the Configuration.
* **Historical Load**: Toggle `INCREMENTAL_LOAD` to control whether the project performs a historical or full load of data. Ex. For historical load set ``INCREMENTAL_LOAD=False``
* **Sample File:** Customize `SAMPLE_FILE` to specify the number of files to download from the URL. For example, to download only 100 files from each year set ``SAMPLE_FILE=100``.
* **Save to Cloud Storage**: Use `SAVE_TO_CLOUD` to determine whether to save files to cloud storage.If you want to save the files to cloud storage set ``SAVE_TO_CLOUD`` to True.
* **Cloud Platform Name**: `CLOUD_NAME` is used to define the which cloud platform to use to save the files.Ex. `CLOUD_NAME='Azure'`. At present, the project is configured to save files to Microsoft Azure. However, you have the flexibility to enhance its capabilities by adjusting the cloudStorageSaver.py file. This allows for the addition of methods tailored for other cloud providers such as AWS or GCP.
* All file output paths and other relevant paths are defined within the configuration file, providing the flexibility to customize them according to your specific requirements.


