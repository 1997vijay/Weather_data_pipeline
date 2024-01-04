import os
import logging
from behave import given, when, then
from src.data_loader import download_files

@given('a URL "{url}"')
def step_given_url(context, url):
    context.url = url

@given('a connection string "{connection_string}"')
def step_given_connection_string(context, connection_string):
    context.connection_string = connection_string

@given('cloud name "{cloud_name}"')
def step_given_cloud_name(context, cloud_name):
    context.cloud_name = cloud_name

@given('a file output path "{file_output_path}"')
def step_given_file_output_path(context, file_output_path):
    context.file_output_path = file_output_path

@given('saving to cloud is (enabled|disabled)')
def step_given_saving_to_cloud(context):
    context.save_to_cloud = context.text.lower() == 'enabled'

@given('a file name "{file_name}"')
def step_given_file_name(context, file_name):
    context.file_name = file_name

@when('files are downloaded')
def step_when_download_files(context):
    try:
        download_files(
            url=context.url,
            connectionString=context.connection_string,
            cloudName=context.cloud_name,
            fileOutputPath=context.file_output_path,
            saveToCloud=context.save_to_cloud,
            fileName=context.file_name
        )
        context.result = "success"
    except Exception as e:
        context.result = f"error: {e}"

@then('the file "{expected_file_name}" should be saved locally at "{expected_path}"')
def step_then_check_local_save(context, expected_file_name, expected_path):
    assert os.path.exists(expected_path), f"File not found at {expected_path}"

@then('at least one file should be saved locally at "{expected_path}"')
def step_then_check_local_save(context, expected_path):
    assert os.listdir(expected_path), f"No files found at {expected_path}"
