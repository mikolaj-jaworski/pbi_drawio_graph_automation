import io
import os
import pandas as pd

from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeDirectoryClient, DataLakeServiceClient

def get_directory_client(connection_string: str, container_name: str, folder_name: str) -> DataLakeDirectoryClient:
    try:
        # create a Data Lake service client
        datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)
    except Exception:
        print("Connection string either blank or malformed.")
        raise
    # create or get a container that will contain imported data
    try:
        filesystem_client = datalake_service_client.create_file_system(file_system=container_name)
    except ResourceExistsError:
        filesystem_client = datalake_service_client.get_file_system_client(container_name)

    # create a directory inside the container
    dir_client = filesystem_client.get_directory_client(folder_name)
    dir_client.create_directory()
    return dir_client

def dataframe_to_csv_content(data: pd.DataFrame, compression='gzip'):
    f = io.BytesIO()
    data.replace('"""', '"').to_csv(f, sep = ',', compression=compression, index = False, encoding = 'CP1250')
    f.seek(0)
    content = f.read()
    return content

def save_data(data: bytes, connection_string: str, container_name: str, folder_name: str, file_name: str):
    dir_client = get_directory_client(connection_string, container_name, folder_name)
    # save data to Data Lake Storage
    file_client = dir_client.create_file(file_name)  # DataLakeFileClient
    file_client.append_data(data, 0, len(data))
    file_client.flush_data(len(data))
    return f"{folder_name}/{file_name}"

def read_file(connection_string: str, container_name: str, folder_name: str, file_name: str) -> io.BytesIO:
    dir_client = get_directory_client(connection_string, container_name, folder_name)
    file_client = dir_client.get_file_client(file_name)  # DataLakeFileClient
    download = file_client.download_file()
    downloaded_contents = download.readall()
    return io.BytesIO(downloaded_contents)

def list_and_sort_files(connection_string: str, container_name: str, folder_name: str):
    datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = datalake_service_client.get_file_system_client(file_system=container_name)
    paths = file_system_client.get_paths(path = folder_name)
    files = [path.name[4:20] for path in paths]
    files.sort(reverse=True)
    return files
