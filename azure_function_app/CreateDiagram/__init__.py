import os
import pandas as pd
import json
import logging
import datetime

from Shared.data_load_transform import get_app_token, execute_load_transform
from Shared.drawio_spec import html_spec, drawio_spec
from Shared.key_vault_util import get_secret_value
from Shared.data_lake_util import get_directory_client, save_data, dataframe_to_csv_content



def main(name: list) -> str:
    user = get_secret_value('username')
    pwd = get_secret_value('password')
    client = get_secret_value('client-id')
    tenant = get_secret_value('tenant-id')
    ws_names = name

    token = get_app_token(username=user, password=pwd, client_id=client, tenant_id=tenant)
    
    # create diagram df 
    diagram_csv = execute_load_transform(token, ws_names)
    
    # add html variables
    diagram_csv = pd.merge(diagram_csv, html_spec, on = 'type', how = 'left')

    # convert df to bytes and save in ADLS
    diagram_csv_bytes = dataframe_to_csv_content(diagram_csv, None)
    save_data(diagram_csv_bytes, os.environ['DataLakeConnectionString'], os.environ['DataLakeContainerName'], os.environ['CSVDataFolder'],
              datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M.csv"))

    # convert text to bytes and save in ADLS
    diagram_drawio = (drawio_spec + diagram_csv.to_csv(index = False, sep = ',', encoding = 'CP1250').replace('"""', '"')).encode()
    save_data(diagram_drawio, os.environ['DataLakeConnectionString'], os.environ['DataLakeContainerName'], os.environ['DiagramDataFolder'],
              datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M.txt"))

    return 'OK'


