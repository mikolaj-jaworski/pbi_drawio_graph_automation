import logging
import os
import io
import json
import pandas as pd
from csv_diff import load_csv, compare

import azure.functions as func
from Shared.data_lake_util import list_and_sort_files, save_data, read_file

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    files = list_and_sort_files(os.environ['DataLakeConnectionString'], os.environ['DataLakeContainerName'], os.environ['CSVDataFolder'])

    if len(files) <= 1:
        return func.HttpResponse(json.dumps({"message": "Not enough data to compare, nothing returned."}), status_code=200)
    
    # read two latest files
    latest = read_file(os.environ['DataLakeConnectionString'], os.environ['DataLakeContainerName'], os.environ['CSVDataFolder'], files[0]+'.csv')
    previous = read_file(os.environ['DataLakeConnectionString'], os.environ['DataLakeContainerName'], os.environ['CSVDataFolder'], files[1]+'.csv')
    
    # compare them
    diff = compare(
        load_csv(io.StringIO(pd.read_csv(latest, encoding = 'CP1250').to_csv(encoding = 'CP1250')), key='id'),
        load_csv(io.StringIO(pd.read_csv(previous, encoding = 'CP1250').to_csv(encoding = 'CP1250')), key='id')
    )
    # save as json
    diff_json = json.dumps(diff).encode('CP1250')
    save_data(diff_json, os.environ['DataLakeConnectionString'], os.environ['DataLakeContainerName'], os.environ['ChangesDataFolder'],
              files[0]+' VS ' + files[1] + '.json')

    return func.HttpResponse(json.dumps({"message": "Data comparison successful, check ADLS."}), status_code=200)