## Version of the script to run on Azure

This function utilizes below Azure resources (first two are mandatory):
- Functions
- Data Lake
- Key Vault
- Data Factory

### Description
This function works as follows:
- CreateDiagram is an Durable Activity function and it downloads the data through PowerBI API using credentials stored in KeyVault (username,password, client_id, tenant_id). Then it transforms the data into DrawIO digestible format and returns two files: CSV with table only, and TXT with both table and DrawIO parameters. TXT file has to be copy-pasted to DrawIO to create visual diagram.
- ControlChanges is just a HttpTriggered function, which should be run AFTER CreateDiagram is done. It can also be run independently (that is why it isn't another Durable Activity). It compares the latest and previous CSV files and returns JSON with info which rows have changed.

All files are being stored in the DataLake.

### Environment setup

To set up virtual environment, run below commands in your bash terminal.
```bash
conda create --name pbidrawio python=3.7 --yes
conda activate pbidrawio
pip3 install --upgrade pip --user
pip3 install -r requirements.txt --user
```

You need to create local.settings.json file, with specified content, which needs to be filled:
```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "",
    "DataLakeConnectionString": "",
    "DataLakeContainerName": "",
    "CSVDataFolder": "",
    "DiagramDataFolder": "",
    "ChangesDataFolder": "",
    "KeyVaultURL": "",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "FUNCTIONS_EXTENSION_VERSION": "~3"
  }
}
```

### How to use?

To check performance, **scripts** could be used, but creating proper pipeline in Azure Data Factory would be the most convenient. For more information how to deploy the function & set up ADF pipeline, check out my post [here](https://mikolaj-jaworski.github.io/2021-02-20-azure-durable-functions/).