import msal
import requests
import json
import pandas as pd

def get_app_token(username: str, password: str, client_id: str, tenant_id: str) -> str:
    '''
    Retrieve token for the app registered in Azure & PowerBI Service.
    
    Parameters:
        username (str): for Azure & PowerBI account
        password (str): for Azure & PowerBI account
        client_id (str): Azure App Registration (client) ID
        tenant_id (str): Azure App Registration directory (tenant) ID
    
    Returns:
        access_token (str): token for accessing PBI Service API
        
    '''
    authority_url = 'https://login.microsoftonline.com/' + tenant_id
    scope = ['https://analysis.windows.net/powerbi/api/.default']
    
    
    app = msal.PublicClientApplication(client_id, authority=authority_url)
    result = app.acquire_token_by_username_password(username=username,password=password,scopes=scope)
    access_token = result['access_token']
        
    return access_token
    
def download_content_df(access_token: str, url_extension = 'groups') -> pd.DataFrame:
    '''
    Downloading specific entity data from PBI service.
    
    Parameters:
        access_token (str): token allowing connection to PBI Service API
        url_extension (str): specification which entity download
    
    Returns:
        content_df (pd.DataFrame): downloaded data
    '''
    
    url_groups = 'https://api.powerbi.com/v1.0/myorg/' + url_extension
    header = {'Content-Type':'application/json','Authorization': f'Bearer {access_token}'}
    
    api_out = requests.get(url=url_groups, headers=header)
    content_df = pd.DataFrame(api_out.json()['value'])
    content_df = content_df.rename(columns = {'objectId': 'id'})
    
    return content_df

def download_specific_content_df(url_base:str, resources_ids: pd.Series, content_type: str, token: str, data_category: str) -> pd.DataFrame:
    '''
    Having list of resources of the entity (for example dataflows within workspace), iterate over the resources
    to retrieve each resource specific information (in this case - datasources) and concatenate them into one dataframe.
    
    Parameters:
        url_base (str): first part of url pointing on the entity
        resources_ids (pd.Series): collection of resources to iterate on (part of url)
        content_type (str): category of entities to iterate on (part of url), it may be tiles or datasources
        token (str): access token
        data_category (str): datasource category (dataflows, datasets, dashboards)
    
    Returns:
        merged_df (pd.DataFrame): downloaded and merged data
    '''
    
    merged_df = pd.DataFrame()
    for resource_id in resources_ids:
        url_base_ext = url_base + f'/{resource_id}/{content_type}'
        content_spec = download_content_df(token, url_base_ext)
        content_spec[data_category+'Id'] = resource_id
        merged_df = merged_df.append(content_spec)
    
    return merged_df

def retrieve_data_set_or_flow_sources(data_dict: dict, data_type: str, data_params: tuple) -> pd.DataFrame:
    '''
    Extract datasources (flows or sets) dataframe from dictionary and transform into draw.io-digestible format.
    
    
    Parameters:
        data_dict (dict): dictionary with key-dataframe pairs
        datatype (str): type of entity (dataflows or datasets)
        data_params (tuple): collection of workspace id and name.
    
    Returns:
        df (pd.DataFrame): transformed datasources dataframe.
    '''
    
    df = data_dict[data_type + '_datasources']
    
    if not df.empty:
        df = df.loc[:,['datasourceType', data_type + 'Id','connectionDetails']] #
        df['connectionDetails'] = df['connectionDetails'].astype(str)
        df = df.rename(columns = {'connectionDetails': 'id',
                                  'datasourceType': 'name', 
                                  data_type + 'Id': 'parent'}) 
        df = df.groupby(['id','name'])['parent'].apply(join_strings).reset_index()
        df['type'] = data_type + '_datasources'
        return df
    else:
        return None
        
def select_groups(df, groups: list) -> pd.DataFrame:
    '''
    Select subset of workspaces defined by name in the list, or get all workspaces when list is empty.
    '''
    if not groups:
        df = df.loc[:,['name', 'id']]
    else:
        df = df[df['name'].isin(groups)][['name', 'id']]
    
    return df
    
def join_strings(collection):
    '''
    Create string sequence of unique objects from collection.
    '''
    return ','.join(set(collection))