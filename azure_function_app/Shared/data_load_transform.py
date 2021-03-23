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

def download_all_data(token: str, categories:list, workspace_id: str) -> dict:
    '''
    Iterating over all entity categories and saving the result into one dictionary (key:dataframe).
    '''
    data_dict = {}
    missing_cat = []
    for cat in categories:
        url_base = f'groups/{workspace_id}/{cat}'
        content = download_content_df(token, url_base)

        # if category is missing, note it and continue to next
        if content.empty:
            missing_cat.append(cat)
            continue
        else:
            data_dict[cat] = content

        # some entities have child entities, which has to be extracted too
        if cat in ['dataflows', 'datasets']:
            content_ext = download_specific_content_df(url_base, content['id'], 'datasources', token, cat) 
            data_dict[cat+'_datasources'] = content_ext

        if cat in ['datasets']:
            url_base_ext = url_base + f'/upstreamdataflows'
            content_ext = download_content_df(token, url_base_ext)
            data_dict[cat+'_upstreamdataflows'] = content_ext

        if cat in ['dashboards']:
            content_ext = download_specific_content_df(url_base, content['id'], 'tiles', token, cat) 
            data_dict[cat+'_datasources'] = content_ext
    
    return data_dict, missing_cat

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

def execute_load_transform(token: str, ws_names: list) -> pd.DataFrame:
    '''
    Main function for creating CSV digestible for draw.io. 
    It performs data download, transformation and utilizes many previously defined functions.
    '''
    group_df = download_content_df(token, 'groups')
    selected_groups = select_groups(group_df, ws_names)
    categories = ['users', 'dataflows', 'datasets', 'reports', 'dashboards']

    output_all = pd.DataFrame()
    for row in selected_groups.itertuples():

        output = pd.DataFrame()
        data_dict, missing_cat = download_all_data(token, categories, row.id)

        # if workspace is empty (has only usage-monitoring datasets and reports), continue to the next workspace
        if 'identifier' not in data_dict['users'].columns:
            continue

        ''' USERS '''
        if 'users' not in missing_cat:
            users = data_dict['users']
            users['id'] = users['groupUserAccessRight'] + ':' + users['identifier']
            users = users.loc[:,['id', 'displayName']]
            users = users[users['id'].str.contains('@')]
            users['type'] = 'users'
            users = users.rename(columns = {'displayName': 'name'})

            output = pd.concat([output, users])

        ''' DATAFLOWS '''
        if 'dataflows' not in missing_cat:
            dataflows = data_dict['dataflows']
            dataflows = dataflows.loc[:,['id', 'name']]
            dataflows['type'] = 'dataflows'
            dataflows['parent'] = row.id

            ''' DATAFLOWS DATASOURCES '''
            dataflows_datasources = retrieve_data_set_or_flow_sources(data_dict, 'dataflows', row)

            output = pd.concat([output, dataflows, dataflows_datasources])

        ''' DATASETS '''
        if 'datasets' not in missing_cat:
            datasets = data_dict['datasets']
            datasets = datasets.loc[:,['id', 'name', 'configuredBy']]
            datasets['type'] = 'datasets'
            datasets = datasets.rename(columns = {'configuredBy': 'relatives'})

            datasets_upstream = data_dict['datasets_upstreamdataflows']
    #         shared_workspaces = list(datasets_upstream['workspaceObjectId'].unique())
    #         shared_workspaces.remove(row.id)

            # upstream is empty, when there are no original datasets (only shared datasets), but other resources exist
            if not datasets_upstream.empty:
                datasets_upstream = datasets_upstream.groupby('datasetObjectId').agg({'dataflowObjectId': join_strings,
                                                            'workspaceObjectId': join_strings}).reset_index()
                datasets_upstream = datasets_upstream.rename(columns = {'datasetObjectId': 'id', 'dataflowObjectId': 'relatives',
                                                                        'workspaceObjectId': 'parent'})
                datasets = pd.merge(datasets, datasets_upstream, on = 'id', how = 'left')
                datasets['parent'] = datasets['parent'].fillna(row.id)
                datasets['relatives'] = (datasets['relatives_x'] + ',' + datasets['relatives_y'].fillna(',')).replace(r',,', '', regex=True)
                datasets = datasets.drop(columns = ['relatives_x', 'relatives_y'])
            else:
                datasets['parent'] = row.id

            ''' DATASETS DATASOURCES '''
            datasets_datasources = retrieve_data_set_or_flow_sources(data_dict, 'datasets', row)

            output = pd.concat([output, datasets, datasets_datasources])     

        ''' REPORTS '''
        if 'reports' not in missing_cat:
            reports = data_dict['reports']
            reports = reports.loc[:,['id', 'name', 'datasetId']]
            reports['type'] = 'reports'
            reports = reports.rename(columns = {'datasetId': 'parent'})
            reports['parent'] = row.id + ',' + reports['parent']

            output = pd.concat([output, reports])

        ''' DASHBOARDS '''
        if 'dashboards' not in missing_cat:
            dashboards = data_dict['dashboards']
            dashboards = dashboards.loc[:,['id', 'displayName']]
            dashboards['type'] = 'dashboards'
            dashboards = dashboards.rename(columns = {'displayName': 'name'})

            dashboards_datasources = data_dict['dashboards_datasources'].fillna(',')
            dashboards_datasources = dashboards_datasources.groupby('dashboardsId').agg({'reportId': join_strings,
                                                                'datasetId': join_strings}).reset_index().replace(r',,', '', regex=True)
            dashboards_datasources['parent'] = dashboards_datasources['reportId'] + ',' + dashboards_datasources['datasetId']
            dashboards_datasources = dashboards_datasources.drop(columns = ['reportId', 'datasetId'])
            dashboards_datasources = dashboards_datasources.rename(columns = {'dashboardsId': 'id'})

            dashboards = pd.merge(dashboards, dashboards_datasources, on ='id', how = 'left')
            dashboards['parent'] = dashboards['parent'].fillna(row.id) 

            output = pd.concat([output, dashboards])

        ''' FINAL OUTPUT '''
        # add workspace row to the dataframe
        output = output.append({'id': row.id, 'name':row.name, 'type': 'workspaces', 'relatives': join_strings(users['id'].unique())},
                            ignore_index=True)
        
        output_all = pd.concat([output_all, output])

    # in case drawio has problems with reading special characters, take ids between quotation marks
    output_all['id'] = '"' + output_all['id'] + '"'
    output_all = output_all.drop_duplicates().sort_values(by = ['type', 'id'])

    return output_all
        
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

