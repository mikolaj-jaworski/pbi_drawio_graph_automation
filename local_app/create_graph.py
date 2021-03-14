import os
import pandas as pd
import argparse
from getpass import getpass
from Shared.data_load_transform import get_app_token, download_content_df, download_specific_content_df, \
                                       retrieve_data_set_or_flow_sources, select_groups, join_strings
from Shared.drawio_spec import html_spec, drawio_spec

wd = os.getcwd()

def main(user, pwd, client, tenant, ws_names):
    '''
    Download data from PBI Service API and prepare relationships dataframe as input for draw.io.

    Parameters:
        user, pwd, client, tenant (str): information for using PBI Service API.
        ws_names (list): collection of workspaces we want to create graph on (if list is empty, all available workspaces will be used)

    Returns:
        drawio_input.csv (file): text file with specification ready to copy-paste into draw.io CSV reader.
        drawio_relationships.csv (file): csv file with raw dataframe of relationships.
    '''

    token = get_app_token(username=user, password=pwd, client_id=client, tenant_id=tenant)
    group_df = download_content_df(token, 'groups')
    selected_groups = select_groups(group_df, ws_names)
    output_all = pd.DataFrame()

    # iterate over selected workspaces
    for row in selected_groups.itertuples():
        print(row.name)
        
        # placeholders
        output = pd.DataFrame()
        data_dict = {}
        missing_cat = []
        
        # loop for data download
        for cat in ['users', 'dataflows', 'datasets', 'reports', 'dashboards']:
            url_base = f'groups/{row.id}/{cat}'
            content = download_content_df(token, url_base)
            
            # if category is missing, note it and continue to next
            if content.empty:
                missing_cat.append(cat)
                continue
            else:
                data_dict[cat] = content
            
            if cat in ['dataflows', 'datasets']:
                content_ext = download_specific_content_df(url_base, content['id'], 'datasources', token, cat) 
                data_dict[cat+'_datasources'] = content_ext
            
            if cat in ['datasets']:
                # get links from dataset to dataflows
                url_base_ext = url_base + f'/upstreamdataflows'
                content_ext = download_content_df(token, url_base_ext)
                data_dict[cat+'_upstreamdataflows'] = content_ext
                
            if cat in ['dashboards']:
                content_ext = download_specific_content_df(url_base, content['id'], 'tiles', token, cat) 
                data_dict[cat+'_datasources'] = content_ext
        
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
        
        ''' WORKSPACE & OUTPUT '''
        
        # add workspace row to the dataframe
        output = output.append({'id': row.id, 'name':row.name, 'type': 'workspaces', 'relatives': join_strings(users['id'].unique())},
                            ignore_index=True)
        # in case drawio has problems with reading special characters, take ids between quotation marks
        output['id'] = '"' + output['id'] + '"'
        output = pd.merge(output, html_spec, on = 'type', how = 'left')
        
        output_all = pd.concat([output_all, output])

    # avoid duplicates and sort df for easier version controll
    output_all = output_all.drop_duplicates().sort_values(by = ['type', 'id'])

    # saave df as csv and create txt input file for draw.io
    output_all.to_csv(wd + '/output/drawio_relationships.csv', index = False, sep = ',', encoding = 'CP1250')
    with open(wd + '/output/drawio_input.txt', 'w') as file:
        file.write(drawio_spec)
        file.write(output_all.to_csv(index = False, sep = ',', encoding = 'CP1250').replace('\n','').replace('"""', '"'))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create a Power BI resource graph')
    parser.add_argument('--user', required=True)
    parser.add_argument('--client', required=True, help='cleint id')
    parser.add_argument('--tenant', required=True, help='tenant id')
    parser.add_argument('--ws_names', nargs="*", help='list of workspaces')
    
    args = parser.parse_args()
    pwd = getpass("User password:")

    main(args.user, pwd, args.client, args.tenant, args.ws_names)