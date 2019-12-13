import tableauserverclient as TSC
from tableaudocumentapi import Workbook
import os
import pandas as pd
import pandasql as psql
import datetime as dt
import logging
import logConfig
import configparser
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import json
from tableauserverclient.server.endpoint.exceptions import ServerResponseError

#parsing credentials in config.ini
current_path = os.path.dirname(os.path.abspath(__file__))
cfg = configparser.ConfigParser()
cfg.read('{0}/config.ini'.format(current_path)) 

#set up logging 
logger = logging.getLogger(__name__)

class Directory:
  def __init__(self, path, name):
    self.path = os.path.join(path, name)
    self.name = name

def getListOfFiles(dirName):
    """ 
        Prepares all files under given directory.

        Returns: 
        list: Recursive file list under given directory name parameter.
    
        """
    listOfFile = os.listdir(dirName)
    allFiles = list()
    
    for entry in listOfFile:
        fullPath = os.path.join(dirName, entry)
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles

def get_bucket(GOOGLE_APPLICATION_CREDENTIALS, BUCKET_NAME):
    storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
    return storage_client.get_bucket(BUCKET_NAME)

def get_workbook_permissions():
    """ 
        Workbook permissions. 
    
        Prepares object's permissions granted to users or roles from Tableau server database. 

        Returns: 
        pandas.Dataframe: Permissions as pandas.Dataframe 
    
        """

    user=cfg['tableauDB']['user']
    password=cfg['tableauDB']['password']
    host=cfg['tableauDB']['host']
    port=cfg['tableauDB']['port']
    database=cfg['tableauDB']['database']

    tableau_db_connection = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, database, user, password))
    sql = "select  pro.luid as Project_Id, \
                    replace(replace(pro.name, '/', '_'), '\n', '') as Project_Name, \
                    w.luid as Workbook_Id, \
                    replace(replace(w.name, '/', '_'), '\n', '')  as Workbook_Name, \
                    case \
                        when c.display_name = 'View' then 1 \
                        when c.display_name = 'Export Image' then 2 \
                        when c.display_name = 'Export Data' then 3 \
                        when c.display_name = 'View Comments' then 4 \
                        when c.display_name = 'Add Comment' then 5 \
                        when c.display_name = 'Filter' then 6 \
                        when c.display_name = 'View Underlying Data' then 7 \
                        when c.display_name = 'Share Customized' then 8 \
                        when c.display_name = 'Web Authoring' then 9 \
                        when c.display_name = 'Write' then 10 \
                        when c.display_name = 'Download File' then 11 \
                        when c.display_name = 'Move' then 12 \
                        when c.display_name = 'Delete' then 13 \
                        when c.display_name = 'Set Permissions' then 14 \
                        else 15 \
                        end   as Web_Order, \
                    case \
                        when c.display_name = 'Export Image' then 'Download Image/PDF' \
                        when c.display_name = 'Export Data' then 'Download Summary Data' \
                        when c.display_name = 'View Underlying Data' then 'Download Full Data' \
                        when c.display_name = 'Web Authoring' then 'Web Edit' \
                        when c.display_name = 'Write' then 'Save' \
                        when c.display_name = 'Download File' then 'Download Workbook/Save as' \
                        else c.display_name \
                        end   as Web_Name, \
                    u.name    as User_Name, \
                    g.name    as Group_Name, \
                    pr.reason as Permission_Reasons \
            from next_gen_permissions n \
                 inner join capabilities c on c.id = n.capability_id \
                 inner join workbooks w on n.authorizable_id = w.id \
                 inner join projects pro on w.project_id = pro.id \
                 inner join permission_reasons pr on n.permission = pr.precedence \
                 left join groups g on n.grantee_id = g.id \
                 left join _users u on n.grantee_id = u.id \
            where 1 = 1 \
              and n.authorizable_type = 'Workbook' \
            order by workbook_name, web_order, user_name, group_name"

    tableau_privileges = pd.read_sql_query(sql, tableau_db_connection)
    tableau_db_connection = None

    title_column_name = []
    for column_name in list(tableau_privileges.columns):
        title_column_name.append(column_name.title())
    tableau_privileges.columns = title_column_name

    return tableau_privileges

def main():

    server = TSC.Server(cfg['tableauServer']['url'])
    tableau_auth = TSC.TableauAuth(cfg['tableauServer']['user'], cfg['tableauServer']['password'])
    with server.auth.sign_in(tableau_auth):
        all_views = list(TSC.Pager(server.views, usage=True))
        all_workbooks = list(TSC.Pager(server.workbooks))

    df_views = pd.DataFrame(columns = ['Name', 'Workbook_Id', 'Total_Views'])
    df_workbooks = pd.DataFrame(columns = ['Project_Name', 'Project_Id', 'Name', 'Id'])

    for view in all_views:
        df_views = df_views.append({'Name': view.name, 'Workbook_Id': view.workbook_id, 'Total_Views': view.total_views}, ignore_index=True)
        
    for workbook in all_workbooks:
        df_workbooks = df_workbooks.append({'Project_Name': workbook.project_name,
                                            'Project_Id': workbook.project_id,
                                            'Name': workbook.name, 'Id': workbook.id}, ignore_index=True)


    df = psql.sqldf("select a.Project_Id, replace(replace(a.Project_Name, '/', '_'), '\n', '') as Project_Name, \
                            a.Id as Workbook_Id, replace(replace(a.Name, '/', '_'), '\n', '') as Workbook_Name, \
                            b.Name as View_Name, b.Total_Views as Total_Views_Of_View, \
                            sum(b.Total_Views) over(partition by a.Id) as Total_Views_Of_Workbook \
                    from df_workbooks a join df_views b on(a.Id = b.Workbook_Id)")

    df_dist_workbook_id = psql.sqldf("select distinct Project_Id, Project_Name, Workbook_Id, Workbook_Name from df where Total_Views_Of_Workbook = 0")

    now_datetime = dt.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    workbook_directory = Directory('./', 'Workbooks_' + now_datetime)

    # Workbooks are downloading to local directory
    with server.auth.sign_in(tableau_auth):
        for index, row in df_dist_workbook_id.iterrows():
            target_directory = workbook_directory.path + '/' + row.Project_Name + '/'
            file_name = target_directory + row.Workbook_Name + '.twb'
            os.makedirs(target_directory, exist_ok=True)

            downloaded_file_path = server.workbooks.download(row.Workbook_Id, filepath=file_name)

    logger.info("{} workbook(s) downloaded...".format(df_dist_workbook_id.shape[0]))

    # Workbooks to be removed
    df_dist_workbook_id.to_csv(os.path.join(workbook_directory.path, "Tableau_Unused_Workbooks.csv"), index = False)

    df_tableau_permissions = get_workbook_permissions()
    df_tableau_permissions = psql.sqldf("select b.* from df_dist_workbook_id a join df_tableau_permissions b on(a.Project_Id = b.Project_Id and a.Workbook_Id = b.Workbook_Id)")
    df_tableau_permissions.to_csv(os.path.join(workbook_directory.path, "Tableau_Unused_Workbooks_Permissions.csv"), index = False)

    BUCKET_NAME = cfg['googleCloud']['BUCKET_NAME']
    GOOGLE_APPLICATION_CREDENTIALS = cfg['googleCloud']['GOOGLE_APPLICATION_CREDENTIALS']

    bucket = get_bucket(GOOGLE_APPLICATION_CREDENTIALS, BUCKET_NAME)

    for workbook_file_path in getListOfFiles(workbook_directory.path):
        workbook_file_path = workbook_file_path.replace(workbook_directory.path, workbook_directory.name)
        blob = bucket.blob(workbook_file_path)
        if not (blob.exists()): blob.upload_from_filename(workbook_file_path)

    parent_blob_name = workbook_directory.name
    uploaded_workbook_count = 0

    for blob in list(bucket.list_blobs()):
        if (blob.name.startswith(parent_blob_name) and blob.name.endswith(".twb")):
            uploaded_workbook_count = uploaded_workbook_count + 1
    logger.info("{} twb files uploaded...".format(uploaded_workbook_count))

    if df_dist_workbook_id.shape[0] == uploaded_workbook_count:
        logger.info("Backup totally completed. {0} workbook(s) uploaded...".format(uploaded_workbook_count))
    else: logger.info("Backup couldn't complete! {0} workbook(s) uploaded but it must be {1}...".format(uploaded_workbook_count, df_dist_workbook_id.shape[0]))
        
    with server.auth.sign_in(tableau_auth):
        for index, row in df_dist_workbook_id.iterrows():
            try:
                server.workbooks.delete(row.Workbook_Id)
            except ServerResponseError:
                logger.error("{0} couldn't find!".format(row.Workbook_Name))
main()