# tableau-clean-backup

This is a script for clean unused tableau server workbooks and backing up to google cloud storage.

This python script finds workbooks that have never viewed from each site in the listed tableau server location.

This script queries all sites within the listed tableau server and downloads workbooks and then loads this files to a specified google cloud storage bucket. 

This script also prepares and loads two information files. This files contains workbooks to be deleted and it's permission informations.

    Tableau_Unused_Workbooks.csv: [Project_Id, Project_Name, Workbook_Id, Workbook_Name]
    Tableau_Unused_Workbooks_Permissions.csv: [Project_Id, Project_Name, Workbook_Id, Workbook_Name, Web_Order, Web_Name, User_Name, Group_Name, Permission_Reasons]

Runs in python3

Dependencies 
- tableauserverclient
- tableaudocumentapi
- google.cloud
- oauth2client
- pandas
- pandasql

Configuration Parameters:
    
    Open congif.ini file and input belowed informations:
    
    [tableauServer]
    url = <Tableau server url>
    user = <Tableau server username>
    pass = <Tableau server password>

    [tableauDB]
    user = <Tableau server DB username>
    password = <Tableau server DB password>
    host = <Tableau server DB host>
    port = <Tableau server DB port>
    database = <Tableau server DB name>

    [googleCloud]
    BUCKET_NAME = <Bucket name under your google cloud storage account. For more info: (https://cloud.google.com/storage/docs/key-terms)>
    GOOGLE_APPLICATION_CREDENTIALS = <Credential file of your google cloud account. For more info: (https://cloud.google.com/docs/authentication/getting-started)>
