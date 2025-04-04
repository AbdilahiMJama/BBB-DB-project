import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import connect_iabbb as ci
from setup import getBusinessDataBatch, getScriptId, getExistingScriptId, initiateScriptActivity, terminateScriptActivity, logProcessedToDB
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData
from main_url_scrape import main_scrape_urls
from urllib.parse import urlparse
from create_urls import extract_domain_name
import sqlalchemy as sa

### For the url type (later use) use a dictionary.

SCRIPT_NAME = 'generating_urls_test_s2025'
SCRIPT_VERSION = '1.0.2'
VERSION_NOTE = None         # put version note string here if desired, otherwise None
CONNECT_USER = 'ABDI'
CONNECT_DB = 'MNSU'
CONNECT_INSTANCE = 'SANDBOX'
CONNECT_SCHEMA = 'spring2025'
SCRIPT_TABLE = 'mnsu_script'
SCRIPT_ACTIVITY_TABLE = 'mnsu_script_activity'
PROCESSED_TABLE = 'mnsu_firm_processed'
BUSINESS_TABLE = 'tblfirms_firm'
NAME_TABLE = 'tblfirms_firm_companyname'
EMAIL_TABLE = 'tblfirms_firm_email'
URL_TABLE = 'tblfirms_firm_url'
GENERATED_URL_TABLE = 'mnsu_generated_firm_url'
BATCH_SIZE = 50
MNSU_URL_ID = 1

errorCode = 42804
errorText = 'Datatype mismatch'

def getDomainName(url):
    """   
    Extracts the domain name from a URL in the dataframe.
    Args:
        url: The URL string.
    Returns:
        The domain name, or None if the URL is invalid.
    """
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None

## Create a function to store the values in the generated_url table


def setUrlType(df):
    pass


def logGeneratedUrlToDB(engine,processedRows,saId):
    """
    Log the generated urls to the database
    :param engine: sqlalchemy engine
    :param processedRows: dataframe of processed rows
    :param scriptId: script id not necessary
    :param activityId: script activity id
    :
    :return: None
    """
    #columns = {'BusinessId':'firm_id','Website':'url'}
    
    assert isinstance(processedRows,pd.core.frame.DataFrame)
    
        
    #processedRows.rename(columns=columns, inplace=True)
    processedRows = processedRows[['firm_id','url','domain', 'main', 'url_type_id', 'url_status_id']]
    

    # issue here, it adds the url and script_activity_id but fails to add the rest of the columns
    # pull 1000 rows at a time and then put all into one dataframe
    # process it 
    # push it to the table, 1000 rows at a time and we keep track of the rows, 1000, 1001
    processedRows[['mnsu_script_activity_id']] = saId
    processedRows[['note']] = 'Testing generated URLs'
    processedRows[['confidence_level']] = 1
    #print(processedRows.columns)

    
    processedRows.to_sql(name=GENERATED_URL_TABLE,
                                con=engine,
                                schema=CONNECT_SCHEMA,
                                if_exists='append',
                                index=False)
    
    
# Query for the firm_id     
#subq2 = sa.select(1).where(
#sa.and_(businessTable.c.firm_id==processedTable.c.firm_id,
#               processedTable.c.mnsu_script_id==scriptId))

# Insert into script id table and mnsu_firm_processed table. Adds duplicate firm_ids. How to handle this.
# add a function to check if the firm_id exists? Shouldn't the processed table be handling this.

if __name__=='__main__':
    
    # Load Environment Variables
    load_dotenv()
    # Create connection
    con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')

   # Create a metadata object
    mnsuMeta = sa.schema.MetaData(schema=CONNECT_SCHEMA)
      
    # Get the script ID and script activity ID
    sId = getScriptId(con, mnsuMeta)
    saId = initiateScriptActivity(con, mnsuMeta,sId) 
    #print(sId)
    # Get the business data
    
    dfs = getBusinessDataBatch(con,mnsuMeta,None,50) 

    # Get the email, name and url tables as a dataframe from the batch
    # Put a business dataframe (BUSINESS_TABLE), return this table to logProcessedToDB
    email_df = dfs[EMAIL_TABLE][['firm_id', 'email']]
    name_df = dfs[NAME_TABLE][['firm_id', 'company_name']]
    url_df = dfs[URL_TABLE][['firm_id', 'url','main','url_type_id','url_status_id']]

    # Merge the three dataframes on their firm id
    business_email_df = pd.merge(name_df, email_df, on='firm_id', how='inner')
    business_email_df = pd.merge(business_email_df, url_df, on='firm_id', how='left')

    

    print(business_email_df[['firm_id','company_name','url_type_id','url_status_id']])

    
    # Update the dataframe with the new urls that have been generated from email and business name.
    # Add the processed rows to the dataframe.
    # Filter out for the generated urls to the generated_url table.
    # Include the urls even if they don't exist.
    update_df = main_scrape_urls(business_email_df)
    print(update_df[['firm_id','url','status_code']])


    # Extract the domain name from the url and update the url_status_id column
    update_df['domain'] = update_df['url'].apply(getDomainName)
    update_df['url_status_id'] = update_df['status_code'].apply(lambda x: 1 if x == 200 else 3)
        
    # Rename the columns to match the convention for the function logGeneratedUrlToDB
    #update_df.rename(columns={'BusinessId':'firm_id','Website':'url'}, inplace=True)
    print(update_df[['firm_id','url','status_code','domain','url_status_id']])

    print(update_df.duplicated())
    #updatedf = update_df.drop_duplicates(subset=['firm_id'], keep='last')
    
    logGeneratedUrlToDB(con, update_df, saId)


    logProcessedToDB(con, update_df, sId, saId)

    # 20k 1k rows at a time
    terminateScriptActivity(con, mnsuMeta, saId, errorCode=errorCode, errorText=errorText)
