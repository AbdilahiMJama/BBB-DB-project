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



SCRIPT_NAME = 'generating_urls_for_ten_test_s2025'
SCRIPT_VERSION = '1.0'
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


def getExistingUrlId(engine, metadata, saId, scriptTable=None):
    if scriptTable==None:
        scriptTable = sa.Table(GENERATED_URL_TABLE,metadata,autoload_with=engine)
    qry = sa.select(scriptTable).filter_by(mnsu_url_id=MNSU_URL_ID, mnsu_script_activity_id=saId)

    with engine.connect() as con:
        res = con.execute(qry)

        if res.rowcount==0:
            return False
        return next(res)[0]

def getUrlId(engine, metadata,saId):
    """
    Returns the primary key associated with the current script name and 
    version, or false if none exists
    
    engine: a sqlalchemy engine
    metadata: a sqlalchemy Metadata object
    scriptTable: (optional) a Table object for the script table. This will be 
        generated if not present, but if passed as a keyword argument, the 
        table object will be updated from the database
    """
    scriptTable = sa.Table(GENERATED_URL_TABLE,metadata,autoload_with=engine)
    if (script_id:=getExistingUrlId(engine,metadata,saId,scriptTable=scriptTable)):
        return script_id
    
    with engine.connect() as con:
        qry = sa.insert(scriptTable).returning(scriptTable.c.mnsu_url_id)
        params = {'mnsu_url_id':MNSU_URL_ID, 'mnsu_script_activity_id':saId}
        res = con.execute(qry,params)
        con.commit()
        print(next(res)[0])
        return next(res)[0]


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
    columns = {'BusinessId':'firm_id','Website':'url'}
    
    assert isinstance(processedRows,pd.core.frame.DataFrame)
    
        
    processedRows.rename(columns=columns, inplace=True)
    processedRows = processedRows[['firm_id','url','domain', 'main', 'url_type_id', 'url_status_id']]
    
    # issue here, it adds the url and script_activity_id but fails to add the rest of the columns
    processedRows['mnsu_url_id'] = getUrlId(engine, mnsuMeta, saId)
    processedRows['mnsu_script_activity_id'] = saId
    processedRows['note'] = 'Testing generated URLs'
    processedRows['confidence_level'] = 1
    print(processedRows.columns)

    print(processedRows[['mnsu_url_id','mnsu_script_activity_id','firm_id','url']])
    '''
    finally:
        processedRows.to_sql(name=GENERATED_URL_TABLE,
                                con=engine,
                                schema=CONNECT_SCHEMA,
                                if_exists='append',
                                index=False)
    '''


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

    # Get the business data
    dfs = getBusinessDataBatch(con,mnsuMeta,None,50) 

    # Get the email, name and url tables as a dataframe from the batch
    email_df = dfs[EMAIL_TABLE][['firm_id', 'email']]
    name_df = dfs[NAME_TABLE][['firm_id', 'company_name']]
    url_df = dfs[URL_TABLE][['firm_id', 'url','main','url_type_id','url_status_id']]

    # Merge the three dataframes on their firm id
    business_email_df = pd.merge(name_df, email_df, on='firm_id', how='inner')
    business_email_df = pd.merge(business_email_df, url_df, on='firm_id', how='left')

    # Rename the columns to match the convention for the function main_scrape_urls.
    business_email_df.rename(columns={'firm_id':'BusinessId','company_name':'BusinessName','email':'Email','url':'Website'}, inplace=True)

    print(business_email_df[['BusinessId','BusinessName','url_type_id','url_status_id']])
    # Update the dataframe with the new urls that have been generated from email and business name.
    update_df = main_scrape_urls(business_email_df)
    #print(update_df[['BusinessId','Website','status_code']])

    # Extract the domain name from the url and update the url_status_id column
    update_df['domain'] = update_df['Website'].apply(getDomainName)
    update_df['url_status_id'] = update_df['status_code'].apply(lambda x: 1 if x == 200 else 3)
    
    # Rename the columns to match the convention for the function logGeneratedUrlToDB
    update_df.rename(columns={'BusinessId':'firm_id','Website':'url'}, inplace=True)
    print(update_df[['firm_id','url','status_code','domain','url_status_id']])
    
    logGeneratedUrlToDB(con, update_df, saId)

    #terminateScriptActivity(con, mnsuMeta, saId, errorCode=23502, errorText='Not Null Violation on firm_id')

    #logProcessedToDB(con, update_df, sId, saId)