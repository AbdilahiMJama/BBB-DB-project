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
MNSU_SCRIPT_ACTIVITY = None



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

'''
def getExisitingUrlId(engine, metadata, scriptTable=None):
    if scriptTable==None:
        scriptTable = sa.Table(GENERATED_URL_TABLE,metadata,autoload_with=engine)
    qry = sa.select(scriptTable).filter_by(mnsu_url_id=MNSU_URL_ID, 
                                        mnsu_script_actvity=MNSU_SCRIPT_ACTIVITY)

    with engine.connect() as con:
        res = con.execute(qry)

        if res.rowcount==0:
            return False
        return next(res)[0]

def getUrlId(engine, metadata):
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
    if (script_id:=getExistingScriptId(engine,metadata,scriptTable=scriptTable)):
        return script_id
    
    with engine.connect() as con:
        qry = sa.insert(scriptTable).returning(scriptTable.c.mnsu_script_id)
        params = {'mnsu_url_id':MNSU_URL_ID, 'mnsu_script_activity':MNSU_SCRIPT_ACTIVITY}
        res = con.execute(qry,params)
        con.commit()
        return next(res)[0]
'''

def logGeneratedUrlToDB(engine,processedRows, saId):
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
    processedRows['mnsu_url_id'] = 1
    processedRows['mnsu_script_activity_id'] = saId
    processedRows.rename(columns=columns, inplace=True)
    processedRows = processedRows[['firm_id','url','domain', 'main', 'url_type_id', 'url_status_id']]
    processedRows['note'] = 'Testing generated URLs'
    processedRows['confidence_level'] = 1
    
    print(processedRows.columns)
    
    
    
    processedRows.to_sql(name=GENERATED_URL_TABLE,
                         con=engine,
                         schema=CONNECT_SCHEMA,
                         if_exists='append',
                         index=False)
    

#logProcessedToDB(con, update_df, sId, saId)
#logGeneratedUrlToDB(con, update_df)
#terminateScriptActivity(eng, mnsuMeta, saId, errorCode=errorCode, errorText=errorText)

if __name__=='__main__':
    
    load_dotenv()
    # Create connection
    con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')


    metadata_obj = MetaData()
    metadata_obj.reflect(bind=con)
      
    # Build DB metadata object
    sId = getScriptId(con, metadata_obj)
    saId = initiateScriptActivity(con, metadata_obj) 

    dfs = getBusinessDataBatch(con,metadata_obj,None,50) 

    
    email_df = dfs[EMAIL_TABLE][['firm_id', 'email']]
    name_df = dfs[NAME_TABLE][['firm_id', 'company_name']]
    url_df = dfs[URL_TABLE][['firm_id', 'url','main','url_type_id','url_status_id']]

    business_email_df = pd.merge(name_df, email_df, on='firm_id', how='inner')
    business_email_df = pd.merge(business_email_df, url_df, on='firm_id', how='left')

    business_email_df.rename(columns={'firm_id':'BusinessId','company_name':'BusinessName','email':'Email','url':'Website'}, inplace=True)

    print(business_email_df[['BusinessId','BusinessName','url_type_id','url_status_id']])
    update_df = main_scrape_urls(business_email_df)
    print(update_df[['BusinessId','Website','status_code']])

    update_df['domain'] = update_df['Website'].apply(getDomainName)
    update_df['url_status_id'] = update_df['status_code'].apply(lambda x: 1 if x == 200 else 3)
    print(update_df[['Website','status_code','domain','url_status_id']])

    
    logGeneratedUrlToDB(con, update_df, saId)

    logProcessedToDB(con, update_df, sId, saId)


    terminateScriptActivity(con, metadata_obj, saId, errorCode=None, errorText=None)

