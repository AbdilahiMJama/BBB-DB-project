'''
Written by Spring 2025 MNSU project team
This is the generate_url file that generates urls from either the business name or the email given.
It does the following:
 1. Pulls data from the business, email and url table on the firm id where the urls are missing.
 2. Logs the processed firm_ids (Business Ids) in a processed table (mnsu_firm_processed) to keep track of the processed firms.
 3. Generates the URLs from business names with helper functions.
 4. Logs the generated URLs into a table (mnsu_generated_url).
 5. Logs the script id and script activity id to its various tables to keep track of all the script activities.
'''

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
import sqlalchemy as sa


#Script configuration
SCRIPT_NAME = 'generating_urls_final_s2025'
SCRIPT_VERSION = '1.0.3'
VERSION_NOTE = None         # put version note string here, otherwise None
CONNECT_USER = 'ABDI'
CONNECT_DB = 'MNSU'
CONNECT_INSTANCE = 'SANDBOX'
CONNECT_SCHEMA = 'spring2025'

#Database table names
SCRIPT_TABLE = 'mnsu_script'
SCRIPT_ACTIVITY_TABLE = 'mnsu_script_activity'
PROCESSED_TABLE = 'mnsu_firm_processed'
BUSINESS_TABLE = 'tblfirms_firm'
NAME_TABLE = 'tblfirms_firm_companyname'
EMAIL_TABLE = 'tblfirms_firm_email'
URL_TABLE = 'tblfirms_firm_url'
GENERATED_URL_TABLE = 'mnsu_generated_firm_url'

#batch Processing configuration
BATCH_SIZE = 300
MNSU_URL_ID = 1

errorCode = None
errorText = None


def getDomainName(url):
    """   
    Extracts the domain name from a URL in the dataframe.
    :param url: The URL string.
    :return:The domain name, or None if the URL is invalid.
    """
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None

def logGeneratedUrlToDB(engine,processedRows,saId):
    """
    Log the generated urls to the database
    :param engine: sqlalchemy engine
    :param processedRows: dataframe of processed rows
    :param activityId: script activity id
    :
    :return: None
    """
    assert isinstance(processedRows,pd.core.frame.DataFrame)
    
    #Select required columns
    processedRows = processedRows[['firm_id','url','domain', 'main', 'url_type_id', 'url_status_id']]
    
    processedRows[['mnsu_script_activity_id']] = saId
    processedRows[['note']] = 'Testing generated URLs'
    processedRows[['confidence_level']] = 1
    #Save to database
    processedRows.to_sql(name=GENERATED_URL_TABLE,
                                con=engine,
                                schema=CONNECT_SCHEMA,
                                if_exists='append',
                                index=False)
def process_urls_in_batches(con, mnsuMeta, sId, saId, batch_size=BATCH_SIZE):
    """
    Pull, process, and put the URLs in batches into the database.
    :param: con database connection
    :param: mnsuMeta: metadata object
    :param: sId: script id
    :param: saId: script activity id
    :param: batch_size: number of records to pull and push at once (default 50)
    :return : None
    """
    processed_count = 0
    print("\n=== Starting URL Processing ===")
    print(f"Script ID: {sId}")
    print(f"Script Activity ID: {saId}")
    print(f"Batch Size: {batch_size}")
    print("==============================\n")

    while True:
        print(f"\n--- Starting Batch {(processed_count // batch_size) + 1} ---")
        print("Pulling data from database...")
        dfs = getBusinessDataBatch(con, mnsuMeta, sId, batch_size)
        
        #Check if there's no more data to process
        if not dfs or dfs[BUSINESS_TABLE].empty:
            print("\n=== Process Complete ===")
            print(f"Total records processed: {processed_count}")
            print("========================")
            break

        # Get the required dataframes
        business_df = dfs[BUSINESS_TABLE][['firm_id']]
        email_df = dfs[EMAIL_TABLE][['firm_id', 'email']]
        name_df = dfs[NAME_TABLE][['firm_id', 'company_name']]
        url_df = dfs[URL_TABLE][['firm_id', 'url', 'main', 'url_type_id', 'url_status_id']]

        print(f"Records in current batch: {len(business_df)}")

        # Merge dataframes
        business_email_df = pd.merge(name_df, email_df, on='firm_id', how='inner')
        business_email_df = pd.merge(business_email_df, url_df, on='firm_id', how='left')

        # Remove duplicates
        business_email_df = business_email_df.drop_duplicates(subset='firm_id')

        #Generate and process URLs
        update_df = main_scrape_urls(business_email_df)
        update_df['domain'] = update_df['url'].apply(getDomainName)
        update_df['url_status_id'] = update_df['status_code'].apply(lambda x: 1 if x == 200 else 3)

        # Push this batch immediately
        print("Pushing generated URLs to database...")
        logGeneratedUrlToDB(con, update_df, saId)
        #log processed businesses
        print("Logging processed businesses...")
        logProcessedToDB(con, business_df, sId, saId)
        
        #Update progress
        processed_count += len(business_df)
        print(f"\n✓ Batch {(processed_count // batch_size)} completed")
        print(f"✓ Records in this batch: {len(business_df)}")
        print(f"✓ Total records processed: {processed_count}")

#running the code
if __name__=='__main__':
    
    print("\n=== URL Generation Script Starting ===")
    #Load Environment Variables
    load_dotenv()
    #Create connection
    con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')

   #Create a metadata object
    mnsuMeta = sa.schema.MetaData(schema=CONNECT_SCHEMA)
      
    #Get the script ID and script activity ID
    sId = getScriptId(con, mnsuMeta)
    saId = initiateScriptActivity(con, mnsuMeta,sId) 
    print(sId)
    #Get the business data 
    try:
        process_urls_in_batches(con, mnsuMeta, sId, saId, batch_size=BATCH_SIZE)
        
        print("\nScript completed successfully!")
        print("Terminating script activity...")
        terminateScriptActivity(con, mnsuMeta, saId)
    except Exception as e:
        print("\n!!! Error occurred !!!")
        print(f"Error message: {str(e)}")
        print("Terminating script activity with error...")
        terminateScriptActivity(con, mnsuMeta, saId, errorCode=errorCode, errorText=str(e))
        raise
    finally:
        print("\n=== Script Execution Finished ===")
