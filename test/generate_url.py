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

### For the url type (later use) use a dictionary.

SCRIPT_NAME = 'generating_urls_final_s2025'
SCRIPT_VERSION = '1.0.3'
VERSION_NOTE = None         # put version note string here, otherwise None
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
BATCH_SIZE = 300
MNSU_URL_ID = 1

errorCode = None
errorText = None

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
    assert isinstance(processedRows,pd.core.frame.DataFrame)
    processedRows = processedRows[['firm_id','url','domain', 'main', 'url_type_id', 'url_status_id']]
    
    processedRows[['mnsu_script_activity_id']] = saId
    processedRows[['note']] = 'Testing generated URLs'
    processedRows[['confidence_level']] = 1
    #print(processedRows.columns)

    
    processedRows.to_sql(name=GENERATED_URL_TABLE,
                                con=engine,
                                schema=CONNECT_SCHEMA,
                                if_exists='append',
                                index=False)


def process_urls_in_batches(con, mnsuMeta, sId, saId, batch_size=BATCH_SIZE):
    """
    Pull, process, and put the URLs in batches into the database.
    Args:
        con: database connection
        mnsuMeta: metadata object
        sId: script id
        saId: script activity id
        batch_size: number of records to pull and push at once (default 50)
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

        # Process URLs for this batch remove from the fuction itself print
        update_df = main_scrape_urls(business_email_df)
        
        # Extract domain and update status
        update_df['domain'] = update_df['url'].apply(getDomainName)
        update_df['url_status_id'] = update_df['status_code'].apply(lambda x: 1 if x == 200 else 3)

        # Push this batch immediately
        print("Pushing generated URLs to database...")
        logGeneratedUrlToDB(con, update_df, saId)

        # Log processed businesses
        print("Logging processed businesses...")
        logProcessedToDB(con, business_df, sId, saId)
        
        processed_count += len(business_df)
        print(f"\n✓ Batch {(processed_count // batch_size)} completed")
        print(f"✓ Records in this batch: {len(business_df)}")
        print(f"✓ Total records processed: {processed_count}")
        

if __name__=='__main__':
    
    print("\n=== URL Generation Script Starting ===")
    # Load Environment Variables
    load_dotenv()
    # Create connection
    con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')

   # Create a metadata object
    mnsuMeta = sa.schema.MetaData(schema=CONNECT_SCHEMA)
      
    # Get the script ID and script activity ID
    sId = getScriptId(con, mnsuMeta)
    saId = initiateScriptActivity(con, mnsuMeta,sId) 
    print(sId)
    # Get the business data 
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

    '''
    dfs = getBusinessDataBatch(con,mnsuMeta,sId,50) 

    # Get the email, name and url tables as a dataframe from the batch
    # Put a business dataframe (BUSINESS_TABLE), return this table to logProcessedToDB
    business_df = dfs[BUSINESS_TABLE][['firm_id']]
    email_df = dfs[EMAIL_TABLE][['firm_id', 'email']]
    name_df = dfs[NAME_TABLE][['firm_id', 'company_name']]
    url_df = dfs[URL_TABLE][['firm_id', 'url','main','url_type_id','url_status_id']]

    # Merge the three dataframes on their firm id
    business_email_df = pd.merge(name_df, email_df, on='firm_id', how='inner')
    business_email_df = pd.merge(business_email_df, url_df, on='firm_id', how='left')
    # Get output of the merged dataframe
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

    
    # Log the generated URLs to the database
    logGeneratedUrlToDB(con, update_df, saId)

    # Log the processed rows to the database
    logProcessedToDB(con, business_df, sId, saId)

    # Terminate the script activity
    terminateScriptActivity(con, mnsuMeta, saId, errorCode=errorCode, errorText=errorText)
    '''