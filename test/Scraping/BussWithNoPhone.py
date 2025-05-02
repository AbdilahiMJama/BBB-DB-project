import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import connect_iabbb as ci
from setup import getBusWoutPhone, getScriptId, getExistingScriptId, initiateScriptActivity, terminateScriptActivity, logProcessedToDB
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData
from main_url_scrape import main_scrape_urls
from data_extraction import extract_email_data, extract_phone_data, contains_phone_number
import sqlalchemy as sa
from sqlalchemy import MetaData


SCRIPT_NAME = 'generating_phones_for_ten_firms_test_s2025'
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
ADDRESS_TABLE = 'tblfirms_firm_address'
NAME_TABLE = 'tblfirms_firm_companyname'
EMAIL_TABLE = 'tblfirms_firm_email'
PHONE_TABLE = 'tblfirms_firm_phone'
URL_TABLE = 'tblfirms_firm_url'
GENERATED_PHONE_TABLE = 'mnsu_generated_firm_phone'
BATCH_SIZE = 1000

errorCode = None 
errorText = None


def phoneScrape(urlDf, phoneDf):
    """
    Scrapes email from a given URL using the extract_email_data function.
    
    Args:
        urlDf (pd.DataFrame): DataFrame containing URLs.
        emlDf (pd.DataFrame): DataFrame to store scraped email addresses.
        
    """

    for index, row in urlDf.iterrows():
        url = row['url']
        firm_id = row['firm_id']
        
        # Scrape email data
        scrapedPhone = extract_phone_data(firm_id, url)
        print(scrapedPhone, url)
        
        # Append the scraped phones to the email DataFrame
        # for now, we're only considering 2 phones per firm ID
        i = 0
        while i < 2 and scrapedPhone != None and  i < len(scrapedPhone):
            # Check if the email is already in the DataFrame
            if scrapedPhone[i] not in phoneDf['phone'].values:
                print(True)
                # Create a new row as a DataFrame
                new_row = pd.DataFrame({'firm_id': [firm_id], 'phone': [scrapedPhone[i]]})
                
                # Use pd.concat() to append the new row
                phoneDf = pd.concat([phoneDf, new_row], ignore_index=True)

                i += 1
            else: 
                i += 1
                continue

    return phoneDf


def logGeneratedPhoneToDB(engine, processedRows, saId):
    """
    Log the generated urls to the database
    :param engine: sqlalchemy engine
    :param processedRows: dataframe of processed rows
    :param activityId: script activity id
    :
    :return: None
    """
    assert isinstance(processedRows,pd.core.frame.DataFrame)
    processedRows = processedRows[['firm_id','phone','phone_type_id','phone_status_id']]
    
    processedRows[['mnsu_script_activity_id']] = saId
    processedRows[['note']] = 'Testing generated phones'
    processedRows[['confidence_level']] = 1
    
    processedRows.to_sql(name=GENERATED_PHONE_TABLE,
                                con=engine,
                                schema=CONNECT_SCHEMA,
                                if_exists='append',
                                index=False)

def processPhonesInBatches(con,mnsuMeta,sId,saId,batch_size=BATCH_SIZE):
    """
    """
    processed_count = 0
    print("\n=== Starting Phone Processing ===")
    print(f"Script ID: {sId}")
    print(f"Script Activity ID: {saId}")
    print(f"Batch Size: {batch_size}")
    print("==============================\n")
    while True:
        print(f"\n--- Starting Batch {(processed_count // batch_size) + 1} ---")
        print("Pulling data from database...")
        dfs = getBusWoutPhone(con, mnsuMeta, sId, batch_size)
        
        if not dfs or dfs[BUSINESS_TABLE].empty:
            print("\n=== Process Complete ===")
            print(f"Total records processed: {processed_count}")
            print("========================")
            break

        # Get the required dataframes
        business_df = dfs[BUSINESS_TABLE][['firm_id']]
        phone_df = dfs[PHONE_TABLE][['firm_id', 'phone','phone_status_id','phone_type_id']]
        url_df = dfs[URL_TABLE][['firm_id', 'url']]
        print(f"Records in current batch: {len(business_df)}")

        # Merge dataframes
        updated_phone_df = phoneScrape(url_df,phone_df)
        
        # Push this batch immediately
        print("Pushing generated Emails to database...")
        logGeneratedPhoneToDB(con, updated_phone_df, saId)

        # Log processed businesses
        print("Logging processed businesses...")
        logProcessedToDB(con, business_df, sId, saId)
        print(updated_phone_df)
        processed_count += len(business_df)
        print(f"\n✓ Batch {(processed_count // batch_size)} completed")
        print(f"✓ Records in this batch: {len(business_df)}")
        print(f"✓ Total records processed: {processed_count}")

if __name__ == '__main__':
    print("\n=== Phone Generation Script Starting ===")
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
    try:
        processPhonesInBatches(con, mnsuMeta, sId, saId, batch_size=BATCH_SIZE)
        print("\nScript completed successfully!")
        print("Terminating script activity...")
        #terminateScriptActivity(con, mnsuMeta, saId)
    except Exception as e:
        print("\n!!! Error occurred !!!")
        print(f"Error message: {str(e)}")
        print("Terminating script activity with error...")
        terminateScriptActivity(con, mnsuMeta, saId, errorCode=errorCode, errorText=str(e))
        raise
    finally:
        print("\n=== Script Execution Finished ===")