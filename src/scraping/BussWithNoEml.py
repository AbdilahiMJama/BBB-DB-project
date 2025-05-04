'''
Written by Spring 2025 MNSU project team
This script generates emails for businesses that don't have an email address.
It does the following:
 1. Pulls data from the business, email and url table on the firm id where the email are missing.
 2. Logs the processed firm_ids (Business Ids) in a processed table (mnsu_firm_processed) to keep track of the processed firms.
 3. Scrape emails from the urls with helper functions.
 4. Logs the generated emails into a table (mnsu_generated_firm_email).  
'''
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import config.connect_iabbb as ci
from setup import getBusWoutEml, getScriptId, getExistingScriptId, initiateScriptActivity, terminateScriptActivity, logProcessedToDB
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData
from scripts.main_url_scrape import main_scrape_urls
from scripts.data_extraction import extract_email_data, extract_phone_data, contains_phone_number
from urllib.parse import urlparse
import sqlalchemy as sa


#Configuration
SCRIPT_NAME = 'generating_emails_firms_with_no_emails_s2025'
SCRIPT_VERSION = '1.0.4'
VERSION_NOTE = None         #put version note string here if desired, otherwise None
CONNECT_USER = 'ABDI'
CONNECT_DB = 'MNSU'
CONNECT_INSTANCE = 'SANDBOX'
CONNECT_SCHEMA = 'spring2025'

#Tables
SCRIPT_TABLE = 'mnsu_script'
SCRIPT_ACTIVITY_TABLE = 'mnsu_script_activity'
PROCESSED_TABLE = 'mnsu_firm_processed'
BUSINESS_TABLE = 'tblfirms_firm'
ADDRESS_TABLE = 'tblfirms_firm_address'
NAME_TABLE = 'tblfirms_firm_companyname'
EMAIL_TABLE = 'tblfirms_firm_email'
PHONE_TABLE = 'tblfirms_firm_phone'
URL_TABLE = 'tblfirms_firm_url'
GENERATED_EMAIL_TABLE = 'mnsu_generated_firm_email'
BATCH_SIZE = 300

errorCode = None 
errorText = None

def emlScrape(urlDf, emlDf):
    """
    Scrapes email from a given URL using the extract_email_data function.
    
    Args:
        urlDf (pd.DataFrame): DataFrame containing URLs.
        emlDf (pd.DataFrame): DataFrame to store scraped email addresses.
        
    """

    for index, row in urlDf.iterrows():
        url = row['url']
        firm_id = row['firm_id']
        
        #Scrape email data
        scrapedEmail = extract_email_data(firm_id, url)
        #Append the scraped emails to the email DataFrame
        #for now, we're only considering 2 emails per firm ID
        i = 0
        while i < 2 and scrapedEmail != None and  i < len(scrapedEmail):
            #Check if the email is already in the DataFrame
            if scrapedEmail[i] not in emlDf['email'].values:
                #Create a new row as a DataFrame
                new_row = pd.DataFrame({'firm_id': [firm_id], 'email': [scrapedEmail[i]]})
                
                #Use pd.concat() to append the new row
                emlDf = pd.concat([emlDf, new_row], ignore_index=True)

                i += 1
            else: 
                i += 1
                continue

    return emlDf
       

def getDomainName(email):
    """   
    Extracts the domain name from an email address.
    :param url: The email string.
    :return:The domain name, or None if the URL is invalid.
    """
    try:
        return email.split('@')[1]
    except:
        return None

def logGeneratedEmailToDB(engine, processedRows, saId):
    """
    Log the generated urls to the database
    :param engine: sqlalchemy engine
    :param processedRows: dataframe of processed rows
    :param activityId: script activity id
    :
    :return: None
    """
    assert isinstance(processedRows,pd.core.frame.DataFrame)
    processedRows = processedRows[['firm_id','email','email_type_id','domain','email_status_id','address_id']]
    
    processedRows[['mnsu_script_activity_id']] = saId
    processedRows[['note']] = 'Testing generated emails'
    processedRows[['confidence_level']] = 1
    
    processedRows.to_sql(name=GENERATED_EMAIL_TABLE,
                                con=engine,
                                schema=CONNECT_SCHEMA,
                                if_exists='append',
                                index=False)

def processEmailsInBatches(con,mnsuMeta,sId,saId,batch_size=BATCH_SIZE):
    """
    """
    processed_count = 0
    print("\n=== Starting Email Processing ===")
    print(f"Script ID: {sId}")
    print(f"Script Activity ID: {saId}")
    print(f"Batch Size: {batch_size}")
    print("==============================\n")

    while True:
        print(f"\n--- Starting Batch {(processed_count // batch_size) + 1} ---")
        print("Pulling data from database...")
        dfs = getBusWoutEml(con, mnsuMeta, sId, batch_size)
        
        if not dfs or dfs[BUSINESS_TABLE].empty:
            print("\n=== Process Complete ===")
            print(f"Total records processed: {processed_count}")
            print("========================")
            break

        #Get the required dataframes
        business_df = dfs[BUSINESS_TABLE][['firm_id']]
        email_df = dfs[EMAIL_TABLE][['firm_id', 'email','email_type_id','email_status_id','address_id']]
        url_df = dfs[URL_TABLE][['firm_id', 'url']]
        print(f"Records in current batch: {len(business_df)}")

        #Merge dataframes
        updated_email_df = emlScrape(url_df,email_df)
        
        #Extract domain and update status
        updated_email_df['domain'] = updated_email_df['email'].apply(getDomainName)

        #Push this batch immediately
        print("Pushing generated Emails to database...")
        logGeneratedEmailToDB(con, updated_email_df, saId)

        #Log processed businesses
        print("Logging processed businesses...")
        logProcessedToDB(con, business_df, sId, saId)
        print(updated_email_df)
        processed_count += len(business_df)
        print(f"\n✓ Batch {(processed_count // batch_size)} completed")
        print(f"✓ Records in this batch: {len(business_df)}")
        print(f"✓ Total records processed: {processed_count}")

def main():
    print("\n=== Email Generation Script Starting ===")
    #Load Environment Variables
    load_dotenv()
    #Create connection
    con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')

    #Create a metadata object
    mnsuMeta = sa.schema.MetaData(schema=CONNECT_SCHEMA)
      
    #Get the script ID and script activity ID
    sId = getScriptId(con, mnsuMeta)
    saId = initiateScriptActivity(con, mnsuMeta,sId) 
    #Get the business data 
    try:
        processEmailsInBatches(con, mnsuMeta, sId, saId, batch_size=BATCH_SIZE)
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

if __name__ == '__main__':
    main()
    