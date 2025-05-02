import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import connect_iabbb as ci
from setup import getBusWoutEml
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData
from main_url_scrape import main_scrape_urls
from data_extraction import extract_email_data, extract_phone_data, contains_phone_number





SCRIPT_NAME = 'generating_urls_for_ten_firms_test_s2025'
SCRIPT_VERSION = '1.0'
VERSION_NOTE = None         # put version note string here if desired, otherwise None
CONNECT_USER = 'AMANUEL'
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
BATCH_SIZE = 10


load_dotenv()

DB = os.environ.get("DB")
# Create connection
con = ci.connect(db='MNSU', instance='SANDBOX', user='AMANUEL', engine='sqlalchemy')


metadata_obj = MetaData()
metadata_obj.reflect(bind=con)
test = getBusWoutEml(con,metadata_obj,None,50)

print(test.keys())

# Assuming 'test' is a dictionary containing DataFrames for each table

# Extract relevant DataFrames
business_df = test[BUSINESS_TABLE][['firm_id']]
address_df = test[ADDRESS_TABLE][['firm_id', 'zip']]
name_df = test[NAME_TABLE][['firm_id', 'company_name']]
email_df = test[EMAIL_TABLE][['firm_id', 'email']]
phone_df = test[PHONE_TABLE][['firm_id', 'phone']]
url_df = test[URL_TABLE][['firm_id', 'url']]

# # Print the extracted columns for each DataFrame
# print("Business DataFrame:")
# print(business_df)

# print("\nAddress DataFrame:")
# print(address_df)

# print("\nName DataFrame:")
# print(name_df)

# print("\nEmail DataFrame:")
# print(email_df)

# print("\nPhone DataFrame:")
# print(phone_df)

# print("\nURL DataFrame:")
# print(url_df)




def addScrape(urlDf, emlDf):
    """
    Scrapes email from a given URL using the extract_email_data function.
    
    Args:
        urlDf (pd.DataFrame): DataFrame containing URLs.
        emlDf (pd.DataFrame): DataFrame to store scraped email addresses.
        
    """

    for index, row in urlDf.iterrows():
        # print(row[1])
        # print(type(row[1]))
        url = row['url']
        firm_id = row['firm_id']
        
        # Scrape email data
        scrapedEmail = extract_adress_data(firm_id, url)
        print(scrapedEmail, url)
        
        # Append the scraped emails to the email DataFrame
        # for now, we're only considering 2 emails per firm ID
        i = 0
        while i < 2 and scrapedEmail != None and  i < len(scrapedEmail):
            # Check if the email is already in the DataFrame
            if scrapedEmail[i] not in emlDf['email'].values:
                print(True)
                # Create a new row as a DataFrame
                new_row = pd.DataFrame({'firm_id': [firm_id], 'email': [scrapedEmail[i]]})
                
                # Use pd.concat() to append the new row
                emlDf = pd.concat([emlDf, new_row], ignore_index=True)

                i += 1
            else: 
                i += 1
                continue

    return emlDf
        
# print(emlScrape(url_df, email_df))
