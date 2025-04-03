import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import connect_iabbb as ci
from setupcopy2 import getBusinessDataBatch2
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData
from main_url_scrape import main_scrape_urls
from data_extraction import extract_email_data, extract_phone_data





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
test = getBusinessDataBatch2(con,metadata_obj,None,50)

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

for url in url_df['url']:
    #print(url)
    #main_scrape_urls(url)
    scrapedEmail = extract_email_data(00000, url)
    print(scrapedEmail)

# for url in url_df['url']:
#     #print(url)
#     #main_scrape_urls(url)
#     scrapedPhone = extract_phone_data(00000, url)
#     print(scrapedPhone)

scrapedPhone = extract_phone_data(00000, "https://www.jodcpa.com/")
print(scrapedPhone)



# # Perform a left merge to get firms with URLs
# business_url_df = pd.merge(business_df, url_df, on='firm_id', how='left')

# # Filter out firms that have emails
# business_url_df = business_url_df[~business_url_df['firm_id'].isin(email_df['firm_id'])]

# # Merge with other DataFrames to include additional information
# business_url_df = pd.merge(business_url_df, name_df, on='firm_id', how='left')
# business_url_df = pd.merge(business_url_df, address_df, on='firm_id', how='left')
# business_url_df = pd.merge(business_url_df, phone_df, on='firm_id', how='left')
# business_url_df = pd.merge(business_url_df, email_df, on='firm_id', how='left')  # Include email_df

# # Rename columns for clarity
# business_url_df.rename(columns={
#     'firm_id': 'BusinessId',
#     'company_name': 'BusinessName',
#     'url': 'Website',
#     'zip': 'ZipCode',
#     'phone': 'Phone',
#     'email': 'Email'  # Rename email column
# }, inplace=True)

# # Display the resulting DataFrame
# print(business_url_df)