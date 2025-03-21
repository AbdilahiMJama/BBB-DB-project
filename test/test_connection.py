import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import connect_iabbb as ci
from setup import getBusinessDataBatch
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData
from main_url_scrape import main_scrape_urls




SCRIPT_NAME = 'generating_urls_for_ten_firms_test_s2025'
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
BATCH_SIZE = 10


load_dotenv()

DB = os.environ.get("DB")
# Create connection
con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')


metadata_obj = MetaData()
metadata_obj.reflect(bind=con)
test = getBusinessDataBatch(con,metadata_obj,None,10)

print(test.keys())


business_df = test[BUSINESS_TABLE][['firm_id']]
address_df = test[ADDRESS_TABLE][['firm_id','zip']]
name_df = test[NAME_TABLE][['firm_id', 'company_name']]
email_df = test[EMAIL_TABLE][['firm_id', 'email']]
phone_df = test[PHONE_TABLE][['firm_id', 'phone']]
url_df = test[URL_TABLE][['firm_id', 'url']]


business_email_df = pd.merge(name_df, email_df, on='firm_id', how='inner')
business_email_df = pd.merge(business_email_df, url_df, on='firm_id', how='inner')

business_email_df.rename(columns={'firm_id':'BusinessId','company_name':'BusinessName','email':'Email','url':'Website'}, inplace=True)
#business_email_df = business_email_df['Website']

print(business_email_df.head())



#merged_df = pd.merge(business_df, name_df, on='firm_id', how='left')
#merged_df = pd.merge(merged_df, email_df, on='firm_id', how='inner')
#merged_df = pd.merge(merged_df, url_df, on='firm_id', how='left')
#merged_df = pd.merge(merged_df, address_df, on='firm_id', how='left')

#print(business_email_df[business_email_df.duplicated()])
#print(business_email_df.head())
#merged_df = pd.merge(merged_df, phone_df, on='firm_id', how='left')
#
 
#merged_df.rename(columns={'company_name':'BusinessName','email':'Email','url':'Website','firm_id':'BusinessId','zip': 'PostalCode'}, inplace=True)
#print(merged_df[['BusinessId','BusinessName','Email','Website']])

#update_df = main_scrape_urls(merged_df)
#print(update_df[['BusinessName','Website']])

