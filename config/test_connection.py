import os
import connect_iabbb as ci
import pandas as pd
from dotenv import load_dotenv



load_dotenv()

DB = os.environ.get("DB")
# Create connection
con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')

try:
    # Test 1: Check connection
    test_query = "SELECT 1 as connection_test"
    result = pd.read_sql(test_query, con)
    print("Connection test:", "Success!" if len(result) > 0 else "Failed")

    # Test 2: List all schemas
    schema_query = """
    SELECT schema_name 
    FROM information_schema.schemata
    """
    schemas = pd.read_sql(schema_query, con)
    print("\nAvailable schemas:")
    print(schemas)

    # Test 3: List all tables in all schemas
    tables_query = """
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    """
    tables = pd.read_sql(tables_query, con)
    print("\nAvailable tables in all schemas:")
    print(tables)

    #If you know a specific table name, try querying it:
    #Example:
    '''
    sample_query = """
    SELECT DISTINCT 
	f.firm_id,
	f.createdon
    FROM tblfirms_firm f    
    LEFT JOIN mnsu_firm_processed mfp
	ON f.firm_id = mfp.firm_id
    LEFT JOIN tblfirms_firm_url u
	ON f.firm_id = u.firm_id
    WHERE f.active
	AND NOT EXISTS (SELECT 1 FROM mnsu_firm_processed mfp WHERE mfp.mnsu_script_id = 1 AND mfp.firm_id = f.firm_id)
	AND f.outofbusiness_status IS NULL
	AND u.firm_id IS NULL
	AND f.createdon <= CURRENT_TIMESTAMP - '1 month'::INTERVAL
    ORDER BY f.createdon DESC NULLS LAST
    LIMIT 1000;
  """
    data = pd.read_sql(sample_query, con)
    print("\nSample data:")
    print(data)
    '''
except Exception as e:
    print("Error:", str(e))
finally:
    if con:
        con.dispose()
