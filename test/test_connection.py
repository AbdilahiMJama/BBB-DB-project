import os
import config.connect_iabbb as ci
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

    #test query goes here
    #Example:
    
except Exception as e:
    print("Error:", str(e))
finally:
    if con:
        con.dispose()




