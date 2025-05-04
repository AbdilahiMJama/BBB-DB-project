"""
This file tests the connection to the database
"""
import os, sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config.connect_iabbb as ci
import pandas as pd
con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')

try:
    # Test 1: Check connection
    test_query = "SELECT 1 as connection_test"
    result = pd.read_sql(test_query, con)
    print("Connection test:", "Success!" if len(result) > 0 else "Failed")
except Exception as e:
    print("Error:", str(e))
finally:
    if con:
        con.dispose()