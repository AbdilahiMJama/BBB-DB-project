import os, sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config.connect_iabbb as ci

con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')


print(con)
