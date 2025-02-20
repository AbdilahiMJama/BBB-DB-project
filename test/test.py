import os
import config.connect_iabbb as ci
import pandas as pd



# Create connection
con = ci.connect(db='MNSU', instance='SANDBOX', user='ABDI', engine='sqlalchemy')

#Tests here