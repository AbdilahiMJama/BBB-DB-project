# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 10:25:37 2025

@author: Eli Johnson
"""


import connect_iabbb as ci
import pandas as pd
import os
#import configparser
from sqlalchemy import text


###################################
# Global variables and structures #
###################################

SCRIPT_NAME = 'data_quality_s2025'
SCRIPT_VERSION = '1.0.1'
VERSION_NOTE = None         # put version note string here if desired, otherwise None
CONNECT_USER = 'ABDI'
CONNECT_DB = 'MNSU'
CONNECT_INSTANCE = 'SANDBOX'
CONNECT_SCHEMA = 'spring2025'
SCRIPT_TABLE = 'mnsu_script'



def getExistingScriptId(engine):
    """
    Returns the primary key associated with the current script name and 
    version, or false if none exists
    
    engine: a sqlalchemy engine that the connection will be created from
    """

    with engine.connect() as con:
        qry = f"""
    SELECT mnsu_script_id 
    FROM {SCRIPT_TABLE} 
    WHERE mnsu_script_name = :SCRIPT_NAME
        AND mnsu_script_version = :SCRIPT_VERSION
        """
        params = {'SCRIPT_NAME':SCRIPT_NAME,
                  'SCRIPT_VERSION':SCRIPT_VERSION}
        res = con.execute(text(qry),params)
        if res.rowcount==0:
            return False
        return next(res)[0]


def getScriptId(engine):
    """
    Returns the pkey associated with the current script if one exists
    Otherwise, inserts a new row and returns the pkey
    
    engine: a sqlalchemy engine that the connection will be created from
    """
    if (script_id:=getExistingScriptId(engine)):
        return script_id
    
    with engine.connect() as con:
        qry = f"""
        INSERT INTO {SCRIPT_TABLE} (mnsu_script_name,mnsu_script_version,description)
        VALUES (:SCRIPT_NAME,:SCRIPT_VERSION,:VERSION_NOTE)
        RETURNING mnsu_script_id
        """
        params = {'SCRIPT_NAME':SCRIPT_NAME,
                  'SCRIPT_VERSION':SCRIPT_VERSION,
                  'VERSION_NOTE':VERSION_NOTE}
        res = con.execute(text(qry),params)
        con.commit()
        return next(res)[0]
    

    
    