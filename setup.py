# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 10:25:37 2025

@author: Eli Johnson
"""


import connect_iabbb as ci
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import sqlalchemy as sa


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
SCRIPT_ACTIVITY_TABLE = 'mnsu_script_activity'
PROCESSED_TABLE = 'mnsu_firm_processed'
BUSINESS_TABLE = 'tblfirms_firm'
ADDRESS_TABLE = 'tblfirms_firm_address'
NAME_TABLE = 'tblfirms_firm_companyname'
EMAIL_TABLE = 'tblfirms_firm_email'
PHONE_TABLE = 'tblfirms_firm_phone'
URL_TABLE = 'tblfirms_firm_url'
BATCH_SIZE = 300






# Update these if something goes wrong with the execution
errorCode = None
errorText = None




def getExistingScriptId(engine,metadata,scriptTable=None):
    """
    Returns the primary key associated with the current script name and 
    version, or false if none exists
    
    engine: a sqlalchemy engine
    metadata: a sqlalchemy Metadata object
    scriptTable: (optional) a Table object for the script table. This will be 
        generated if not present, but if passed as a keyword argument, the 
        table object will be updated from the database
    """
    
    if scriptTable==None:
        scriptTable = sa.Table(SCRIPT_TABLE,metadata,autoload_with=engine)
    qry = sa.select(scriptTable).filter_by(mnsu_script_name=SCRIPT_NAME, 
                                        mnsu_script_version=SCRIPT_VERSION)

    with engine.connect() as con:
        res = con.execute(qry)

        if res.rowcount==0:
            return False
        return next(res)[0]


def getScriptId(engine,metadata):
    """
    Returns the pkey associated with the current script if one exists
    Otherwise, inserts a new row and returns the pkey
    
    engine: a sqlalchemy engine
    metadata: a sqlalchemy Metadata object
    """
    scriptTable = sa.Table(SCRIPT_TABLE,metadata,autoload_with=engine)
    if (script_id:=getExistingScriptId(engine,metadata,scriptTable=scriptTable)):
        return script_id
    
    with engine.connect() as con:
        qry = sa.insert(scriptTable).returning(scriptTable.c.mnsu_script_id)
        params = {'mnsu_script_name':SCRIPT_NAME,
                  'mnsu_script_version':SCRIPT_VERSION,
                  'description':VERSION_NOTE}
        res = con.execute(qry,params)
        con.commit()
        return next(res)[0]
    

def initiateScriptActivity(engine,metadata,scriptId=None):
    """
    Inserts a row into the script activity table when this is called
    Returns the row pkey
    
    engine: a sqlalchemy engine
    metadata: a sqlalchemy Metadata object
    scriptId: this script's pkey (optional, but intent is for this to be passed
        to reduce needless calls to the DB)
    """
    activityTable = sa.Table(SCRIPT_ACTIVITY_TABLE,metadata,autoload_with=engine)
    if scriptId==None: 
        scriptId = getScriptId(engine, metadata)
    with engine.connect() as con:
        qry = sa.insert(activityTable).returning(activityTable.c.mnsu_script_activity_id)
        params = {'mnsu_script_id':scriptId,
                  'initiated_at':datetime.now()}
        res = con.execute(qry,params)
        con.commit()
        return next(res)[0]
        
def terminateScriptActivity(engine,metadata,activityId,errorCode=None,errorText=None):
    """
    Updates the script activity row with a terminated_at timestamp
    """
    activityTable = sa.Table(SCRIPT_ACTIVITY_TABLE,metadata,autoload_with=engine)
    with engine.connect() as con:
        qry = sa.update(activityTable).where(
            activityTable.c.mnsu_script_activity_id==activityId).values(
                terminated_at=datetime.now(),
                error_code=errorCode,
                error_text=errorText)
        con.execute(qry)
        con.commit()



def getBusinessDataBatch(engine,metadata,scriptId,batchSize=BATCH_SIZE):
    """
    Returns a dict of dataframes for the next BATCH_SIZE number of firm_ids to
    be processed.  These dataframes will all share the same firm_ids
    
    engine: a sqlalchemy engine object
    metadata: a sqlalchemy Metadata object
    scriptId: this script's pkey
    batchSize: number of firm_ids to pull
    """
    
    # Create sqlalchemy table objects
    tmpTableName = 'mnsu_firm_pull_{}'.format(date.today().strftime('%Y%m%d'))
    tmpMeta = sa.schema.MetaData()
    tmpTable = sa.Table(tmpTableName,tmpMeta,
                     sa.Column('firm_id',sa.INTEGER,primary_key=True),
                     prefixes=['TEMPORARY'])    
    businessTable = sa.Table(BUSINESS_TABLE,metadata,autoload_with=engine)
    addressTable = sa.Table(ADDRESS_TABLE,metadata,autoload_with=engine)
    nameTable = sa.Table(NAME_TABLE,metadata,autoload_with=engine)
    emailTable = sa.Table(EMAIL_TABLE,metadata,autoload_with=engine)
    phoneTable = sa.Table(PHONE_TABLE,metadata,autoload_with=engine)
    urlTable = sa.Table(URL_TABLE,metadata,autoload_with=engine)
    processedTable = sa.Table(PROCESSED_TABLE,metadata,autoload_with=engine)
            
    # Timestamp from 1 month ago, used in the pull query
    aMonthAgo = datetime.now() - relativedelta(month=1)
    
    # Query selecting firm_ids
    subq1 = sa.select(1).where(
        businessTable.c.firm_id==urlTable.c.firm_id)
    subq2 = sa.select(1).where(
        sa.and_(businessTable.c.firm_id==processedTable.c.firm_id,
                processedTable.c.mnsu_script_id==scriptId))
    qry = sa.select(businessTable.c.firm_id).filter(
        ~subq1.exists()).filter(
            ~subq2.exists()).filter(
                businessTable.c.active).filter(
                    businessTable.c.outofbusiness_status.is_(None)).filter(
                        businessTable.c.createdon < aMonthAgo).order_by(
                            businessTable.c.createdon.desc()).limit(
                            batchSize)

    with engine.connect() as con:
        
        # Insert previous query into a temp table
        tmpTable.create(con)
        ins = sa.insert(tmpTable).from_select([businessTable.c.firm_id],qry)
        con.execute(ins)
        
        # Iterate through data tables
        dataTables = [businessTable,addressTable,nameTable,emailTable,phoneTable,urlTable]
        dataFrames = {}
        for dt in dataTables:
            
            # Join each table to the temp table
            dtJoin = sa.join(dt,tmpTable,dt.c.firm_id == tmpTable.c.firm_id)
            slct = sa.select(dt).select_from(dtJoin)
            
            # Then write the result to a dataframe
            dataFrames[dt.name] = pd.read_sql(slct,con)
    
    return dataFrames

def logProcessedToDB(engine,processedRows,scriptId,activityId):
    """
    Inserts the firm_ids processed by this script into the processed firms table
    
    processedRows: a dataFrame containing firm_id.  Pass only the rows that were 
        processed by this script.
    scriptId: the script's pkey
    activityId: the script activity id
    """
    
    assert isinstance(processedRows,pd.core.frame.DataFrame)
    processedRows = processedRows[['firm_id']]
    processedRows['mnsu_script_id'] = scriptId
    processedRows['mnsu_script_activity_id'] = activityId
    
    processedRows.to_sql(name=PROCESSED_TABLE,
                         con=engine,
                         schema=CONNECT_SCHEMA,
                         if_exists='append',
                         index=False)
    
    
'''   
if __name__=='__main__':
    
    
    # Instantiate sqlalchemy engine
    eng = ci.connect(db=CONNECT_DB,instance=CONNECT_INSTANCE,user=CONNECT_USER,engine='sqlalchemy')    
    # Build DB metadata object
    mnsuMeta = sa.schema.MetaData(schema=CONNECT_SCHEMA)
    # collect script ID / script Activity ID
    sId = getScriptId(eng, mnsuMeta)
    saId = initiateScriptActivity(eng, mnsuMeta)    
    # dataframes placed in a dictionary
    dfs = getBusinessDataBatch(eng, mnsuMeta, sId)
    
    
    
    
    
    #
    #  everything else goes here:
    #
    
    
    # Uncomment the next line to test writing the processed firm_ids to DB
    # Currently this just logs all the firm_ids you pulled    
#    logProcessedToDB(eng, dfs['tblfirms_firm'], sId, saId)    

    terminateScriptActivity(eng, mnsuMeta, saId, errorCode=errorCode, errorText=errorText)
    
'''


def getBusWoutEml(engine, metadata, scriptId, batchSize=BATCH_SIZE):
    """
    Returns a dict of dataframes for the next BATCH_SIZE number of firm_ids to
    be processed. These dataframes will all share the same firm_ids.
    
    This version retrieves firms that do not have emails but have URLs.
    
    engine: a sqlalchemy engine object
    metadata: a sqlalchemy Metadata object
    scriptId: this script's pkey
    batchSize: number of firm_ids to pull
    """
    
    # Create sqlalchemy table objects
    tmpTableName = 'mnsu_firm_pull_{}'.format(date.today().strftime('%Y%m%d'))
    tmpMeta = sa.schema.MetaData()
    tmpTable = sa.Table(tmpTableName, tmpMeta,
                         sa.Column('firm_id', sa.INTEGER, primary_key=True),
                         prefixes=['TEMPORARY'])    
    businessTable = sa.Table(BUSINESS_TABLE, metadata, autoload_with=engine)
    addressTable = sa.Table(ADDRESS_TABLE, metadata, autoload_with=engine)
    nameTable = sa.Table(NAME_TABLE, metadata, autoload_with=engine)
    emailTable = sa.Table(EMAIL_TABLE, metadata, autoload_with=engine)
    phoneTable = sa.Table(PHONE_TABLE, metadata, autoload_with=engine)
    urlTable = sa.Table(URL_TABLE, metadata, autoload_with=engine)
    processedTable = sa.Table(PROCESSED_TABLE, metadata, autoload_with=engine)
            
    # Timestamp from 1 month ago, used in the pull query
    aMonthAgo = datetime.now() - relativedelta(month=1)
    
    # Query selecting firm_ids
    subq1 = sa.select(1).where(businessTable.c.firm_id == emailTable.c.firm_id).exists()  # Firms with emails
    subq2 = sa.select(1).where(businessTable.c.firm_id == urlTable.c.firm_id).exists()  # Firms with URLs
    
    # Adjust the query to select firms that do not have emails but have URLs
    qry = sa.select(businessTable.c.firm_id).filter(
        ~subq1).filter(  # Ensure the firm does not have an email
        subq2).filter(  # Ensure the firm has a URL
        businessTable.c.active).filter(
        businessTable.c.outofbusiness_status.is_(None)).filter(
        businessTable.c.createdon < aMonthAgo).order_by(
        businessTable.c.createdon.desc()).limit(batchSize)

    with engine.connect() as con:
        
        # Insert previous query into a temp table
        tmpTable.create(con)
        ins = sa.insert(tmpTable).from_select([businessTable.c.firm_id], qry)
        con.execute(ins)

        # Iterate through data tables
        dataTables = [businessTable, addressTable, nameTable, emailTable, phoneTable, urlTable]
        dataFrames = {}
        for dt in dataTables:
            
            # Join each table to the temp table
            dtJoin = sa.join(dt, tmpTable, dt.c.firm_id == tmpTable.c.firm_id)
            slct = sa.select(dt).select_from(dtJoin)
            
            # Then write the result to a dataframe
            dataFrames[dt.name] = pd.read_sql(slct, con)
    
    return dataFrames

def getBusWoutPhone(engine, metadata, scriptId, batchSize=BATCH_SIZE):
    """
    Returns a dict of dataframes for the next BATCH_SIZE number of firm_ids to
    be processed. These dataframes will all share the same firm_ids.
    
    This version retrieves firms that do not have phone but have URLs.
    
    engine: a sqlalchemy engine object
    metadata: a sqlalchemy Metadata object
    scriptId: this script's pkey
    batchSize: number of firm_ids to pull
    """
    
    # Create sqlalchemy table objects
    tmpTableName = 'mnsu_firm_pull_{}'.format(date.today().strftime('%Y%m%d'))
    tmpMeta = sa.schema.MetaData()
    tmpTable = sa.Table(tmpTableName, tmpMeta,
                         sa.Column('firm_id', sa.INTEGER, primary_key=True),
                         prefixes=['TEMPORARY'])    
    businessTable = sa.Table(BUSINESS_TABLE, metadata, autoload_with=engine)
    addressTable = sa.Table(ADDRESS_TABLE, metadata, autoload_with=engine)
    nameTable = sa.Table(NAME_TABLE, metadata, autoload_with=engine)
    emailTable = sa.Table(EMAIL_TABLE, metadata, autoload_with=engine)
    phoneTable = sa.Table(PHONE_TABLE, metadata, autoload_with=engine)
    urlTable = sa.Table(URL_TABLE, metadata, autoload_with=engine)
    processedTable = sa.Table(PROCESSED_TABLE, metadata, autoload_with=engine)
            
    # Timestamp from 1 month ago, used in the pull query
    aMonthAgo = datetime.now() - relativedelta(month=1)
    
    # Query selecting firm_ids
    subq1 = sa.select(1).where(businessTable.c.firm_id == phoneTable.c.firm_id).exists()  # Firms with phone numbers
    subq2 = sa.select(1).where(businessTable.c.firm_id == urlTable.c.firm_id).exists()  # Firms with URLs
    
    # Adjust the query to select firms that do not have phone numbers but have URLs
    qry = sa.select(businessTable.c.firm_id).filter(
        ~subq1).filter(  # Ensure the firm does not have an phone number
        subq2).filter(  # Ensure the firm has a URL
        businessTable.c.active).filter(
        businessTable.c.outofbusiness_status.is_(None)).filter(
        businessTable.c.createdon < aMonthAgo).order_by(
        businessTable.c.createdon.desc()).limit(batchSize)

    with engine.connect() as con:
        
        # Insert previous query into a temp table
        tmpTable.create(con)
        ins = sa.insert(tmpTable).from_select([businessTable.c.firm_id], qry)
        con.execute(ins)

        # Iterate through data tables
        dataTables = [businessTable, addressTable, nameTable, emailTable, phoneTable, urlTable]
        dataFrames = {}
        for dt in dataTables:
            
            # Join each table to the temp table
            dtJoin = sa.join(dt, tmpTable, dt.c.firm_id == tmpTable.c.firm_id)
            slct = sa.select(dt).select_from(dtJoin)
            
            # Then write the result to a dataframe
            dataFrames[dt.name] = pd.read_sql(slct, con)
    
    return dataFrames


def getBusWoutAddress(engine, metadata, scriptId, batchSize=BATCH_SIZE):
    """
    Returns a dict of dataframes for the next BATCH_SIZE number of firm_ids to
    be processed. These dataframes will all share the same firm_ids.
    
    This version retrieves firms that do not have addresses but have URLs.
    
    engine: a sqlalchemy engine object
    metadata: a sqlalchemy Metadata object
    scriptId: this script's pkey
    batchSize: number of firm_ids to pull
    """
    
    # Create sqlalchemy table objects
    tmpTableName = 'mnsu_firm_pull_{}'.format(date.today().strftime('%Y%m%d'))
    tmpMeta = sa.schema.MetaData()
    tmpTable = sa.Table(tmpTableName, tmpMeta,
                         sa.Column('firm_id', sa.INTEGER, primary_key=True),
                         prefixes=['TEMPORARY'])    
    businessTable = sa.Table(BUSINESS_TABLE, metadata, autoload_with=engine)
    addressTable = sa.Table(ADDRESS_TABLE, metadata, autoload_with=engine)
    nameTable = sa.Table(NAME_TABLE, metadata, autoload_with=engine)
    emailTable = sa.Table(EMAIL_TABLE, metadata, autoload_with=engine)
    phoneTable = sa.Table(PHONE_TABLE, metadata, autoload_with=engine)
    urlTable = sa.Table(URL_TABLE, metadata, autoload_with=engine)
    processedTable = sa.Table(PROCESSED_TABLE, metadata, autoload_with=engine)
            
    # Timestamp from 1 month ago, used in the pull query
    aMonthAgo = datetime.now() - relativedelta(month=1)
    
    # Query selecting firm_ids
    subq1 = sa.select(1).where(businessTable.c.firm_id == addressTable.c.firm_id).exists()  # Firms with phone numbers
    subq2 = sa.select(1).where(businessTable.c.firm_id == urlTable.c.firm_id).exists()  # Firms with URLs
    
    # Adjust the query to select firms that do not have phone numbers but have URLs
    qry = sa.select(businessTable.c.firm_id).filter(
        ~subq1).filter(  # Ensure the firm does not have an phone number
        subq2).filter(  # Ensure the firm has a URL
        businessTable.c.active).filter(
        businessTable.c.outofbusiness_status.is_(None)).filter(
        businessTable.c.createdon < aMonthAgo).order_by(
        businessTable.c.createdon.desc()).limit(batchSize)

    with engine.connect() as con:
        
        # Insert previous query into a temp table
        tmpTable.create(con)
        ins = sa.insert(tmpTable).from_select([businessTable.c.firm_id], qry)
        con.execute(ins)

        # Iterate through data tables
        dataTables = [businessTable, addressTable, nameTable, emailTable, phoneTable, urlTable]
        dataFrames = {}
        for dt in dataTables:
            
            # Join each table to the temp table
            dtJoin = sa.join(dt, tmpTable, dt.c.firm_id == tmpTable.c.firm_id)
            slct = sa.select(dt).select_from(dtJoin)
            
            # Then write the result to a dataframe
            dataFrames[dt.name] = pd.read_sql(slct, con)
    
    return dataFrames
    