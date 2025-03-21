import pyodbc, os, sqlalchemy, csv
import traceback as tb


staff = {'ELI':{'f':'eli','l':'johnson'},
         'MATT':{'f':'matt','l':'scandale'},
         'RUBENS':{'f':'rubens','l':'pessanha'},
         'RYAN':{'f':'ryan','l':'hessling'},
         # spring 2025 MNSU team
         'MIKE':{'f':'mikiyas','l':'yoseph'},
         'BOUBA':{'f':'boubacary','l':'bocoum'},
         'ABDI':{'f':'abdilahi','l':'jama'},
         'AMANUEL':{'f':'amanuel','l':'fissiha'}
         }

# These must be all caps
dbList = ['CORE','CDW','DATA_APPS','RESEARCH','SCAMTRACKER','WEBAPP','BLUE','MNSU','BLUE_MN','AUDIT']


def getDriver(db):
    """Helper function, used for pyodbc connections, or when sqlalchemy depends
    on a pyodbc connection"""
    
    if db in ('DATA_APPS','WEBAPP','MNSU','AUDIT'):
        driver = 'PostgreSQL Unicode(x64)'
    elif db in ('CORE','CDW','RESEARCH','SCAMTRACKER','BLUE','BLUE_MN'):
        driver = 'ODBC Driver 18 for SQL Server'
    
    if driver not in pyodbc.drivers(): 
        print('SQL Driver not found')
        raise ConnectionError
    
    return driver

def getUsername(name,db,instance='REPORT'):
    if db in ('CORE','CDW','RESEARCH'):
        if instance == 'DEV':
            u = staff[name]['f'][0].upper() + staff[name]['l'].capitalize()
        elif instance in ('REPORT','STAGE'):
            u = staff[name]['f'][0] + staff[name]['l']
        elif instance == 'PROD':
            u = staff[name]['f'].capitalize()
            # not sure if this is standard
    elif db in ('DATA_APPS','MNSU','AUDIT'):
        u = staff[name]['f'][0] + staff[name]['l']
    elif db == 'SCAMTRACKER':
        u = staff[name]['f'][0].upper() + staff[name]['l'].capitalize()
    elif db == 'WEBAPP':
        u = staff[name]['f'][0] + staff[name]['l']
    elif db in ('BLUE','BLUE_MN'):
        u = 'iabbb_'+staff[name]['f'][0] + staff[name]['l']
    
    return u
        

def getPort(db):
    if db in ('CORE','CDW','RESEARCH','SCAMTRACKER'):
        p = '1433'
    elif db in ('DATA_APPS','WEBAPP','MNSU','AUDIT'):
        p = '5432'
    elif db in ('BLUE','BLUE_MN'):
        p = '0'
            
    return p

def getDBName(db):
    if db in ('CORE','RESEARCH'):
        db = db.capitalize()
    elif db == 'CDW':
        pass
    elif db == 'DATA_APPS':
        db = db.lower()
    elif db == 'SCAMTRACKER':
        db = 'BlueScam'
    elif db == 'WEBAPP':
        db = 'core'
    elif db == 'BLUE':
        db = 'OurBBB'
    elif db == 'BLUE_MN':
        db = 'OurBBBMinnesota'
    elif db == 'MNSU':
        db = 'mnsu_sandbox'
    elif db == 'AUDIT':
        db = 'postgres'
    
    return db
        

def getDialect(db):
    """Helper function, used for sqlalchemy connections only"""
    if db in ('DATA_APPS','WEBAPP','MNSU','AUDIT'):
        dialect = 'postgresql'
    else:
        dialect = 'mssql+pyodbc'
#        dialect = 'ODBC Driver 18 for SQL Server'
    return dialect

    

def connect(db='RESEARCH',instance='REPORT',user='MATT',engine='pyodbc'):
    """ Return a connection or engine object to various IABBB or IB databases, capable of executing SQL
    Prerequisite:
    The function gets password and server strings from environment variables, so these need
    to be set first, ideally in a persistent way specific to your package manager (e.g. pip
    or conda). 

    Server variable names must be of the form SERVER_{db}_{instance}, where
    "db" is one of the values in connect_iabbb.dbList.  "instance" is e.g. "PROD", "DEV", 
    "REPORT", "STAGE", or "TEST" (must be in all caps).   

    Password variable names must be of the form PASSWORD_{db}_{instance}_{user}, similar to
    above, but also "user" is the first name, in all caps, of the IABBB staff member whose 
    credentials are being used.
    
    Keyword arguments:
    db -- A nickname for the database. Allowed values are in connect_iabbb.dbList
    instance -- The database instance: prod, dev, report, stage, or test
    user -- The first name of the IABBB staff member whose username is being used to connect
    engine -- The underlying python connection library.  Currently pyodbc and sqlalchemy are
                supported.  If pyodbc is chosen, the function will return a pyodbc connection
                object.  If sqlalchemy is chosen, it returns a sqlalchemy engine object.
    Note: all keyword arguments are case-insensitive.
    """
    db = db.upper()
    assert db in dbList
    instance = instance.upper()
    user = user.upper()
    engine = engine.lower()
    errors = False
        
        
    server = 'CDW' if db in ('CORE','CDW','RESEARCH') else db   

    password_variable = '_'.join(['PASSWORD',server,instance,user])
    if not (password := os.environ.get(password_variable)):
        print(f'Environment variable {password_variable} not set')
        errors = True

    server_variable = '_'.join(['SERVER',server,instance])
    if not (server := os.environ.get(server_variable)):
        print(f'Environment variable {server_variable} not set')
        errors = True
        
    if errors: return False
    

    if engine=='pyodbc':
        conn_str = "DRIVER={sql_driver};SERVER={svr};DATABASE={database};UID={username};PWD={pw};TrustServerCertificate=yes"
        conn_str = conn_str.format(sql_driver = getDriver(db),svr = server,port = getPort(db),\
                               database = getDBName(db),username = getUsername(user,db,instance),\
                                   pw = password)
    
        conn = pyodbc.connect(conn_str)
        return conn
    elif engine=='sqlalchemy':
        dialect = getDialect(db)
        if dialect == 'postgresql':
            url_object = sqlalchemy.URL.create(
                dialect,
                username=getUsername(user, db, instance),
                password=password,
                host=server,
                database=getDBName(db)
                )
        elif dialect == 'mssql+pyodbc':
            url_object = sqlalchemy.URL.create(
                dialect,
                username=getUsername(user, db, instance),
                password=password,
                host=server,
                database=getDBName(db),
                query={"driver":getDriver(db),
                       "TrustServerCertificate": "yes",}
                )
        return sqlalchemy.create_engine(url_object)

