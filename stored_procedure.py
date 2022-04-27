import pandas as pd
import sqlalchemy
import logging

from yaml import load, Loader

# Open Config File
with open('config.yaml', 'r') as config_file:
    config = load(config_file, Loader=Loader)
logging.basicConfig(filename='db.log', level=logging.INFO)
logger = logging.getLogger('AW_export.log')

############# Kolkata Connection and Operations Methods ###############

def conn_kolkata():
    '''
    Connect to kolkata server
    :return obj connection:
    '''
    logger.info('connecting to database')
    # Try to connect to server
    try:
        cnxn = sqlalchemy.engine.url.URL.create(
            'mssql+pyodbc',
            username=config['credentials']['username'],
            password=config['credentials']['password'],
            host=config['credentials']['host'],
            port=config['credentials']['port'],
            database=config['credentials']['database'],
            query=dict(driver='SQL Server Native Client 11.0'))

        engine = sqlalchemy.create_engine(cnxn)
        connection = engine.connect()
        logger.info('connection successful')

    # Log errors
    except sqlalchemy.exc.InterfaceError as e:
        logger.error(f'{e} Failed to connect: Bad connection parameters')
    except ConnectionError as e:
        logger.error(f'{e} Failed to connect to server')
    return connection


def query_kolkata(query):
    '''
    Query kolkata server
    :param Connection cnxn:
    :param str query:
    :return dataframe df:
    '''
    logger.info('Querying database')
    # Try querying database
    cnxn = conn_kolkata()
    try:
        df = pd.read_sql_query(
            query, cnxn)    
        logger.info('Query successful')
        df.sort_values(by=['ID'], inplace=True)
    except Exception as e:
        logger.error(f'{e}')
    cnxn.close()
    return df


