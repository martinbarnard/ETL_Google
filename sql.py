from sys import exit
import mysql.connector
from mysql.connector import errorcode
import logging

# MySQL code 

TABLES  = {}
logger  = logging.getLogger(__name__)

TABLES['etl_agg'] = '''
        CREATE TABLE etl_agg (
            max_temp DECIMAL NOT NULL, 
            celsius DOUBLE, 
            date DATE, 
            year INT NOT NULL, 
            month INT NOT NULL, 
            day INT NOT NULL, 
            state INT NOT NULL, 
            stn VARCHAR(255), 
            stn_name VARCHAR(255)
        ) ENGINE=InnoDB ;
    '''

def upload_data(json_data):
    '''
    Will take the json_data and upload it to the table
    '''
        
    return None

def mysql_connect(cfg):
    '''
    Take the mysql connection info and attempt to return a cursor object
    Assumes that configs is set
    '''
    try:
        logger.info('Connecting to MySQL db')
        port = int(cfg['proxy_port'])
        connection = mysql.connector.connect(user=cfg['username'], password=cfg['password'], port=port)
        cursor = connection.cursor()
    except Exception as e:
        logger.error(e)
        return None

    return cursor

def create_db(cursor, DB_NAME):
    '''
    MySQL 
    '''
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        logger.error('Failed to create database {}'.format(DB_NAME))
        logger.debug(err.msg)
        return False
    return True



def create_tables(connection, DB_NAME):
    '''
    MYSQL. TODO: move out to MySQL only module
    Create our table - single table at the moment
    '''
    try:
        connection.database = DB_NAME  
    except mysql.connector.Error as err:
        logger.error('Unable to connect to database {}'.format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.debug(err.msg)

            create_db(cursor, DB_NAME)
            connection.database = DB_NAME  
        else:
            logger.error(err.msg)
            return False

    for k,v in TABLES.iteritems():
        try:
            logger.info('Creating table {}'.format(k))
            connector.execute(v)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logger.info('table {} exists'.format(k))
            else:
                logger.error(err.msg)
        else:
            logger.info('{} created'.format(k))
    return True



if __name__=='__main__':
    print("Should be used as a module")
