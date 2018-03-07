import mysql.connector
from mysql.connector import errorcode
import logging
import json
from datetime import datetime
from clint.textui import puts, colored

# MySQL code
# Used for connecting to Google cloud SQL (i.e. google hosted MySQL instance)

TABLES = {}
logger = logging.getLogger(__name__)

TABLES['etl_agg'] = '''
        CREATE TABLE etl_agg (
            max_celsius DOUBLE,
            min_celsius DOUBLE,
            date DATE,
            state VARCHAR(10),
            stn VARCHAR(255),
            stn_name VARCHAR(255)
        ) ENGINE=InnoDB ;
    '''
def insert_row(cursor, row):
    '''
    Assumes we're in a transaction. 
    :param: connection object, 
            row dictionary
    :return: 
    '''
    sql = '''
    INSERT INTO etl_agg
        (max_celsius, min_celsius, date, state, stn)
    VALUES
        (%s, %s, %s, %s, %s)
    '''
    our_list = (
        row['max_celsius'],
        row['min_celsius'],
        datetime.strptime(row['date'], '%Y-%M-%d'),
        row['state'],
        row['stn'],
    )
    cursor.execute(sql, our_list)


def upload_data(connection, json_data):
    '''
    Will take the json_data and upload it to the table
    json_data being path to json file for now
    '''
    starttran = 'START TRANSACTION'
    commit = 'COMMIT'
    rollback = 'ROLLBACK'

    sql = '''
    INSERT INTO etl_agg
        (max_celsius, min_celsius, date, state, stn, stn_name)
    VALUES
        (%s, %s, %s, %s, %s, %s)
    '''
    cursor = connection.cursor()
    jd = json.loads(open(json_data, 'r').read())

    if cursor:
        logger.info('starting sql insertion')
        print('we are inserting our data')
        try:
            cursor.execute(starttran)
            for data in jd:
                our_list = (
                    data['max_celsius'],
                    data['min_celsius'],
                    datetime.strptime(data['date'], '%Y-%M-%d'),
                    data['state'],
                    data['stn'],
                    data['name']
                )
                cursor.execute(sql, our_list)
            cursor.execute(commit)
        except Exception as e:
            cursor.execute(rollback)
            raise(e)
    return None


def mysql_connect(cfg=None):
    '''
    Take the mysql connection info and attempt to return a cursor object
    Assumes that configs is set
    '''
    dbname = 'noaa_agg'

    if cfg is None:
        return None

    try:
        logger.info('Connecting to MySQL db')
        puts(colored.blue('{}@{}:{}'.format(cfg['username'],cfg['host'],cfg['port'])))
        port = int(cfg['proxy_port'])
        connection = mysql.connector.connect(
            host=cfg['host'],
            user=cfg['username'],
            password=cfg['password'],
            port=port,
            database=dbname
        )
    except Exception as e:
        logger.error(e)
        return None


    return connection


def create_db(connection, DB_NAME):
    '''
    MySQL
    '''
    cursor = connection.cursor()
    try:
        logger.info('Attempting to create database: {}'.format(DB_NAME))
        cursor.execute(
            "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        logger.error('Failed to create database: {}'.format(DB_NAME))
        logger.debug(err.msg)
        return False
    return True


def drop_tables(connection):
    '''
    Assumes we don't care about our data & will just drop the tables
    '''
    cursor = connection.cursor()
    for t in TABLES:
        st = '''DROP TABLE IF EXISTS {}'''.format(t)
        try:
            cursor.execute(st)
        except ReferenceError as e:
            logger.debug(e)
            raise(e)

    return True


def create_tables(connection, DB_NAME):
    '''
    MYSQL. TODO: move out to MySQL only module
    Create our table - single table at the moment
    '''
    cursor = connection.cursor()
    logger.info('connecting to {}'.format(DB_NAME))
    try:
        cursor.database = DB_NAME
    except mysql.connector.Error as err:
        logger.error('Unable to connect to database {}'.format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.debug(err.msg)
            create_db(cursor, DB_NAME)
            cursor.database = DB_NAME
        else:
            logger.error(err.msg)
            return False

    for k, v in TABLES.items():
        try:
            logger.info('Creating table {}'.format(k))
            cursor.execute(v)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logger.info('table {} exists'.format(k))
            else:
                logger.error(err.msg)
        else:
            logger.info('{} created'.format(k))
    return True


if __name__ == '__main__':
    print("Should be used as a module")
