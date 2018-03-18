import logging
import sys
import os
import mysql.connector

sys.path.insert(0, os.path.abspath('..'))

logger = logging.getLogger(__name__)


class cloudsql():
    def __init__(self, config=None):
        '''
        Need to ensure that config and DBNAME are correctly set
        '''
        self.sql = {
            'commit': 'COMMIT',
            'start': 'START TRANSACTION',
            'insert_row': '''
                INSERT INTO etl_agg
                    (max_celsius, min_celsius, date, state )
                VALUES
                    (%s, %s, %s, %s)
            ''',
            'create_db': "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'",
            'rollback': "ROLLBACK",
            'create': '''CREATE TABLE etl_agg (
                    max_celsius DOUBLE,
                    min_celsius DOUBLE,
                    date DATE,
                    state VARCHAR(10)
                ) ENGINE=InnoDB ;''',
            'drop': '''DROP TABLE IF EXISTS etl_agg'''
        }
        self.cursor = None
        self.config = config
        self.transaction = False
        self.db = config['db_name']

    def drop_table(self):
        '''
        Assumes we don't care about our data & will just drop the tables
        '''
        cursor = self.connection.cursor()
        cursor.execute(self.sql['drop'])
        return True

    def create_db(self):
        '''
        MySQL
        '''
        DB_NAME = self.db
        logger.info('Attempting to create database: {}'.format(DB_NAME))
        cursor = self.connection.cursor()
        cursor.execute(self.sql['create_db'].format(DB_NAME))
        return True

    def connect(self):
        '''
        Take the mysql connection info and attempt to return a cursor object
        Assumes that configs is set
        '''
        dbname = 'noaa_agg'

        cfg = self.config

        if cfg is None:
            return False

        try:
            self.port = int(cfg['port'])
            self.connection = mysql.connector.connect(
                host=cfg['host'],
                user=cfg['username'],
                password=cfg['password'],
                port=self.port,
                database=dbname
            )
        except Exception as e:
            logger.error(e)
            return False

        return True

    def insert_rows(self, rows):
        '''
        :param: connection object,
                row list
        :return:
        '''
        cursor = self.connection.cursor()
        cursor.execute(self.sql['start'])
        for row in rows:
            our_list = (
                row['max'],
                row['min'],
                row['date'],
                row['state'],
            )
            cursor.execute(self.sql['insert_row'], our_list)
        cursor.execute(self.sql['commit'])
        return True

    def create_table(self):
        '''
        '''
        logger.info('connecting to {}'.format(self.db))
        cursor = self.connection.cursor()
        cursor.database = self.db
        cursor.execut(self.sql['create'])
        return True


if __name__ == '__main__':
    print("Should be used as a module")
