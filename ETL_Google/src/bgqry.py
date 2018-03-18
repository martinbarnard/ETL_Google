import logging
import os
import sys

# BigQuery codes
from google.cloud import bigquery

sys.path.insert(0, os.path.abspath('..'))

logger = logging.getLogger(__name__)


class etl():
    def __init__(self, qry=None):
        self.SQL = {
            'etl_ex': '''SELECT
                max((max-32)*5/9) max,
                min((min-32)*5/9) min,
                DATE(CAST(year as int64),CAST(mo as int64),CAST(da as int64)) date,
                state
            FROM  (
                `bigquery-public-data.noaa_gsod.gsod199*` a
            JOIN
                `bigquery-public-data.noaa_gsod.stations` b
            ON
                a.stn = b.usaf
                and a.wban = b.wban
            )
            GROUP BY
                year,
                da,
                mo,
                state,
                country
            HAVING
            state is not null
            and max < 1000
            and country = 'US'
            ORDER BY DATE
            '''
        }
        self.results = None
        self.rowset = []

    def connect(self):
        '''
        Connect to our Google Client
        '''
        self.GClient = bigquery.Client()
        return True

    def query(self, qry=None):
        '''
        Will hit the query
        '''
        if not qry:
            qry = 'etl_ex'

        if qry in self.SQL:
            qry = self.SQL[qry]
            # This is a bit weird, but it should be good
            self.results = self.GClient.query(qry)
            return True
        else:
            return False

    def create_rows(self):
        '''
        This would only work if self.results != None
        '''
        self.rowset = []
        if self.results:
            for row in self.results:
                r = {k: v for k, v in row.items()}
                self.rowset.append(r)

        return self.rowset
