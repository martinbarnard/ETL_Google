import logging
import os
import sys

# BigQuery codes
from google.cloud import bigquery
from clint.textui import puts, colored

from ETL_Google.src import sql

sys.path.insert(0, os.path.abspath('..'))

logger = logging.getLogger(__name__)
SQL = {}


def get_data(qry_name, do_insert = False, connection=None):
    '''
    Will do our connection and then run our query
    '''
    puts(colored.green('getting data'))
    if qry_name in SQL:
        qry = SQL[qry_name]
    else:
        puts(colored.red('No query with that name'))
        logging.error('No query called {}'.format(qry_name))
        return False

    try:
        cl = bigquery.Client()
        logger.debug(qry)
        rows = cl.query(qry)
    except Exception as e:
        logger.error('Unable to query bigquery')
        raise e
    except Exception as e:
        puts(colored.red('error trying to connect'))
        raise e

    if do_insert:
        # create our cursor
        cursor = connection.cursor()
        cursor.execute('START TRANSACTION')
    # Iterate
    iterator = 0
    for row in rows:
        r = {k: v for k, v in row.items()}
        # we need to insert our row anyhow
        if do_insert:
            sql.insert_row(cursor, r)
            if iterator % 10000 == 0:
                puts(colored.green('iterating {} with commit'.format(iterator)))
                cursor.execute('COMMIT')
                cursor.execute('START TRANSACTION')
        else:
            if iterator % 10000 == 0:
                puts(colored.green('iterating {} without insertion'.format(iterator)))
        iterator += 1

    if do_insert:
        cursor.execute('COMMIT')

    return True


SQL = {
    'etl_ex': '''
    SELECT
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

if __name__ == '__main__':
    logger.error('module called as application')
    print("Should be used as a module")
