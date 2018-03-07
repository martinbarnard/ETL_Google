import logging

# BigQuery codes
from google.cloud import bigquery
from clint.textui import puts, colored

logger = logging.getLogger(__name__)
SQL = {}


def get_data(configs, qry_name):
    '''
    Will do our connection and then run our query
    '''
    puts(colored.green('getting data'))
    rv = []
    if qry_name in SQL:
        qry = SQL[qry_name]
    else:
        puts(colored.red('No query with that name'))
        logging.error('No query called {}'.format(qry_name))
        return None

    try:
        cl = bigquery.Client()
        logger.debug(qry)
        rows = cl.query(qry)
    except Exception as e:
        raise e
        logger.error('Unable to query bigquery')
        return None
    except Exception as e:
        puts(colored.red('error trying to connect'))
        raise e


    # Iterate
    iterator = 0
    for row in rows:
        if iterator % 100 == 0:
            puts(colored.green('iterating {}'.format(iterator)))
        rdate = "{}-{}-{}".format(row.year, row.mo, row.da)
        r = {k: v for k, v in row.items()}
        r['date'] = rdate
        rv.append(r)
        iterator += 1

    return rv


SQL = {
    'etl_ex': '''
    SELECT distinct
      max,
        (max-32)*5/9 max_celsius,
      min,
        (min-32)*5/9 min_celsius,
      year,
      mo,
      da,
      state
     FROM (
        `bigquery-public-data.noaa_gsod.gsod199*` a
      JOIN
        `bigquery-public-data.noaa_gsod.stations` b
      ON
        a.stn = b.usaf
        AND a.wban = b.wban
        )
      GROUP BY
        year,
        da,
        mo,
        max,
        min,
        state,
        country
      HAVING
        state IS NOT NULL
        AND max < 1000
        AND country='US'
    '''
}

if __name__ == '__main__':
    logger.error('module called as application')
    print("Should be used as a module")
