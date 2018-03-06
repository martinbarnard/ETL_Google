import logging

# BigQuery codes
from google.cloud import bigquery

logger = logging.getLogger(__name__)
SQL = {}


def get_data(configs, qry_name):
    '''
    Will do our connection and then run our query
    '''
    logger.info('starting bigquery')
    rv = []
    if qry_name in SQL:
        qry = SQL[qry_name]
    else:
        logging.error('No query called {}'.format(qry_name))
        return None

    try:
        cl = bigquery.Client()
        logger.debug(qry)
        rows = cl.query(qry)

    except Exception as e:
        logger.error('Unable to query bigquery')
        logger.debug(e)
        return None

    # Iterate
    for row in rows:
        rdate = "{}-{}-{}".format(row.year, row.mo, row.da)
        r = {k: v for k, v in row.items()}
        r['date'] = rdate
        rv.append(r)

    return rv


SQL = {
    'etl': '''
        SELECT
            max,
            (max-32)*5/9 max_celsius,
            min,
            (min-32)*5/9 min_celsius,
            year,
            mo,
            da,
            state,
            stn,
            name
        FROM (
            SELECT
                max,
                min,
                year,
                mo,
                da,
                state,
                stn,
                name,
                ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) rn
            FROM
                `bigquery-public-data.noaa_gsod.gsod199*` a
            JOIN
                `bigquery-public-data.noaa_gsod.stations` b
            ON
                a.stn=b.usaf
            AND a.wban=b.wban
            HAVING
                state IS NOT NULL
                AND max<1000
                AND country='US'
            )
        ''',
    'etl_ex': '''
    SELECT DISTINCT
    max, min, year, mo, da, state, stn
    FROM (
      SELECT
        max, min, year, mo, da, state, stn,
        ROW_NUMBER() OVER(PARTITION BY year ORDER BY year, mo, da DESC) rn
      FROM
        `bigquery-public-data.noaa_gsod.gsod199*` a
      JOIN
        `bigquery-public-data.noaa_gsod.stations` b
      ON
        a.stn = b.usaf
        AND a.wban = b.wban
      GROUP BY
        year,
        da,
        mo,
        max,
        min,
        state,
        stn,
        country
      HAVING
        state IS NOT NULL
        AND max < 1000
        AND country='US'
      LIMIT
        1000 )
    '''
}

if __name__ == '__main__':
    logger.error('module called as application')
    print("Should be used as a module")
