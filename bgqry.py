from sys import exit
import logging

# BigQuery codes
from google.cloud import bigquery

logger = logging.getLogger(__name__)
SQL = {}


def get_data(configs, qry_name):
    '''
    Will do our connection and then run our query
    '''
    rv = []
    if qry_name in SQL:
        qry = SQL[qry_name]
    else:
        logging.error('No query called {}'.format(qry_name))
        return None

    try:
        cl = bigquery.Client()
        logger.info('starting bigquery')
        logger.debug(qry)

        rows = cl.query(qry)

        # Iterate
        for row in rows:
            rdate = "{}-{}-{}".format(row.year, row.mo, row.da)
            r = {k: v for k, v in row.items()}
            r['date'] = rdate
            rv.append(r)
    except Exception as e:
        print(e)
        logger.error(e)
        return None

    return rv


SQL['etl'] = '''
    SELECT
        max, (max-32)*5/9 celsius, year, mo, da,
        state, stn, name
    FROM (
        SELECT
            max,
            year, mo, da,
            state, stn, name,
            ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) rn
        FROM
            `bigquery-public-data.noaa_gsod.gsod199*` a
        JOIN
            `bigquery-public-data.noaa_gsod.stations` b
        ON
            a.stn=b.usaf
        AND a.wban=b.wban
        WHERE
            state IS NOT NULL
            AND max<1000
            AND country='US'
        )
    WHERE rn=1
    ORDER BY max DESC
    '''
# Station names query
SQL['stn_names'] = '''
    SELECT
        max, (max-32)*5/9 celsius,
        mo, da,
        state, stn, name
    FROM (
        SELECT
            max,
            mo, da,
            state, stn, name,
            ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) rn
        FROM
            `bigquery-public-data.noaa_gsod.gsod{}` a
        JOIN
            `bigquery-public-data.noaa_gsod.stations` b
        ON
            a.stn=b.usaf
        AND a.wban=b.wban
        WHERE
            state IS NOT NULL
        AND max<1000
        AND country='US'
    )
    WHERE
        rn=1
    ORDER BY
        max DESC
    '''

# Warmest day query
SQL['warmest_day'] = '''
    SELECT
        max, (max-32)*5/9 celsius,
        year, mo, da,
        state, stn, name
    FROM (
        SELECT
            max,
            year, mo, da,
            state, stn, name,
            ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) rn
        FROM (
            SELECT
                max,
                year, mo, da,
                stn, wban
            FROM
                TABLE_QUERY(`bigquery-public-data.noaa_gsod`, 'table_id CONTAINS "gsod"')
        ) a
        JOIN
            `bigquery-public-data.noaa_gsod.stations` b
        ON
            a.stn=b.usaf
        AND a.wban=b.wban
        WHERE
            state IS NOT NULL
        AND max<1000
        AND country='US'
    )
    WHERE
        rn=1
    ORDER BY
    year DESC
'''

# Most snow query
SQL['most_snow'] = '''
    SELECT
        c as days, name, country
    FROM (
        SELECT stn, wban, COUNT(*) c
        FROM `bigquery-public-data.noaa_gsod.gsod2015`
        WHERE snow_ice_pellets='1'
        GROUP BY 1, 2
        ORDER BY c DESC
        LIMIT 20
    ) a
    LEFT JOIN
        `bigquery-public-data.noaa_gsod.stations` b
    ON
        a.stn=b.usaf
    AND a.wban=b.wban
    WHERE
        name IS NOT NULL
    '''

if __name__ == '__main__':
    print("Should be used as a module")
