# SQL Examples


def qry_stn_names(input_yr):
    '''
    joins the gsod table for 2015 with station data to provide a station name for each record
    '''

    sql = '''
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
            `bigquery-public-data.noaa_gsod.gsod{input_yr}` a
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
    '''.format(input_yr)
    return sql

def qry_warmest_day_state(input_yr=None):
    '''
    finding the warmest day in each state since 1929
    '''

    sql = '''
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
              TABLE_QUERY(`bigquery-public-data.noaa_gsod`, 'table_id CONTAINS "gsod"')) a
          JOIN
            `bigquery-public-data.noaa_gsod.stations` b
          ON
            a.stn=b.usaf
            AND a.wban=b.wban
          WHERE
            state IS NOT NULL
            AND max<1000
            AND country='US' )
        WHERE
          rn=1
        ORDER BY
          YEAR DESC
    '''

    return sql

def qry_most_snow():
    '''
    stations that record the most days of snow precipitation.
    '''

    sql = '''
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
    return sql
