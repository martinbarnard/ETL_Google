#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# From the API ref docs:
# https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python

import json
import sys
import os
import logging as log
import configparser

# Clint parser
from clint.arguments import Args
from clint.textui import puts, colored, indent


sys.path.insert(0, os.path.abspath('..'))


try:
    import sql
    import bgqry
except ImportError as e:
    from ETL_Google.src import sql
    from ETL_Google.src import bgqry

cfg_filename = 'private/config.ini'

# Our config parser object
cfgparser = configparser.ConfigParser()

def parse_cmdline():
    '''
    Will parse our commandline flags and set up our dictionary
    '''
    cfgfile = 'private/config.ini'

    # This was sane defaults, but now I don't want it
    configs = {
        'bigquery': {
            'dump_file': 'dump/output.json',
        },
        'cloudsql': {
            'db_name': 'noaa_agg',
            'username': 'root',
            'password': '',
            'proxy_port': '3306',
        },
        'logging': {
            'logfile': 'log/ELT.log',
            'loglevel': '10',
        },
    }

    args = Args()
    gargs = args.grouped
    if args.contains('-h'):
        puts(colored.green('Usage {} <FLAGS>'.format(sys.argv[0])))
        with indent(4):
            puts('-h: This help')
            puts('-c <cfg>: location of config file')
            puts('-x <port>: Cloud SQL proxy port')
            puts('-d <db_name>: name of Cloud SQL database')
            puts('-u <db_uname>: name of Cloud SQL user')
            puts('-p <db_pwd>: Cloud SQL password')
            puts('-l <logfile>: Location of logfile')
            puts('-j <jsonfile>: Location of dump file')

        sys.exit(1)

    # Bit hacky, but whatever
    if args.contains('-c'):
        cfgfile = gargs['-c'][0]

    if args.contains('-x'):
        configs['cloudsql']['proxy_port'] = gargs['-x'][0]
        puts(colored.green('setting proxy port'))

    if args.contains('-d'):
        configs['cloudsql']['db_name'] = gargs['-d'][0]
        puts(colored.green('setting db name'))

    if args.contains('-u'):
        configs['cloudsql']['username'] = gargs['-u'][0]
        puts(colored.green('setting db username'))

    if args.contains('-p'):
        configs['cloudsql']['password'] = gargs['-p'][0]
        puts(colored.green('setting db password'))

    if args.contains('-l'):
        configs['logging']['logfile'] = gargs['-l'][0]
        puts(colored.green('setting logfile location'))

    if args.contains('-j'):
        configs['bigquery']['dump_file'] = gargs['-j'][0]
        puts(colored.green('setting json dumpfile location'))

    log.info('config file is {}'.format(cfgfile))
    return cfgfile, configs


def get_configs(cfgparser):
    '''
    Returns a dict of config sections: items
    '''

    # Parse our arguments and populate our dictionary
    # Then, we will use that to grab our cfgfile and try to open that for reading
    cfgfile, configs = parse_cmdline()

    log.info('trying to open {}'.format(cfgfile))

    try:
        cfgparser.read(cfgfile)
    except Exception as e:
        log.error('error parsing {}'.format(cfgfile))
        log.error(e)
        puts(colored.red('Unable to parse config at ' + str(cfgfile)))
        raise e

    log.info('reading', cfgfile)

    # Iterate through our sections and populate our configs dict
    for s in cfgparser.sections():
        # passing in the 3rd value ensures that extra args already in the config aren't deleted
        if s in configs:
            configs[s] = _conf_sec_map(cfgparser, s, configs[s])
        else:
            configs[s] = _conf_sec_map(cfgparser, s)

    return configs


def _conf_sec_map(cfg, section, rv={}):
    '''
    Just pulls all the config items from a section as described in the tutorial
    '''
    options = cfg.options(section)
    for option in options:
        rv[option] = cfg.get(section, option)
    return rv



def main():
    '''
    Assumes our creds are stored somewhere in ENV variable, as mentioned in Google Docs!!!
    '''
    configs = get_configs(cfgparser)

    # Set up our logging
    # Note - loglevel 10 is DEBUG
    logcfg = configs['logging']
    log.basicConfig(
        filename=logcfg['logfile'],
        level=int(logcfg['loglevel']),
        format='%(asctime)s;%(levelname)s;%(message)s',
    )

    log.info('Started')

    connection = sql.mysql_connect(configs['cloudsql'])

    if not connection:
        log.error('Unable to connect to CloudSQL')
        sys.exit(1)

    # This assumes we are using the right db!!!
    log.info('dropping old tables')
    try:
        sql.drop_tables(connection)
    except ReferenceError as e:
        print('weak bastard')
        cursor = sql.mysql_connect(configs['cloudsql'])
        sql.drop_tables(cursor)

    # Recreate them now
    log.info('recreating new tables')
    sql.create_tables(connection, configs['cloudsql']['db_name'])

    # TODO: Test for tables & create if not exists
    bgqry_params = configs['bigquery']

    # This is the output from our bigquery
    rv = bgqry.get_data(bgqry_params, 'etl')

    # Print out our configs
    # TODO: cmd-line args to pass filename
    output_file = os.path.join(os.path.abspath('.'),bgqry_params['dump_file'])
    try:
        print('dumping json')
        f = open(output_file, 'w')
        f.write(json.dumps(rv))
        f.close()
    except Exception as e:
        print('unable to dump')
        log.error('unable to dump data')
        log.debug(e)

    # Note: To read the json:
    # json_data = json.loads(open('output.json','r').read())

    # Now we recreate our tables
    # Should also point to json and let people look at it, but case-study time limits...
    # Now we need to amend the path, since it's most likely relative to app call
    sql.upload_data(connection, output_file)

    log.info('Finished')


if __name__ == '__main__' and __package__ is None:
    __package__ = 'ETL_Google'
    main()
