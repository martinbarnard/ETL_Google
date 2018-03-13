#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# From the API ref docs:
# https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python

import sys
import os
import logging as log
import configparser

# Clint parser
from clint.arguments import Args
from clint.textui import puts, colored, indent


sys.path.insert(0, os.path.abspath('..'))

# TODO: refactor with setuptools
try:
    import sql
    import bgqry
except ImportError as e:
    from ETL_Google.src import sql
    from ETL_Google.src import bgqry

# Our config parser object
cfgparser = configparser.ConfigParser()

def parse_cmdline():
    '''
    Will parse our commandline flags and set up our dictionary
    :param:
    :return: configfile, config
    '''
    cfgfile = 'private/config.ini'

    # This was sane defaults, but now I don't want it
    configs = { }

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
            puts('-q <state>: Query by state')

        return None

    # Bit hacky, but whatever
    if args.contains('-c'):
        cfgfile = gargs['-c'][0]

    if args.contains('-x'):
        configs['cloudsql']['port'] = gargs['-x'][0]
        puts(colored.green('setting port port'))

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

    if args.contains('-q'):
        configs['query']['state'] = gargs['-q'][0]
        puts(colored.green('querying for {}'.format(configs['query']['state'])))

    log.info('config file is {}'.format(cfgfile))
    return cfgfile, configs


def get_configs(cfgparser):
    '''
    Returns a dict of config sections: items
    :param:  config parser object
    :return: dict of config items
    '''

    # Parse our arguments and populate our dictionary
    # Then, we will use that to grab our cfgfile and try to open that for reading
    try:
        cfgfile, configs = parse_cmdline()
    except:
        puts(colored.red('cannot parse cmdline'))
        return None

    try:
        cfgparser.read(cfgfile)
    except Exception as e:
        log.exception(e)
        puts(colored.red('Unable to parse config at ' + str(cfgfile)))
        return None

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
    if not configs:
        puts(colored.red('Unable to load configs'))
        sys.exit(1)

    if query in configs:
        # We are running a query - everything else doesn't matter
        # import our query
    else:
        # we are doing other shit
        log.info('trying to open {}'.format(cfgfile))

    # Set up our logging
    logcfg = configs['logging']
    log.basicConfig(
        filename=os.path.abspath(logcfg['logfile']),
        level=int(logcfg['loglevel'] or 10),
        format='%(asctime)s;%(levelname)s;%(message)s',
    )
    puts('logging to {}'.format(logcfg['logfile']))

    # Here we need to test config to see if we are dropping db

    connection = sql.mysql_connect(configs['cloudsql'])
    if not connection:
        puts(colored.red('unable to connect to CloudSQL'))
        log.error('Unable to connect to CloudSQL')
        sys.exit(1)

    # This assumes we are using the right db!!!
    log.info('dropping old tables')
    puts(colored.blue('dropping old table'))
    sql.drop_tables(connection)

    # Recreate them now
    log.info('recreating new tables')
    sql.create_tables(connection, configs['cloudsql']['db_name'])

    # This is the output from our bigquery
    puts(colored.blue('starting our bigquery'))
    rv = bgqry.get_data('etl_ex', do_insert=True, connection = connection)
    puts(colored.green('query over. Insertion status: {}'.format(rv)))
        
    log.info('Finished')


if __name__ == '__main__' and __package__ is None:
    __package__ = 'ETL_Google'
    main()
