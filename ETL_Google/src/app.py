#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
    import qry
except ImportError as e:
    from ETL_Google.src import sql
    from ETL_Google.src import bgqry
    from ETL_Google.src import qry

# Our config parser object
cfgparser = configparser.ConfigParser()
def print_help():
    puts(colored.green('Usage {} <FLAGS>'.format(sys.argv[0])))
    with indent(4):
        puts('-h: This help')
        puts('-c <cfg>: location of config file')
        puts('-l <logfile>: Location of logfile')
        puts('-d Dump & recreate dataset')
        puts('-state <state>: Query by state')
        puts('-date <state>: Query by date')

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
        print_help()
        sys.exit(0)

    # Bit hacky, but whatever
    if args.contains('-c'):
        cfgfile = gargs['-c'][0]

    if args.contains('-l'):
        configs['logging']['logfile'] = gargs['-l'][0]
        puts(colored.green('setting logfile location'))

    if args.contains('-d'):
        configs['status'] = ['recreate']
        return cfgfile, configs

    if args.contains('-state'):
        configs['status'] = ['query_state', gargs['-state'][0]]
        return cfgfile, configs

    if args.contains('-date'):
        configs['status'] = ['query_date', gargs['-date'][0]]
        return cfgfile, configs

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
    except Exception as e:
        puts(colored.red('cannot parse cmdline'))
        raise e

    try:
        cfgparser.read(cfgfile)
    except Exception as e:
        log.exception(e)
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
    Just pulls all the config items from a section
    '''
    options = cfg.options(section)
    for option in options:
        rv[option] = cfg.get(section, option)
    return rv

def recreate_data():
    '''
    Drop old tables, pull in new stuff & insert as ETL operation
    '''
    return True

def main():
    '''
    Assumes our creds are stored somewhere in ENV variable, as mentioned in Google Docs!!!
    '''
    configs = get_configs(cfgparser)
    if not configs:
        puts(colored.red('Unable to load configs'))
        sys.exit(1)

    # Set up our logging
    logcfg = configs['logging']
    log.basicConfig(
        filename=os.path.abspath(logcfg['logfile']),
        level=int(logcfg['loglevel'] or 10),
        format='%(asctime)s;%(levelname)s;%(message)s',
    )
    puts('logging to {}'.format(logcfg['logfile']))

    if 'status' in configs:
        st = configs['status']
        qryobj = qry.queryObj(configs['cloudsql'])

        if st[0] == 'recreate':
            # Drop our database & pull in shiny
            # Our google query object
            GQ = bgqry.etl()
            mydb = sql.cloudsql(configs['cloudsql'])

            mydb.connect()
            GQ.connect()

            res = GQ.query()
            if res:
                cnt = 0
                rowset = []
                for row in GQ.results:
                    r = {k:v for k,v in row.items()}
                    rowset.append(r)
                    cnt += 1
                    if cnt % 1000 == 0:
                        mydb.insert_rows(rowset)
                        rowset = []
                        puts(colored.yellow('{} rows inserted...'.format(cnt)))

                puts(colored.yellow('completed with {} rows inserted'.format(cnt)))
            sys.exit(0)
        # A bit duplicate, but clean
        elif st[0] == 'query_state':
            # state query
            res = qryobj.do_qry('states',st[1] )
            if res:
                qryobj.print_results()
        elif st[0] == 'query_date':
            # date query
            res = qryobj.do_qry('dates',st[1] )
            if res:
                qryobj.print_results()
        else:
            # unknown - bail
            puts(colored.red('No idea what you want'))
            sys.exit(1)
    else:
        # unknown - bail
        print_help()

        sys.exit(0)

    log.info('Finished')


