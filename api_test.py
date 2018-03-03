#!/usr/bin/env python3

# From the API ref docs: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
from google.cloud import bigquery

import json,sys
import logging as log
import mysql.connector
import configparser

# Our local sql strings 
import sql, bgqry

# Our config parser object
cfgparser = configparser.ConfigParser()

configs = {
    'bigquery'  : {
        'dump_file' : 'dump/output.json',
    },
    'cloudsql'  : {
        'db_name'   : 'noaa_agg',
        'username'  : 'root',
        'password'  : '',
        'proxy_port':'3306',
    },
    'logging'   : {
        'logfile'   : 'log/ELT.log',
        'loglevel'  : '10',
    },
}

def write_configs(cfgfile='private/config.ini'):
    global cfgparser
    f = open(cfgfile,'w')
    cfgparser.write(f)
    f.close()
    return True

    
def get_configs(cfgparser, configs, cfgfile='private/config.ini'):

    print(cfgparser)
    print(configs)
    print(cfgfile)

    try:
        cfgparser.read(cfgfile)
    except Exception as e:
        print('Unable to parse config')
        print(e)
        sys.exit(1)

    print('reading', cfgfile)

    # Iterate through our sections and populate our configs dict
    for s in cfgparser.sections():
        if s in configs:
            configs[s] = conf_sec_map(cfgparser, s, configs[s])
        else:
            configs[s] = conf_sec_map(cfgparser, s)

    return configs
            
            
def conf_sec_map(cfg,section, rv = {}):
    '''
    Just pulls all the config items from a section
    as described in the tutorial
    '''
    options = cfg.options(section)
    for option in options:
        rv[option] = cfg.get(section, option)
    return rv




def main():
    '''
    Assumes our creds are somewhere!!!
    '''
    global configs

    configs = get_configs(cfgparser, configs, 'private/config.ini')

    # Set up our logging
    logcfg = configs['logging']
    log.basicConfig(
        filename    = logcfg['logfile'], 
        level       = int(logcfg['loglevel']),
        format      = '%(asctime)s;%(levelname)s;%(message)s',
    )

    log.info('Started')

    # Just to test our cursor for now
    cursor = sql.mysql_connect(configs['cloudsql'])
    # TODO: Test for tables & create if not exists
    #sql.create_tables()

    bgqry_params = configs['bigquery']
    # This is the output from our bigquery 
    rv = bgqry.get_data(bgqry_params, 'etl')
    # Print out our configs
    output_file = bgqry_params['dump_file']
    f = open(output_file, 'w')
    f.write(json.dumps(rv))
    f.close()

    # Note: To read the json: 
    # json_data = json.loads(open('output.json','r').read())

    log.info('Finished')
        



if __name__=='__main__':
    main()


