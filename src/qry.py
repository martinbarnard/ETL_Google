#!/usr/bin/env python
import sys
import os
import configparser
from clint.arguments import Args
from clint.textui import puts, colored

try:
    import sql
except ImportError as e:
    from ETL_Google.src import sql

sys.path.insert(0, os.path.abspath('..'))

def get_options():
    cfgparser = configparser.ConfigParser()
    cfgfile = 'private/config.ini'
    cfgparser.read(cfgfile)
    our_sql_cfg = dict()
    options = cfgparser.options('cloudsql')
    our_sql_cfg['dbname'] = cfgparser.get('cloudsql','db_name')
    our_sql_cfg['username'] = cfgparser.get('cloudsql', 'username')
    our_sql_cfg['password'] = cfgparser.get('cloudsql', 'password')
    our_sql_cfg['proxy_port'] = cfgparser.get('cloudsql','port')
    our_sql_cfg['host'] = cfgparser.get('cloudsql','host')
    return our_sql_cfg
        

our_sql = "SELECT min_celsius min_c, max_celsius max_c, date, state FROM etl_agg where state=%s"
get_states = "SELECT DISTINCT state from etl_agg ORDER BY STATE"
get_dates = "SELECT min_celsius min_c, max_celsius max_c, date, state from etl_agg where date=%s"

# argparsing made easy
args = Args()
gargs = args.grouped
our_sql_cfg = get_options()
connection = sql.mysql_connect(our_sql_cfg)
cursor = connection.cursor()

try:
    import sql
except ImportError as e:
    from ETL_Google.src import sql


if args.contains('-h'):
    puts(colored.green('Help is on the way'))
    puts(colored.green('Usage: {} -state <STATE> / -date <DATE>\n'.format(args[0])))
    puts('where -state [STATE] is the state 2-letter code (e.g. AL)')
    puts('where -date is a date in [YYYY-MM-DD] format (e.g. 1995-03-21)')
    sys.exit(0)

if args.contains('-state'):
    state = gargs['-state'][0].upper()
    puts(colored.yellow('Calling with state "{}"'.format(state)))
    cursor.execute(our_sql,(state,))
    for row in cursor:
        puts("min: {: .2f}\t max: {: .2f}\tDate: {}".format(row[0], row[1], row[2]))
    sys.exit(0)

elif args.contains('-date'):
    dt = gargs['-date'][0]
    puts(colored.yellow('going for a date: {}'.format(dt)))
    cursor.execute(get_dates,(dt,))
    for row in cursor:
        puts("min: {: .2f}\t max: {: .2f}\tState: {}".format(row[0], row[1], row[3]))
    sys.exit(0)

else:
    cursor.execute(get_states)
    for row in cursor:
        puts(row[0])



