#!/usr/bin/env python
import sys
import os

from clint.arguments import Args
from clint.textui import puts, colored

sys.path.insert(0, os.path.abspath('..'))

# our hack account:
our_sql_cfg = dict(
    username='infectious',
    password = 'infectiousmedia',
    dbname = 'noaa_agg',
    proxy_port = 3333,
    host = 'localhost'
)

our_sql = "SELECT max_celsius max_c, min_celsius min_c, date, state FROM etl_agg where state=%s"

# argparsing made easy
args = Args()
gargs = args.grouped

try:
    import sql
except ImportError as e:
    from ETL_Google.src import sql


if args.contains('-h'):
    puts(colored.green('Help is on the way'))
    puts(colored.green('Usage: {} -state [STATE]'.format(args[0])))

if args.contains('-state'):
    state = gargs['-state'][0].upper()
else:
    puts(colored.blue('nothing to do'))
    sys.exit(0)

connection = sql.mysql_connect(our_sql_cfg)
if connection and state:
    print(our_sql,state)
    cursor = connection.cursor()
    cursor.execute(our_sql,(state,))

    for row in cursor:
        print(row)
else:
    puts(colored.red('Unable to connect to db'))


