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

our_sql = '''SELECT * FROM etl_agg where state='%s';'''

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

connection = sql.mysql_connect(our_sql_cfg)
if connection and state:
    cursor = connection.cursor(buffered=True)
    puts(colored.green('executing "{}" : "{}"'.format(our_sql, state)))

    results = cursor.execute(our_sql,state)

    if results is not None:
        for row in results:
            puts(colored.green(row))
    else:
        puts(colored.blue('no results found'))
else:
    puts(colored.red('Unable to connect to db'))



