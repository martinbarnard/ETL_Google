#!/usr/bin/env python
import sys
import os
import configparser
import mysql.connector
from clint.arguments import Args
from clint.textui import puts, colored

# standalone command-line tool to query the db.
sys.path.insert(0, os.path.abspath('..'))

class queryObj(object):
    '''
    A query object. Takes a value or set of values and does query stuff with them
    '''
    def __init__(self, cfg):
        dbname = 'noaa_agg'
        self.queries = {
            'get_states':  "SELECT DISTINCT state from etl_agg ORDER BY STATE",
            'dates' : "SELECT min_celsius min_c, max_celsius max_c, date, state from etl_agg where date=%s",
            'states' : "SELECT min_celsius min_c, max_celsius max_c, date, state FROM etl_agg where state=%s",
        }
        self.cfg = cfg
        self.results = []
        self.connection = mysql.connector.connect(
            host=cfg['host'],
            user=cfg['username'],
            password=cfg['password'],
            port=cfg['port'],
            database=dbname
        )

    def print_results(self):
        '''
        Print out the results
        '''
        if len(self.results) > 0:
            for row in self.results:
                puts("Date: {}\tmin: {:.2f}\t max: {:.2f}\tState: {}".format(row[2], row[0], row[1], row[3]))

    def do_qry(self, qry='states', qv = None):
        '''
        Run the selected query & put results into a dcit
        '''
        if qry in self.queries:
            query = self.queries[qry]
            if self.connection is not None:
                cnx = self.connection
                cursor = cnx.cursor()
                cursor.execute(query,(qv,))

                # our return values
                for row in cursor:
                    self.results.append(row)
                if len(self.results)>0:
                    return True
                else:
                    return False
            else:
                puts(colored.red('No connection!'))
                return False


    
def get_config():
    cfgparser = configparser.ConfigParser()
    cfgfile = 'private/config.ini'
    cfgparser.read(cfgfile)
    our_sql_cfg = dict()
    our_sql_cfg['dbname'] = cfgparser.get('cloudsql','db_name')
    our_sql_cfg['username'] = cfgparser.get('cloudsql', 'username')
    our_sql_cfg['password'] = cfgparser.get('cloudsql', 'password')
    our_sql_cfg['port'] = cfgparser.get('cloudsql','port')
    our_sql_cfg['host'] = cfgparser.get('cloudsql','host')
    return our_sql_cfg
        


def print_help(nm='qry'):
    '''
    '''
    puts(colored.green('Help is on the way'))
    puts(colored.green('Usage: {} -state <STATE> / -date <DATE>\n'.format(nm)))
    puts(colored.green('To list states -get'))
    puts('where -state [STATE] is the state 2-letter code (e.g. AL)')
    puts('where -date is a date in [YYYY-MM-DD] format (e.g. 1995-03-21)')

def main():
    '''
    Main application for queryinmt
    '''
    # argparsing made easy
    args = Args()
    gargs = args.grouped
    cfg = get_config()

    if args.contains('-h'):
        print_help(args[0])
        sys.exit(0)

    # Get our db object
    dbobj = queryObj(cfg)

    if args.contains('-state'):
        state = gargs['-state'][0].upper()
        puts(colored.yellow('Calling with state "{}"'.format(state)))
        result = dbobj.do_qry('states', state)
        if result:
            dbobj.print_results()

    elif args.contains('-date'):
        dt = gargs['-date'][0]
        result = dbobj.do_qry('dates', dt)
        if result:
            dbobj.print_results()
    elif args.contains('-get'):
        dt = gargs['-get'][0]
        result = dbobj.do_qry('get_state')
        if result:
            dbobj.print_results()
    else:
        print_help(args[0])

    sys.exit(0)

if __name__=='__main__':
    main()
