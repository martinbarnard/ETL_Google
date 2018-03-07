import sys
import os
import mock
import ETL_Google.src.sql

sys.path.insert(0, os.path.abspath('..'))

def my_exception():
    raise Exception('testing')

@mock.patch('ETL_Google.src.app.Args')
@mock.patch('ETL_Google.src.app.parse_cmdline')
@mock.patch('ETL_Google.src.app.cfgparser')
def insert_row_happy_path():
    assert True

