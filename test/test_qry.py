import sys
import os
import mock
import ETL_Google.src.qry as qry
import ETL_Google.src.configparser as cfgparser


sys.path.insert(0, os.path.abspath('..'))

def my_exception():
    '''
    Raise a standard mockery of an exception
    '''
    raise Exception('testing')

def test_argparsing():
    '''
    Testing the argparse functions
    '''
    rv = qry.get_options()
    assert True




