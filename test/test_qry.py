import sys
import os
import mock
import ETL_Google.src.qry
import ETL_Google.src.configparser as cfgparser

sys.path.insert(0, os.path.abspath('..'))

@mock.patch('ETL_Google.src.qry')
def my_exception(mock_qry):
    '''
    Raise a standard mockery of an exception
    '''
    connection = mock.Object()

    raise Exception('testing')

def test_argparsing():
    '''
    Testing the argparse functions
    '''
    rv = qry.get_options()
    assert True




