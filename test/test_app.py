# Test app

import mock
import sys
import os

sys.path.insert(0, os.path.abspath('..'))

from ETL_Google.src import app
#from ETL_Google.src import sql

def my_exception():
    raise Exception('testing')

@mock.patch('ETL_Google.src.app.Args')
@mock.patch('ETL_Google.src.app.parse_cmdline')
@mock.patch('ETL_Google.src.app.cfgparser')
def test_get_configs(mock_parser, mock_cmdparse, mock_args):
    '''
    Testing if our configs are available - happy path
    This is a bit hacky, but I've never used the mock decorator
    '''
    mockery = {
        'test': {
            'test_a': 'test_b',
        },
    }
    mock_args.grouped.return_value = {'-h': 'help I test'}
    mock_args.contains.return_value = False
    mock_args.return_value = ['-h']

    mock_cmdparse.return_value = 'config file', {'test_a': 'test_b'}

    mock_parser.read
    mock_parser.get.return_value = mockery['test']['test_a']
    mock_parser.sections.return_value = mockery.keys()
    mock_parser.options.return_value = mockery['test']

    v, w = app.get_configs(mock_parser)
    assert v == 'test_a'

@mock.patch('ETL_Google.src.app.Args')
@mock.patch('ETL_Google.src.app.parse_cmdline')
@mock.patch('ETL_Google.src.app.cfgparser')
def test_err_reading_configs(mock_parser, mock_cmdparse, mock_args):
    '''
    Not so happy - Error reaading config
    '''

    mock_parser.read.side_effect = my_exception
    mock_cmdparse.return_value = 'config file', {'test_a': 'test_b'}
    rv = app.get_configs(mock_parser)
    assert rv == None
    


