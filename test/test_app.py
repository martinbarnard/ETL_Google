# Test app

import pytest
import mock
import sys
import os

sys.path.insert(0, os.path.abspath('..'))

from ETL_Google.src import app
from ETL_Google.src import sql


@mock.patch('ETL_Google.src.app.Args')
@mock.patch('ETL_Google.src.app.parse_cmdline')
@mock.patch('ETL_Google.src.app.cfgparser')
def test_get_configs(mock_parser, mock_cmdparse, mock_args):
    '''
    Testing if our configs are available - happy path
    This is a bit hacky, but I've never used the mock decorator
    '''
    mock_parser.sections.return_value = ['test']
    mock_parser.options.return_value = {'test_a': 'test_b'}
    mock_args.grouped.return_value = {'-h': 'help I test'}
    mock_args.contains.return_value = False
    mock_cmdparse.return_value = 'config file', {'test_a': 'test_b'}

    mock_args.return_value = ['-h']
    mock_parser.objects.filter.return_value = {
        'test': {
            'test_a': 'test_b',
        },
    }

    v, w = app.get_configs(mock_parser)
    assert v == 'test_a'
    assert w == 'test'

