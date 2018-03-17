# Test app

import mock
import sys
import os
import pytest
import src.app as A

def my_exception(stuff):
    raise Exception('testing')

@mock.patch('src.app.Args')
def test_parse_cmdline(mock_args):
    '''
    Test that our command-line parser works
    '''
    mock_args.grouped.return_value = {'-h': 'help I test'}
    mock_args.contains.return_value = True
    with pytest.raises(SystemExit):
        r,v = A.parse_cmdline()


@mock.patch('src.app.Args')
@mock.patch('src.app.parse_cmdline')
@mock.patch('src.app.cfgparser')
def test_err_reading_configs(mock_parser, mock_cmdparse, mock_args):
    '''
    Not so happy - Error reaading config
    '''
    mock_parser.read.side_effect = my_exception
    mock_cmdparse.return_value = 'config file', {'test_a': 'test_b'}
    with pytest.raises(Exception):
        rv = app.get_configs(mock_parser)
    

