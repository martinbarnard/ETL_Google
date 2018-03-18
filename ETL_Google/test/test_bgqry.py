#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
import pytest
import ETL_Google.src.bgqry as G

def my_exception():
    raise Exception('testing bigquery')

@mock.patch('ETL_Google.src.bgqry.bigquery')
def test_connect_pass(mock_google):
    '''
    '''
    etl = G.etl()
    rv = etl.connect()

    assert rv == True


@mock.patch('ETL_Google.src.bgqry.bigquery')
def test_connect_exception(mock_google):
    '''
    Test error getting data
    '''
    mock_google.Client.side_effect = my_exception
    etl = G.etl()

    with pytest.raises(Exception) as e_info:
        etl.connect()
        assert e_info == 'testing bigquery'


@mock.patch('ETL_Google.src.bgqry.bigquery')
def test_query_hit(mock_google):
    '''
    '''
    some_results = [{'a field':'a value'}]
    mock_google.Client.query.return_value = some_results
    etl = G.etl()
    rv = etl.connect()
    etl.SQL = {'a': 'Stuff'}
    rv2 = etl.query('a')
    assert etl.results
    assert rv  == True
    assert rv2 == True
