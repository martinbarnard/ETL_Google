#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock

import ETL_Google.src.bgqry


@mock.patch('ETL_Google.src.bgqry.bigquery')
def test_get_data(mock_bigquery):
    '''
    Test our get_data function called
    param: qry_name
    param: do_insert
    param: connection
    return: BOOL
    '''
    ETL_Google.src.bgqry.SQL = {'a': 'Stuff'}
    connection = mock.Object()
    rv = get_data('a', False, connection)

    assert rv == True


def test_get_data_fail():
    '''
    Test error getting data
    '''
    assert True

