#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import mock

import ETL_Google.src.bgqry


@mock.patch('ETL_Google.src.bgqry.bigquery')
def test_get_data(mock_bigquery):
    '''
    Test our get_data function called
    '''
    ETL_Google.src.bgqry.SQL = {'a': 'Stuff'}
