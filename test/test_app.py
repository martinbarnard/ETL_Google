# Test app
import pytest
import mock

#from ..src import app
from  ETL_Google.src import app
from  ETL_Google.src import sql

class TestClass(object):
    @mock.patch('ETL_Google.src.app.configs')
    @mock.patch('ETL_Google.src.app.cfgparser')
    def test_get_configs(self, mocked_app_cfgparser):
        '''
        Testing if our configs are available - happy path
        '''
        configs = {
            'test': {
                'test_a': 'test_b',
            },
        }
        mocked_app_cfgparser.return_value.read = configs
        mocked_app_cfgparser.return_value.sections = configs.keys()
        mocked_app_cfgparser.return_value.options = {'test_a': 'test_b'} 
        v = ETL_Google.src.app.get_configs(mocked_app_cfgparser, {}, 'bob')
        assert v == configs


    @mock.patch('ETL_Google.src.app.configs')
    @mock.patch('ETL_Google.src.app.cfgparser')
    def test_get_configs_bad_config(self, mocked_app_cfgparser):
        '''
        Testing our config availability - bad config file
        '''
        configs = {
            'test': {
                'test_a': 'test_b',
            },
        }
        mocked_app_cfgparser.return_value.read = configs
        mocked_app_cfgparser.return_value.sections = configs.keys()
        mocked_app_cfgparser.return_value.options = {'test_a': 'test_b'} 
        v = ETL_Google.src.app.get_configs(mocked_app_cfgparser, {}, 'bob')
        assert v == configs




