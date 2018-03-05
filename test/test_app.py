# Test app
import pytest
from .. import app

    
def test_get_configs():
    '''
    Testing if our configs are available
    '''
    configs = app.get_configs(
        app.cfgparser,
        app.configs
    )
    assert configs != None 


