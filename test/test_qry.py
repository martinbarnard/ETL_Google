import sys
import os
import mock
import pytest
import ETL_Google.src.qry

sys.path.insert(0, os.path.abspath('..'))
@pytest.fixture()

@mock.patch('ETL_Google.src.qry.mysql.connector')
@mock.patch('ETL_Google.src.qry.configparser')
def test_argparsing(mock_parser, mock_connector):
    '''
    Testing the argparse functions
    '''
    qry = ETL_Google.src.qry
    #q = qry.queryObj(cfg)
    #rv = q.get_options()
    with pytest.raises(SystemExit):
        qry.main()




