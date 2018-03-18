import mock


def my_exception():
    raise Exception('testing')

@mock.patch('ETL_Google.src.app.Args')
@mock.patch('ETL_Google.src.app.parse_cmdline')
@mock.patch('ETL_Google.src.app.cfgparser')
def insert_row_happy_path():
    assert True

