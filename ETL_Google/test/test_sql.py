import mock
import ETL_Google.src.sql as SQL

config = {
    'db_name': 'test_db_name',
    'host': 'test_host',
    'username': 'test_username',
    'password': 'test_password',
    'port': 1209,
}


def my_exception():
    raise Exception('testing')


def test_init_happy_path():
    '''insert_rows'''
    s = SQL.cloudsql(config)
    assert s.db == config['db_name']


@mock.patch('ETL_Google.src.sql.mysql')
def test_connect_happy_path(mock_connection):
    # mock_connection.connector
    s = SQL.cloudsql(config)
    assert s.db == config['db_name']

    rval = s.connect()
    assert rval


@mock.patch('ETL_Google.src.sql.mysql')
def test_insert_row_happy_path(mock_connection):
    '''
    '''
    test_rows = [
        {
            'max': 'a max value',
            'min': 'a min value',
            'date': 'a date',
            'state': 'a state',
        }
    ]
    s = SQL.cloudsql(config)
    s.connect()
    rval = s.insert_rows(test_rows)
    assert rval


@mock.patch('ETL_Google.src.sql.mysql')
def test_create_db_happy_path(mock_connection):
    s = SQL.cloudsql(config)
    s.connect()
    rval = s.create_db()
    assert rval


@mock.patch('ETL_Google.src.sql.mysql')
def test_drop_table_happy_path(mock_connection):
    s = SQL.cloudsql(config)
    s.connect()
    rval = s.drop_table()
    assert rval


@mock.patch('ETL_Google.src.sql.mysql')
def test_create_table_happy_path(mock_connection):
    s = SQL.cloudsql(config)
    s.connect()
    rval = s.create_table()
    assert rval
