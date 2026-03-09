import pytest
import cx_Oracle

#target db connection
@pytest.fixture(scope='function')
def Oracle_conn():
    target_conn=cx_Oracle("target/target@localhost:1521/xe")
    yield target_conn
    target_conn.close()