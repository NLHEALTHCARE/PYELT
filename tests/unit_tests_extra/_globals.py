import unittest

from sqlalchemy import create_engine, text

# import _domainmodel
from tests.unit_tests_extra._configs import general_config
from pyelt.pipeline import Pipeline

__author__ = 'hvreenen'


def init_db():
    engine = create_engine(general_config['conn_dwh'])

    sql = """
    DROP SCHEMA IF EXISTS sor CASCADE;
    DROP SCHEMA IF EXISTS sor_extra CASCADE;
    DROP SCHEMA IF EXISTS rdv CASCADE;
    DROP SCHEMA IF EXISTS dv CASCADE;
    DROP SCHEMA IF EXISTS sys CASCADE;
    """

    query = text(sql)
    result = engine.execute(query)


def get_global_test_pipeline():
    pipeline = Pipeline(config=general_config)
    return pipeline


def exec_sql(sql, conn=general_config['conn_dwh']):
    engine = create_engine(conn)
    query = text(sql)
    result = engine.execute(query)
    return result


def get_rows(table_name, fields=['*'], filter='1=1'):
    fields = ','.join(fields)
    test_sql = "SELECT " + fields + " FROM " + table_name + " WHERE " + filter
    result = exec_sql(test_sql)
    return result.fetchall()


def get_fields(table_name, field_numbers=[], fields=['*'], filter='1=1'):
    rows = get_rows(table_name, fields, filter)
    return_rows = []
    for row in rows:
        return_row = []
        if not field_numbers:
            return_row = row
        else:
            for field_number in field_numbers:
                return_row.append(row[field_number])
        return_rows.append(return_row)
    print(return_rows)
    return return_rows


def get_row_count(table_name, filter='1=1'):
    test_sql = "SELECT * FROM " + table_name + " WHERE " + filter
    result = exec_sql(test_sql)
    return len(result.fetchall())
