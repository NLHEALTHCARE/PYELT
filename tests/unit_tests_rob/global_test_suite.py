import unittest
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine, text
from tests._configs import general_config
from pyelt.pipeline import Pipeline

__author__ = 'hvreenen'

testmodules = [
    'tests.unit_tests_rob.test01r_pipeline',
    'tests.unit_tests_rob.test02r_runid',
    'tests.unit_tests_rob.test03r_domain',
    'tests.unit_tests_rob.test05r_process',
    ]

# general_config = {
#     'log_path': '/logs/',
#     'conn_dwh': conn_string_test_db,
#     'on_errors': 'throw'
#     }

test_system_config = {
    'source_path': 'c:/!ontwikkel/DWH2/PYELT/',
    'sor_schema': 'sor_test_system',
    'on_errors': 'throw'
    }


def init_db():
    engine = create_engine(general_config['conn_dwh'])

    sql = """
    DROP SCHEMA IF EXISTS sor CASCADE;
    DROP SCHEMA IF EXISTS sor_test_system CASCADE;
    DROP SCHEMA IF EXISTS rdv CASCADE;
    DROP SCHEMA IF EXISTS dv CASCADE;
    DROP SCHEMA IF EXISTS sys CASCADE;

    --CREATE SCHEMA hoeft niet!! Want wordt gedaan bij aanmaken van pipeline, als schema nog niet bestaat
    --CREATE SCHEMA sor;
    --CREATE SCHEMA rdv;
    --CREATE SCHEMA dv;
    """

    query = text(sql)
    engine.execute(query)


def get_global_test_pipeline():
    pipeline = Pipeline(config=general_config)
    return pipeline


# def exec_sql(sql):
#     engine = create_engine(conn_string_test_db)
#     query = text(sql)
#     result = engine.execute(query)
#     return result.fetchall()


def execute_sql(sql):
    # conn = psycopg2.connect("""host='localhost' dbname='pyelt_unittests' user='postgres' password='{}'""".format(get_your_password()))
    engine = create_engine(general_config['conn_dwh'])
    conn = engine.raw_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute(sql)
    result = cursor.fetchall()
    conn.commit()
    cursor.close()

    return result


def run():
    suite = unittest.TestSuite()

    for module in testmodules:
        try:
            # If the module defines a suite() function, call it to get the suite.
            mod = __import__(module, globals(), locals(), ['suite'])
            suitefn = getattr(mod, 'suite')
            suite.addTest(suitefn())
        except (ImportError, AttributeError):
            # else, just load all the test cases from the module.
            suite.addTest(unittest.defaultTestLoader.loadTestsFromName(module))

    unittest.TextTestRunner().run(suite)

if __name__ == '__main__':
    init_db()
    run()
