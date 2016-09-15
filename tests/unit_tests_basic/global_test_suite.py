import unittest

from sqlalchemy import create_engine, text

# import _domainmodel
from tests._configs import general_config
from pyelt.pipeline import Pipeline

__author__ = 'hvreenen'

testmodules = [
    'tests.test01_pipeline',
    'tests.test02_mappings',
    'tests.test02_ref_mappings',
    'tests.test03_run_proces',
    # 'tests.test04_validations',
    # 'tests.test05_transformations',
    ]

test_system_config = {
    'source_path': '/data/',
    'sor_schema': 'sor_test_system'
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
    result = engine.execute(query)
    test = result

def get_global_test_pipeline():
    pipeline = Pipeline(config = general_config)
    return pipeline

def exec_sql(sql):
    engine = create_engine(general_config['conn_dwh'])
    query = text(sql)
    result = engine.execute(query)
    return result.fetchall()

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




