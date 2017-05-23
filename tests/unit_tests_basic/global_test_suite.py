import unittest

from sqlalchemy import create_engine, text

from pyelt.pipeline import Pipeline
from tests.unit_tests_basic._configs import general_config

__author__ = 'hvreenen'

testmodules = [
    'tests.unit_tests_basic.test01_pipeline',
    'tests.unit_tests_basic.test02_mappings',
    'tests.unit_tests_basic.test02_valueset_mappings',
    'tests.unit_tests_basic.test03_run_proces',
    'tests.unit_tests_basic.test04_validations',
    'tests.unit_tests_basic.test05_db_functions',
    'tests.unit_tests_basic.test06_transformations',
    'tests.unit_tests_basic.test07_deletes',
    ]

test_system_config = {
    'source_path': '/tests/data/',
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
    #zorg dat singleton van Pipeline niet actueel is. Iedere test heeft namelijk zijn eigen pipeline en zijn eigen database nodig.
    Pipeline._instance = None
    pipeline = Pipeline(config = general_config)
    return pipeline

def exec_sql(sql):
    engine = create_engine(general_config['conn_dwh'])
    query = text(sql)
    result = engine.execute(query)
    return result.fetchall()

def run_all():
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

    unittest.TextTestRunner(verbosity=2).run(suite)


def run_tryout():
    import tests.unit_tests_basic.test01_pipeline as mod
    testcase = mod.TestCase_Pipeline('test_pipeline_exists')
    testcase.test_pipeline_exists()

    import tests.unit_tests_basic.test03_run_proces as mod
    testcase = mod.TestCase_RunProces()
    testcase.setUp()
    testcase.test_run1()
    # testcase.test_run1_her()


    # for module in testmodules:
    #     mod = __import__(module, globals(), locals())
    #     # mod.run()
    #     cmd = """import {} as mod
    #     mod.run()""".format(module)
    #     eval('run()')



if __name__ == '__main__':
    # init_db()
    run_all()




