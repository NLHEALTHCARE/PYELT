import unittest
from pyelt.pipeline import Pipeline
from tests.unit_tests_rob import _domain_rob
from tests.unit_tests_rob.global_test_suite import get_global_test_pipeline, general_config, execute_sql


class TestCase_Pipeline(unittest.TestCase):
    def setUp(self):
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system')

    def test_pipeline_exists(self):
        self.pipeline = Pipeline(general_config)
        self.assertIsNotNone(self.pipeline.dwh)
        # hj: sor is uit pipeline gehaald deze zit nu in pipe
        # self.assertIsNotNone(self.pipeline.dwh.sor)
        self.assertIsNotNone(self.pipeline.dwh.rdv)
        self.assertIsNotNone(self.pipeline.dwh.dv)
        self.assertIsNotNone(self.pipeline.dwh.sys)

    # hj: onderstaande tests heb ik uitgezet, want proces van aanmaken is gewijzigd en geeft nu conflicten
    # het zal geen reele situatie mogen zijn dat iemand schema's weg gaat gooien.
    #er zou een auth-error moeten komen

    # def drop_schema_dv(self):
    #     sql = """DROP SCHEMA IF EXISTS dv CASCADE;"""
    #     execute_sql(sql)
    #
    # def test_schema_not_existing(self):
    #     sql = """DROP SCHEMA IF EXISTS dv CASCADE;
    #              SELECT exists(select schema_name FROM information_schema.schemata WHERE schema_name = 'dv')"""
    #     result = execute_sql(sql)
    #     schema_present = result[0][0]
    #
    #     self.assertFalse(schema_present, 'bestaat al wel!')

    def test_schema_existing(self):
        self.pipe.register_domain(_domain_rob)
        self.pipeline.run()
        sql = """SELECT exists(select schema_name FROM information_schema.schemata WHERE schema_name = 'dv')"""
        result = execute_sql(sql)
        schema_present = result[0][0]

        self.assertTrue(schema_present, msg='bestaat nog niet!')

    def test_sql_logger_exists(self):

        self.pipe.register_domain(_domain_rob)
        self.pipeline.run()
        sql_logger = self.pipeline.sql_logger
        self.assertIsNotNone(sql_logger)

if __name__ == '__main__':
    unittest.main()


