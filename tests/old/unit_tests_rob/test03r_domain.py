import unittest
from tests.unit_tests_rob import _domain_rob, _domain_rob_unittest
from tests.unit_tests_rob.global_test_suite import get_global_test_pipeline, execute_sql, init_db


class TestCase_Domain(unittest.TestCase):


    def setUp(self):
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system')
        p = self.pipe

    def test01_domain_registration(self):
        self.pipe.register_domain(_domain_rob)


        self.pipeline.run()
        self.assertNotEquals(len(self.pipe.domain_modules), 0)

    # def test02_domain_number_of_attributes_correct(self):
    #     sql = """SELECT COUNT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS
    #              WHERE TABLE_CATALOG = 'pyelt_unittests' AND TABLE_SCHEMA = 'dv'
    #              AND TABLE_NAME = 'zorgverlener_sat_personalia'"""
    #     result = execute_sql(sql)
    #     result = result[0][0]
    #     self.assertEqual(result, 14, 'Ik verwachte 14 kolommen; is domain_rob misschien recent veranderd?')

        # nieuw attribute toegevoegd aan zorgverlener_sat_personalia:

    def test03_domain_new_attribute_added(self):

        self.pipe.register_domain(_domain_rob_unittest)
        self.pipeline.run()
        sql = """SELECT COUNT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_CATALOG = 'pyelt_unittests' AND TABLE_SCHEMA = 'dv'
                 AND TABLE_NAME = 'zorgverlener_sat_personalia'"""
        result = execute_sql(sql)
        result = result[0][0]
        self.assertEqual(result,15, 'Ik verwachte 15 kolommen; is domain_rob_unittest misschien recent veranderd?')

    # # #




if __name__ == '__main__':
    init_db()
    unittest.main()

    # todo[rob]: bovenstaande is gewijzigd. Je kunt nu wel attributen toevoegen aan een sat. Kun je de test herschrijven zodat hij kijkt of er een nieuw veld bij is gekomen?
