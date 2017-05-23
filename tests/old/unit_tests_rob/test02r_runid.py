import unittest
from _decimal import Decimal

from tests.unit_tests_rob import _domain_rob
from tests.unit_tests_rob.global_test_suite import get_global_test_pipeline, execute_sql


class TestCase_Runid(unittest.TestCase):

    def setUp(self):
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system')
        self.pipe.register_domain(_domain_rob)

    def test_max_runid_selected(self):
        self.pipeline.run()
        sql = 'SELECT max(runid) as max_runid from sys.runs'
        result = execute_sql(sql)
        dec = max(result)[0]

        self.assertEqual(dec, Decimal(str(self.pipeline.runid)),
                         'max(result)[0] en self.pipeline.runid zijn niet hetzelfde!')


if __name__ == '__main__':
    unittest.main()
