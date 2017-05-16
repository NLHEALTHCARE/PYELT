from tests.unit_tests_basic import _domainmodel
from tests.unit_tests_basic.global_test_suite import get_global_test_pipeline, exec_sql, test_system_config, init_db
from tests.unit_tests_basic._mappings import init_source_to_sor_mappings, init_sor_to_dv_mappings
from tests.unit_tests_basic.test02_ref_mappings import init_test_ref_mappings
from main import get_root_path

__author__ = 'hvreenen'

import unittest




class TestCase_RunProces(unittest.TestCase):
    is_init = False

    # def __init__(self, args):
    #     super().__init__(args)
    #     self.

    def setUp(self):
        if not TestCase_RunProces.is_init:
            print('init_db')
            init_db()
            TestCase_RunProces.is_init = True
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system', config=test_system_config)
        self.pipe.register_domain(_domainmodel)
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings())
        self.pipe.mappings.extend(init_sor_to_dv_mappings(self.pipe.sor))
        #todo: onstaande rgel in unittests weer aanzetten
        # self.pipe.mappings.extend(init_test_ref_mappings())

    def test_run1(self):
        self.pipeline.run()
        get_row_count(self, 'sor_test_system.patient_hstage', 4)
        get_row_count(self, 'dv.patient_hub', 4)
        get_row_count(self, 'dv.patient_sat', 4)
        get_row_count(self, 'dv.patient_sat_personalia', 4)
        get_row_count(self, 'dv.patient_sat_adres', 4)
        get_row_count(self, 'dv.patient_sat_inschrijving', 4)
        get_row_count(self, 'dv.patient_sat_contactgegevens', 8)
        get_row_count(self, 'dv.handeling_hub', 4)
        get_row_count(self, 'dv.patient_traject_link', 4)

    def test_run1_her(self):
        self.pipeline.run()
        get_row_count(self, 'sor_test_system.patient_hstage', 4)
        get_row_count(self, 'dv.patient_hub', 4)
        get_row_count(self, 'dv.patient_sat', 4)
        get_row_count(self, 'dv.patient_sat_personalia', 4)
        get_row_count(self, 'dv.patient_sat_adres', 4)
        get_row_count(self, 'dv.patient_sat_inschrijving', 4)
        get_row_count(self, 'dv.patient_sat_contactgegevens', 8)
        get_row_count(self, 'dv.handeling_hub', 4)
        # get_row_count(self, 'dv.hulpverlener_hub', 2)
        get_row_count(self, 'dv.patient_traject_link', 4)

    def test_run2_from_source_to_sor(self):
        self.pipe.mappings[0].file_name = get_root_path() + '/PYELT/tests/data/patienten2.csv'
        self.pipe.mappings[0].delimiter = ';'
        self.pipeline.run()
        get_row_count(self, 'sor_test_system.patient_hstage', 7)
        get_row_count(self, 'dv.patient_hub', 5)
        get_row_count(self, 'dv.patient_sat', 5)
        get_row_count(self, 'dv.patient_sat_personalia', 5)
        get_row_count(self, 'dv.patient_sat_adres', 7)
        get_row_count(self, 'dv.patient_sat_inschrijving', 6)
        get_row_count(self, 'dv.patient_sat_contactgegevens', 11)



def get_row_count(unittest, table_name, count):
    test_sql = "SELECT * FROM " + table_name
    result = exec_sql(test_sql)
    unittest.assertEqual(len(result), count, table_name)



if __name__ == '__main__':
    # init_db()
    unittest.main()
