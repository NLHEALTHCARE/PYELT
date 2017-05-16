from tests.unit_tests_basic import _domainmodel
from tests.unit_tests_basic._domainmodel import Patient
from tests.unit_tests_basic._mappings import init_source_to_sor_mappings, init_sor_to_dv_mappings
from tests.unit_tests_basic.global_test_suite import get_global_test_pipeline, exec_sql, test_system_config, init_db
from main import get_root_path
from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.mappings.validations import Validation, SorValidation, DvValidation
from pyelt.sources.files import CsvFile

__author__ = 'hvreenen'

import unittest

class TestCase_Validations(unittest.TestCase):
    is_init = False
    def setUp(self):
        if not TestCase_Validations.is_init:
            print('init_db')
            init_db()
            TestCase_Validations.is_init = True
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system', config=test_system_config)
        self.pipe.register_domain(_domainmodel)
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings())
        self.pipe.mappings.extend(init_sor_to_dv_mappings(self.pipe.sor))
        validations = []

        validation = SorValidation(tbl='patient_hstage', schema=self.pipe.sor)
        validation.msg = 'Ongeldige postcode'
        validation.sql_condition = "postcode like '0000%'"
        validations.append(validation)

        validation = SorValidation(tbl='patient_hstage', schema=self.pipe.sor)
        validation.msg = 'Ongeldig geslacht'
        validation.sql_condition = "geslacht not in ('m', 'v')"
        validations.append(validation)

        validation = DvValidation()
        validation.msg = '102 is ongeldig'
        validation.set_condition(Patient.Hub.bk == '102_test')
        validations.append(validation)

        validation = DvValidation()
        validation.msg = 'te jong'
        validation.set_condition(Patient.Default.geboortedatum >= '2000-01-01')
        validations.append(validation)

        self.pipe.validations.extend(validations)

    def test_run1(self):
        self.pipeline.run()
        self.assertEqual(self.get_row_count('sor_test_system.patient_hstage', '_valid = True'), 2)
        # 2 ipv 4
        self.assertEqual(self.get_row_count('dv.patient_hub'), 2)
        self.assertEqual(self.get_row_count('dv.patient_hub', '_valid = True'), 1)
        self.assertEqual(self.get_row_count('dv.patient_sat', '_valid = True'), 1)

    def get_row_count(unittest, table_name, condition=''):
        test_sql = "SELECT * FROM " + table_name
        if condition:
            test_sql += ' WHERE ' + condition
        result = exec_sql(test_sql)
        return len(result)

if __name__ == '__main__':
    unittest.main()
