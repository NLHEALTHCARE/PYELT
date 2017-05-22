import unittest

from tests.unit_tests_basic._domainmodel import Valueset
from tests.unit_tests_basic.global_test_suite import *

from pyelt.mappings.sor_to_dv_mappings import SorToValueSetMapping


def init_test_valueset_mappings():
        mappings = []
        mapping = SorToValueSetMapping('patient_hstage', Valueset)
        mapping.map_field("target_valueset", Valueset.valueset_naam)
        mapping.map_field("target_code", Valueset.code)
        mapping.map_field("target_descr", Valueset.omschrijving)
        mappings.append(mapping)
        return mappings



class TestCase_Mappings(unittest.TestCase):
    def setUp(self):
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system')

    def test(self):
        mappings = init_test_valueset_mappings()
        valuset_mapping = mappings[0]
        self.assertEqual(valuset_mapping.name, 'patient_hstage -> valueset')
        self.assertEqual(len(valuset_mapping.field_mappings), 3)
        self.pipe.mappings.extend(mappings)









if __name__ == '__main__':
    unittest.main()
