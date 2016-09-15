from tests.global_test_suite import *
import unittest
from main import get_root_path
from pyelt.helpers.mappingcreator import MappingWriter
from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping, SorToLinkMapping, SorToRefMapping
from pyelt.mappings.transformations import FieldTransformation

from pyelt.sources.files import CsvFile
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping

def init_test_ref_mappings():
        mappings = []

        ref_mapping = SorToRefMapping({'M': 'man', 'V': 'vrouw', 'O': 'onbekend'}, 'geslacht_types')
        mappings.append(ref_mapping)

        ref_mapping = SorToRefMapping({'9': 'patienten', '7': 'mdw'}, 'relatie_soorten')
        mappings.append(ref_mapping)

        ref_mapping = SorToRefMapping('patient_hstage', 'geslacht_types2')
        ref_mapping.map_code_field('patient_hstage.geslacht')
        ref_mapping.map_descr_field('patient_hstage.geslacht')
        mappings.append(ref_mapping)

        return mappings



class TestCase_Mappings(unittest.TestCase):
    def setUp(self):
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system')

    def test(self):
        mappings = init_test_ref_mappings()
        ref_mapping = mappings[2]
        self.assertEqual(ref_mapping.name, 'patient_hstage -> _valuesets')
        self.assertEqual(len(ref_mapping.field_mappings), 2)
        self.pipe.mappings.extend(mappings)









if __name__ == '__main__':
    unittest.main()
