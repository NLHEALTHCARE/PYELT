from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping
from tests.unit_tests_basic import _domainmodel, _db_functions, _db_functions2
from tests.unit_tests_basic._db_functions import CreateAgb
from tests.unit_tests_basic._domainmodel import Patient
from tests.unit_tests_basic._mappings import init_source_to_sor_mappings
from tests.unit_tests_basic.global_test_suite import get_global_test_pipeline, init_db

__author__ = 'hvreenen'

import unittest


class TestCase_DbFunctions(unittest.TestCase):
    is_init = False

    def setUp(self):
        if not TestCase_DbFunctions.is_init:
            init_db()
            TestCase_DbFunctions.is_init = True
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system')
        self.pipe.register_domain(_domainmodel)
        self.pipe.register_db_functions(_db_functions, self.pipe.sor)
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings())
        self.pipe.mappings.extend(init_sor_to_dv_mappings(self.pipe.sor))

    def test_run1(self):
        self.pipeline.run()
        self.pipe.sor.reflect_functions()
        self.assertEquals(2, len(self.pipe.sor.functions))

    def test_run1_her(self):
        self.pipeline.run()
        self.pipe.sor.reflect_functions()
        self.assertEquals(2, len(self.pipe.sor.functions))

    def test_run3(self):
        self.pipe.register_db_functions(_db_functions2, self.pipe.sor)
        self.pipeline.run()
        self.pipe.sor.reflect_functions()
        self.assertEquals(3, len(self.pipe.sor.functions))


def init_sor_to_dv_mappings(sor):
    mappings = []
    mapping = SorToEntityMapping('patient_hstage', Patient, sor)
    mapping.map_bk('patientnummer')
    # mapping.map_field('lower(upper(achternaam)) => personalia.achternaam text')
    # mapping.map_field('tussenvoegsels => personalia.tussenvoegsels text')
    # mapping.map_field('voornaam => personalia.voornaam text')
    # mapping.map_field('straat => adres.straat text')
    # mapping.map_field('huisnummer::integer => adres.huisnummer integer')
    # mapping.map_field('huisnummertoevoeging => adres.huisnummertoevoeging text')
    # mapping.map_field('postcode => adres.postcode text')
    # mapping.map_field('plaats => adres.plaats text')
    # mapping.map_field('geslacht => default.geslacht text')
    # mapping.map_field('now() => default.geboortedatum date')
    # mapping.map_field("Coalesce(inschrijvingsnummer, 'jahgsd') => default.inschrijvingsnummer text")
    # mapping.map_field('bsn => default.bsn text')
    # mapping.map_field('inschrijvingsdatum::date => default.inschrijvingsdatum date')
    mapping.map_field(CreateAgb('inschrijvingsdatum', 'bsn'), Patient.Personalia.achternaam)
    mapping.map_field(CreateAgb('plaats', 'bsn'), Patient.Personalia.tussenvoegsels)
    mappings.append(mapping)
    return mappings


if __name__ == '__main__':
    unittest.main()



# def test_transform(self):
#     t = FieldTransformation()
#     t.field_name = 'field'
#     t.new_step('inner({fld})')
#     t.new_step('outer({fld})')
#     sql = t.get_sql('')
#     self.assertEqual(sql, 'outer(inner(field))')
#
#     t = FieldTransformation()
#     t.field_name = 'field'
#     t.new_step('inner({fld}, 1)')
#     t.new_step('outer({fld})')
#     sql = t.get_sql('')
#     self.assertEqual(sql, 'outer(inner(field, 1))')
#
#     t = FieldTransformation()
#     t.field_name = 'field'
#     t.new_step('inner({fld})')
#     t.new_step('outer({fld}, 1)')
#     sql = t.get_sql('')
#     self.assertEqual(sql, 'outer(inner(field), 1)')
#     print(sql)
#
#     t = FieldTransformation()
#     t.field_name = 'field'
#     t.new_step('inner({fld}, field2)')
#     t.new_step('outer({fld})')
#     sql = t.get_sql('tbl')
#     #todo oplossen
#     self.assertEqual(sql, 'outer(tbl.inner(tbl.field, tbl.field2))')
