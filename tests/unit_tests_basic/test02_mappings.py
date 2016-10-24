import unittest

from tests.unit_tests_basic.global_test_suite import *
from main import get_root_path
from tests.unit_tests_basic._domainmodel import Patient
from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.mappings.transformations import FieldTransformation
from pyelt.sources.files import CsvFile


class TestCase_mappings2(unittest.TestCase):
    def setUp(self):
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system')


    def test_source_to_sor_mappings(self):

        source_file = CsvFile(get_root_path() + '/PYELT/tests/data/patienten1.csv', delimiter=';')
        source_file.reflect()
        l = len(source_file.columns)
        self.assertEqual(len(source_file.columns), 16)
        self.assertEqual(len(source_file.primary_keys()), 0)
        source_file.set_primary_key(['patientnummer'])
        self.assertEqual(len(source_file.primary_keys()), 1)

        sor_mapping = SourceToSorMapping(source_file, 'persoon_hstage', auto_map=True)
        self.assertEqual(sor_mapping.name, 'patienten1.csv -> persoon_hstage')
        self.assertEqual(len(sor_mapping.field_mappings), 16)
        self.pipe.mappings.append(sor_mapping)

    def test_sor_to_dv_mappings(self):
        sor = self.pipe.sor
        mapping = SorToEntityMapping('patient_hstage', Patient, sor)
        mapping.map_field("patientnummer || '_test'", Patient.bk)
        mapping.map_field('achternaam', Patient.Personalia.achternaam)
        mapping.map_field('tussenvoegsels', Patient.Personalia.tussenvoegsels)
        mapping.map_field('voornaam', Patient.Personalia.voornaam)
        mapping.map_field('straat', Patient.Adres.straat)
        mapping.map_field('huisnummer::integer', Patient.Adres.huisnummer)
        mapping.map_field('huisnummertoevoeging', Patient.Adres.huisnummer_toevoeging)
        mapping.map_field('postcode')
        mapping.map_field('LEFT(hstg.postcode, 4)', Patient.Adres.postcode)
        mapping.map_field('plaats', Patient.Adres.plaats)
        mapping.map_field('geslacht')
        mapping.map_field('geboortedatum::date', Patient.Default.geboortedatum)
        mapping.map_field('inschrijvingsnummer', Patient.Inschrijving.inschrijfnummer)
        mapping.map_field('bsn => default.bsn', Patient.Inschrijving.bsn)
        mapping.map_field('inschrijvingsdatum::date')
        mapping.map_field('telefoon', Patient.ContactGegevens.telnummer, type=Patient.ContactGegevens.Types.telefoon)
        mapping.map_field('mobiel', Patient.ContactGegevens.telnummer, type=Patient.ContactGegevens.Types.mobiel)
        mapping.map_field('telefoon', Patient.ContactGegevens.telnummer, type='telefoon')
        self.assertEqual(mapping.name, 'patient_hstage -> patient_entity')
        num_sats = len(mapping.sat_mappings)
        bk = mapping.bk_mapping
        self.assertEqual(num_sats, 6)
        self.assertEqual(bk, "patientnummer || '_test'")
        sat_mapping = mapping.sat_mappings['patient_hstage -> patient_sat_adres']
        num_field_mappings = len(sat_mapping.field_mappings)
        self.assertEqual(num_field_mappings, 5)
        self.assertIsInstance(sat_mapping.field_mappings[3].source, FieldTransformation)

        source_fields_sql_part = sat_mapping.get_source_fields()
        source_fields_sql_part2 = sat_mapping.get_source_fields(alias = 'src')
        self.assertEqual(source_fields_sql_part, 'hstg.straat,hstg.huisnummer::integer,hstg.huisnummertoevoeging,LEFT(hstg.postcode, 4),hstg.plaats')
        #todo herstellen
        # self.assertEqual(source_fields_sql_part2, 'src.straat,src.huisnummer::integer,src.huisnummertoevoeging,LEFT(src.postcode, 4),src.plaats')


if __name__ == '__main__':
    unittest.main()
