import unittest

from tests.unit_tests_extra import _domeinmodel
from tests.unit_tests_extra._configs import test_system_config
from tests.unit_tests_extra._globals import *
from pyelt.mappings.sor_to_dv_mappings import SorToValueSetMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.databases import SourceQuery, SourceTable

# DONE: ref: updates met active en finish date
# todo: hierarchie in refs
# DONE: _valuesets ook vullen vanuit refs (??)
# todo: omschrijving en omschrijving2 in refs



source_db_sql = [
    """DELETE FROM handelingen;""",
    """UPDATE public.specialisaties SET naam = 'ortho'
      WHERE id = 1""",
    """INSERT INTO handelingen(
            id, naam, datum, kosten, fk_org, fk_spec)
    VALUES (1, 'ok1', '2016-8-18 10:25:00', 123.99, 1, 1),
    (2, 'ok2', '2016-8-18 10:26:00', 500, 1, 1)""",

    """INSERT INTO handelingen(
            id, naam, datum, kosten, fk_org, fk_spec)
    VALUES (3, 'ok3', '2016-8-18 10:25:00', 123.99, 2, 2)""",

    """UPDATE public.specialisaties SET naam = 'nieuw'
      WHERE id = 1"""]


def init_source_to_sor_mappings(pipe):
    mappings = []
    validations = []
    source_db = pipe.source_db

    source_qry = SourceQuery(source_db, """
SELECT
  handelingen.id,
  UPPER(handelingen.naam) as naam,
  handelingen.datum,
  handelingen.kosten,
  organisaties.naam as org,
  organisaties.adres,
  (handelingen.naam || organisaties.naam) as naam2,
  'spec-level1' as spec_type,
  specialisaties.naam as spec,
  specialisaties.fk_hoofd,
  handelingen.fk_org,
  handelingen.fk_spec
FROM
  public.handelingen,
  public.organisaties,
  public.specialisaties
WHERE
  organisaties.id = handelingen.fk_org AND
  specialisaties.id = handelingen.fk_spec;
""", 'view_2')
    source_qry.set_primary_key(['id'])
    sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)
    mappings.append(sor_mapping)

    return mappings


def init_ref_mappings():
    mappings = []

    ref_mapping = SorToValueSetMapping({'M': 'man', 'V': 'vrouw', 'O': 'onbekend'}, 'geslacht_types')
    mappings.append(ref_mapping)

    ref_mapping = SorToValueSetMapping({'9': 'patienten', '7': 'mdw'}, 'relatie_soorten')
    mappings.append(ref_mapping)

    ref_mapping = SorToValueSetMapping('handeling_hstage', 'specialisaties')
    ref_mapping.map_code_field('fk_spec')
    ref_mapping.map_descr_field('spec')
    mappings.append(ref_mapping)

    # ref_mapping = SorToRefMapping('handeling_hstage', 'specialisaties2')
    # ref_mapping.map_code_field('fk_spec')
    # mappings.append(ref_mapping)

    return mappings


class TestCase_RunRef(unittest.TestCase):
    is_init = False
    """Testen met databse als bron"""

    def setUp(self):
        if not TestCase_RunRef.is_init:
            print('init_db')
            init_db()
            TestCase_RunRef.is_init = True
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system', config=test_system_config)
        # self.pipe.register_domain(_domeinmodel)
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings(self.pipe))
        self.pipe.mappings.extend(init_ref_mappings())

    def test_run1(self):
        print('======================================================')
        print('===        R U N  1                                ===')
        print('======================================================')
        # maak tabellen in bron leeg
        exec_sql(source_db_sql[0], test_system_config['source_connection'])
        # vul bron tabellen met initiele waarden
        exec_sql(source_db_sql[1], test_system_config['source_connection'])
        exec_sql(source_db_sql[2], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('dv._ref_valuesets'), 3)
        self.assertEqual(get_row_count('dv._ref_values'), 6)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='specialisaties'"), 1)

    def test_run2(self):
        print('======================================================')
        print('===        R U N  2                                ===')
        print('======================================================')
        # nog een keer zelfde runnen, moet geen wijziging zijn

        self.pipeline.run()
        self.assertEqual(get_row_count('dv._ref_valuesets'), 3)
        self.assertEqual(get_row_count('dv._ref_values'), 6)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='specialisaties'"), 1)

    def test_run3(self):
        print('======================================================')
        print('===        R U N  3                                ===')
        print('======================================================')
        # relatie_soorten aanpassen: nieuwe
        ref_mapping = SorToValueSetMapping({'9': 'patienten', '7': 'mdw', '6': 'artsen'}, 'relatie_soorten')
        self.pipe.mappings.append(ref_mapping)
        self.pipeline.run()
        self.assertEqual(get_row_count('dv._ref_valuesets'), 3)
        self.assertEqual(get_row_count('dv._ref_values'), 7)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='specialisaties'"), 1)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='relatie_soorten'"), 3)

    def test_run4(self):
        print('======================================================')
        print('===        R U N  4                                ===')
        print('======================================================')
        # relatie_soorten aanpassen: naam wijzigen
        ref_mapping = SorToValueSetMapping({'9': 'patienten', '7': 'medewerkers', '6': 'artsen'}, 'relatie_soorten')
        self.pipe.mappings.append(ref_mapping)
        self.pipeline.run()
        self.assertEqual(get_row_count('dv._ref_valuesets'), 3)
        self.assertEqual(get_row_count('dv._ref_values'), 8)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='specialisaties'"), 1)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='relatie_soorten'"), 4)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='relatie_soorten' AND _active"), 3)

    def test_run5(self):
        print('======================================================')
        print('===        R U N  5                                ===')
        print('======================================================')
        # specialisaties aanpassen: nieuwe
        exec_sql(source_db_sql[3], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('dv._ref_values'), 9)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='specialisaties'"), 2)

    def test_run6(self):
        print('======================================================')
        print('===        R U N  6                                ===')
        print('======================================================')
        # specialisaties aanpassen: update
        exec_sql(source_db_sql[4], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('dv._ref_values'), 10)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='specialisaties'"), 3)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='specialisaties' AND _active"), 2)

    def test_run7(self):
        print('======================================================')
        print('===        R U N  7                                ===')
        print('======================================================')
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings(self.pipe))
        ref_mapping = SorToValueSetMapping('handeling_hstage', 'specialisaties2')
        ref_mapping.map_code_field('fk_spec')
        ref_mapping.map_descr_field('spec')
        ref_mapping.map_type_field('spec_type', "'oid'")
        ref_mapping.map_level_field("'M-1'")

        self.pipe.mappings.extend([ref_mapping])
        self.pipeline.run()
        self.assertEqual(get_row_count('dv._ref_valuesets'), 4)
        self.assertEqual(get_row_count('dv._ref_values'), 12)
        self.assertEqual(get_row_count('dv._ref_values', filter="valueset_naam='spec-level1'"), 2)
        row = get_fields('dv._ref_values', [11, 12, 13], filter="valueset_naam='spec-level1'")[0]
        self.assertEqual(row[0], 'oid')
        self.assertEqual(row[1], 'spec-level1')
        self.assertEqual(row[2], 'M-1')

    def test_run8(self):
        print('======================================================')
        print('===        R U N  8                                ===')
        print('======================================================')
        # map met alleen code field
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings(self.pipe))
        ref_mapping = SorToValueSetMapping('handeling_hstage', 'specialisaties3')
        ref_mapping.map_code_field('fk_spec')
        self.pipe.mappings.extend([ref_mapping])
        self.pipeline.run()
        self.assertEqual(get_row_count('dv._ref_valuesets'), 5)
        self.assertEqual(get_row_count('dv._ref_values'), 14)


if __name__ == '__main__':
    unittest.main()
