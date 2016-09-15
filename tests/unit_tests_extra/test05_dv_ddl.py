import unittest

from tests.unit_test_extra import _domeinmodel1, _domeinmodel2, _domeinmodel3, _domeinmodel4
from tests.unit_test_extra._configs import test_system_config
from tests.unit_test_extra._globals import *
from pyelt.mappings.sor_to_dv_mappings import SorToRefMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.databases import SourceQuery, SourceTable

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


def init_sor_to_dv_mappings(pipe):
    mappings = []
    return mappings


class TestCase_RunDv(unittest.TestCase):
    is_init = False
    """Testen met databse als bron"""

    def setUp(self):
        if not TestCase_RunDv.is_init:
            print('init_db')
            init_db()
            TestCase_RunDv.is_init = True
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system', config=test_system_config)

    def test_run1(self):
        print('======================================================')
        print('===        R U N  1                                ===')
        print('======================================================')
        # # maak tabellen in bron leeg
        # exec_sql(source_db_sql[0], test_system_config['source_connection'])
        # # vul bron tabellen met initiele waarden
        # exec_sql(source_db_sql[1], test_system_config['source_connection'])
        # exec_sql(source_db_sql[2], test_system_config['source_connection'])
        self.pipeline.run()

        dv = self.pipeline.dwh.dv
        dv.reflect()
        tables = dv.tables
        self.assertEqual(len(tables), 3)

        self.assertIn('_exceptions', tables)
        self.assertIn('_ref_values', tables)
        self.assertIn('_ref_valuesets', tables)

    def test_run2(self):
        print('======================================================')
        print('===        R U N  2                                ===')
        print('======================================================')
        self.pipe.register_domain(_domeinmodel1)
        self.pipeline.run()

        dv = self.pipeline.dwh.dv
        dv.reflect()
        tables = dv.tables
        self.assertEqual(len(tables), 10)
        self.assertIn('_ref_values', tables)
        self.assertIn('handeling_hub', tables)
        self.assertIn('handeling_sat', tables)
        self.assertIn('handeling_sat_financieel', tables)
        self.assertIn('organisatie_handeling_link', tables)
        self.assertIn('organisatie_sat', tables)
        views = dv.views
        self.assertEqual(len(views), 2)
        self.assertIn('organisatie_view', views)
        self.assertIn('handeling_view', views)

        handeling_tbl = tables['handeling_sat']
        cols = handeling_tbl.columns
        self.assertEqual(len(cols), 13)

        organisatie_sat_adres = tables['organisatie_sat_adres']
        cols = organisatie_sat_adres.columns
        self.assertEqual(len(cols), 16)
        self.assertIn('type', cols)
        self.assertIn('postcode', cols)
        # test op volgorde
        self.assertEqual(cols[11].name, 'postcode')

        # view_cols = views['organisatie_view'].columns
        # self.assertEqual(len(view_cols), 24)
        # self.assertIn('default_naam', view_cols)
        # self.assertIn('specialisatie_specialisaties_omschr', view_cols)
        # test op volgorde
        # self.assertEqual(view_cols[11].name, 'postcode')

    def test_run3(self):
        print('======================================================')
        print('===        R U N  3                                ===')
        print('======================================================')
        # Extra domein toevoegen met een overgeerfte entity met 1 extra sat
        self.pipe.register_domain(_domeinmodel2)
        self.pipeline.run()

        dv = self.pipeline.dwh.dv
        dv.reflect()
        tables = dv.tables
        self.assertEqual(len(tables), 13)
        self.assertIn('_ref_values', tables)
        self.assertIn('handeling_hub', tables)
        self.assertIn('handeling_sat_diagnose', tables)
        handeling_sat_diagnose = tables['handeling_sat_diagnose']
        cols = handeling_sat_diagnose.columns
        self.assertEqual(len(cols), 13)
        self.assertIn('diagnaam', cols)
        self.assertIn('diagnosecode', cols)
        col_type = cols[10].type.lower()
        self.assertIn(col_type, 'text')

    def test_run4(self):
        print('======================================================')
        print('===        R U N  4                                ===')
        print('======================================================')
        # Extra sat met alle col types
        self.pipe.register_domain(_domeinmodel3)
        self.pipeline.run()

        dv = self.pipeline.dwh.dv
        dv.reflect()
        tables = dv.tables
        self.assertEqual(len(tables), 15)
        self.assertIn('_ref_values', tables)
        self.assertIn('columntypes_hub', tables)
        self.assertIn('columntypes_sat', tables)
        column_types_sat = tables['columntypes_sat']
        cols = column_types_sat.columns
        self.assertEqual(len(cols), 16)
        self.assertIn(cols[10].type.lower(), 'text')
        self.assertIn(cols[11].type.lower(), 'date')
        self.assertIn(cols[12].type.lower(), 'timestamp without time zone')
        self.assertIn(cols[13].type.lower(), 'integer')
        self.assertIn(cols[14].type.lower(), 'numeric')
        self.assertIn(cols[15].type.lower(), 'text')

    def test_run5(self):
        print('======================================================')
        print('===        R U N  5                                ===')
        print('======================================================')
        # Diagnose sat aangepast en extra sat
        self.pipe.register_domain(_domeinmodel4)
        self.pipeline.run()

        dv = self.pipeline.dwh.dv
        dv.reflect()
        tables = dv.tables
        self.assertEqual(len(tables), 15)
        self.assertIn('handeling_sat_diagnose', tables)
        handeling_sat_diagnose = tables['handeling_sat_diagnose']
        cols = handeling_sat_diagnose.columns
        self.assertEqual(len(cols), 13)
        self.assertIn('diagnaam', cols)
        self.assertIn('extraveld', cols)


if __name__ == '__main__':
    unittest.main()
