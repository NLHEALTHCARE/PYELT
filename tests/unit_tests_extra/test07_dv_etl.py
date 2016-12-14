import unittest

from tests.unit_tests_extra import _domeinmodel
from tests.unit_tests_extra._configs import test_system_config
from tests.unit_tests_extra._globals import *
from tests.unit_tests_extra._domeinmodel import *
from pyelt.datalayers.database import Table
from pyelt.helpers.mappingcreator import MappingWriter
from pyelt.mappings.base import ConstantValue
from pyelt.mappings.sor_to_dv_mappings import SorToValueSetMapping, SorToEntityMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.databases import SourceQuery, SourceTable

from domainmodels import entity_domain, role_domain, act_domain, participation_domain

# Don revision  start bij 0,
# Done auto cast van types anders dan text

source_db_sql = [
    """DELETE FROM handelingen;""",

    """INSERT INTO handelingen(
            id, naam, datum, kosten, fk_org, fk_spec)
    VALUES (1, 'ok1', '2016-8-18 10:25:00', 123.99, 1, 1),
    (2, 'ok2', '2016-8-18 10:26:00', 500, 1, 1)""",

    """UPDATE handelingen SET naam = 'ok1a' where id = 1""",
    """UPDATE handelingen SET naam = 'ok1b' where id = 1""",
    """UPDATE handelingen SET naam = 'ok1' where id = 1""",
    """INSERT INTO handelingen(
                id, naam, datum, kosten, fk_org, fk_spec)
        VALUES (4, 'ok4', '2016-8-19 10:25:00', 123.99, 1, 1),
        (5, 'ok5', '2016-8-19 10:26:00', 500, 3, 3)""",
    """UPDATE handelingen SET naam = 'ok4' where id = 4""",

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

    source_tbl = SourceTable('handelingen', source_db.default_schema, source_db)
    source_tbl.set_primary_key(['id'])
    sor_mapping = SourceToSorMapping(source_tbl, 'handeling2_hstage', auto_map=True, ignore_fields=['fk_org'])
    mappings.append(sor_mapping)
    return mappings


# def init_sor_to_dv_mappings(pipe):
#     mappings = []
#
#     mapping = SorToEntityMapping('handeling_hstage', Handeling, pipe.sor)
#     mapping.map_bk('id')
#     mapping.map_field("naam", Handeling.Default.naam)
#     mapping.map_field("datum::date", Handeling.Default.datum)
#     # mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
#     mappings.append(mapping)
#
#     mapping = SorToEntityMapping('handeling_hstage', Organisatie, pipe.sor)
#     mapping.map_bk('fk_org')
#     mapping.map_field("org", Organisatie.Default.naam)
#     mappings.append(mapping)
#
#     # source_sql = "SELECT een.id, een.id as id2, twee.naam as twee_naam FROM sor_extra.handeling_hstage een JOIN sor_extra.handeling2_hstage twee ON een.id = twee.id"
#     # mapping = SorToEntityMapping(source_sql, Handeling, pipe.sor)
#     # mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
#     # mappings.append(mapping)
#     return mappings


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
        self.pipe.register_domain(_domeinmodel)
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings(self.pipe))
        # self.pipe.mappings.extend(init_sor_to_dv_mappings(self.pipe))

    # def test00(self):
    #     self.pipeline.run()
    #     tbl = Table('handeling_hstage', self.pipe.sor)
    #     s = MappingWriter.create_python_code_mappings(tbl, Handeling)
    #     print(s)

    #
    def test_run1(self):
        print('======================================================')
        print('===        R U N  1                                ===')
        print('======================================================')
        # # maak tabellen in bron leeg
        exec_sql(source_db_sql[0], test_system_config['source_connection'])
        # # vul bron tabellen met initiele waarden
        exec_sql(source_db_sql[1], test_system_config['source_connection'])
        # exec_sql(source_db_sql[2], test_system_config['source_connection'])

        mapping = SorToEntityMapping('handeling_hstage', Handeling, self.pipe.sor)
        mapping.map_bk('id')
        mapping.map_field("naam", Handeling.Default.naam)
        mapping.map_field("datum::timestamp", Handeling.Default.datum)
        self.pipe.mappings.append(mapping)

        self.pipeline.run()

        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 2)
        self.assertEqual(get_row_count('dv.handeling_hub'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat_financieel'), 0)

    def test_run2(self):
        print('======================================================')
        print('===        R U N  2                                ===')
        print('======================================================')
        # # geen aanpassingen doen, aantallen moeten gelijk blijven

        mapping = SorToEntityMapping('handeling_hstage', Handeling, self.pipe.sor)
        mapping.map_bk('id')
        mapping.map_field("naam", Handeling.Default.naam)
        mapping.map_field("datum", Handeling.Default.datum)
        self.pipe.mappings.append(mapping)

        self.pipeline.run()

        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 2)
        self.assertEqual(get_row_count('dv.handeling_hub'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat_financieel'), 0)
        # revision = get_fields('sor_extra.handeling_hstage', [6], filter='_runid = 0.04')[0][0]
        # self.assertEqual(revision, 3)
        # self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 6)
        # revision = get_fields('sor_extra.handeling2_hstage', [6], filter='_runid = 0.04')[0][0]
        # self.assertEqual(revision, 3)

    def test_run3(self):
        print('======================================================')
        print('===        R U N  3                                ===')
        print('======================================================')
        # # sat financieel krijgt nu ook mapping en moet waarden gaan bevatten

        mapping = SorToEntityMapping('handeling_hstage', Handeling, self.pipe.sor)
        mapping.map_bk('id')
        mapping.map_field("naam", Handeling.Default.naam)
        mapping.map_field("datum::date", Handeling.Default.datum)
        mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
        self.pipe.mappings.append(mapping)

        self.pipeline.run()

        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 2)
        self.assertEqual(get_row_count('dv.handeling_hub'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat_financieel'), 2)

    def test_run4(self):
        print('======================================================')
        print('===        R U N  4                                ===')
        print('======================================================')
        # # update naam
        exec_sql(source_db_sql[2], test_system_config['source_connection'])

        mapping = SorToEntityMapping('handeling_hstage', Handeling, self.pipe.sor)
        mapping.map_bk('id')
        mapping.map_field("naam", Handeling.Default.naam)
        mapping.map_field("datum::date", Handeling.Default.datum)
        mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
        self.pipe.mappings.append(mapping)

        self.pipeline.run()

        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 3)
        self.assertEqual(get_row_count('dv.handeling_hub'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat'), 3)
        self.assertEqual(get_row_count('dv.handeling_sat_financieel'), 2)

        revision = get_fields('dv.handeling_sat', [6], filter='_runid = 0.04')[0][0]
        self.assertEqual(revision, 1)

        actual_row = get_fields('dv.handeling_view', fields=['default_naam'], filter="bk = '1'")[0]
        self.assertEqual(actual_row[0], 'OK1A')

    def test_run5(self):
        print('======================================================')
        print('===        R U N  5                                ===')
        print('======================================================')
        # # update naam
        exec_sql(source_db_sql[3], test_system_config['source_connection'])

        mapping = SorToEntityMapping('handeling_hstage', Handeling, self.pipe.sor)
        mapping.map_bk('id')
        mapping.map_field("naam", Handeling.Default.naam)
        mapping.map_field("datum::date", Handeling.Default.datum)
        mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
        self.pipe.mappings.append(mapping)

        self.pipeline.run()

        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 4)
        self.assertEqual(get_row_count('dv.handeling_hub'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat'), 4)
        self.assertEqual(get_row_count('dv.handeling_sat_financieel'), 2)

        revision = get_fields('dv.handeling_sat', [6], filter='_runid = 0.05')[0][0]
        self.assertEqual(revision, 2)

        actual_row = get_fields('dv.handeling_view', fields=['default_naam'], filter="bk = '1'")[0]
        self.assertEqual(actual_row[0], 'OK1B')

    def test_run6(self):
        print('======================================================')
        print('===        R U N  6                                ===')
        print('======================================================')
        # # naam terug naar oorspronkelijke waarde
        exec_sql(source_db_sql[4], test_system_config['source_connection'])

        mapping = SorToEntityMapping('handeling_hstage', Handeling, self.pipe.sor)
        mapping.map_bk('id')
        mapping.map_field("naam", Handeling.Default.naam)
        mapping.map_field("datum::date", Handeling.Default.datum)
        mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
        self.pipe.mappings.append(mapping)

        self.pipeline.run()

        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 5)
        self.assertEqual(get_row_count('dv.handeling_hub'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat'), 5)
        self.assertEqual(get_row_count('dv.handeling_sat_financieel'), 2)

        revision = get_fields('dv.handeling_sat', [6], filter='_runid = 0.06')[0][0]
        self.assertEqual(revision, 3)

        actual_row = get_fields('dv.handeling_view', fields=['default_naam'], filter="bk = '1'")[0]
        self.assertEqual(actual_row[0], 'OK1')

    def test_run7(self):
        print('======================================================')
        print('===        R U N  7                                ===')
        print('======================================================')
        # # naam terug naar oorspronkelijke waarde
        exec_sql(source_db_sql[4], test_system_config['source_connection'])

        mapping = SorToEntityMapping('handeling_hstage', Handeling, self.pipe.sor)
        mapping.map_bk('id')
        mapping.map_field("naam", Handeling.Default.naam)
        mapping.map_field("datum::date", Handeling.Default.datum)
        mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
        mapping.map_field("kosten::numeric+200", Handeling.Financieel.vraagprijs)
        mapping.map_field("(21.0)", Handeling.Financieel.btw)
        mapping.map_field(ConstantValue(9.98), Handeling.Financieel.korting)
        self.pipe.mappings.append(mapping)

        self.pipeline.run()

        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 5)
        self.assertEqual(get_row_count('dv.handeling_hub'), 2)
        self.assertEqual(get_row_count('dv.handeling_sat'), 5)
        self.assertEqual(get_row_count('dv.handeling_sat_financieel'), 4)

        revision = get_fields('dv.handeling_sat_financieel', fields=['_revision'], filter='_runid = 0.07')[0][0]
        self.assertEqual(revision, 1)

        actual_row = get_fields('dv.handeling_view', fields=['financieel_kostprijs', 'financieel_vraagprijs', 'financieel_btw', 'financieel_korting'], filter="bk = '2'")[0]

        self.assertEqual(actual_row[0], 500.0)
        self.assertEqual(actual_row[1], 700.0)
        self.assertEqual(actual_row[2], 21.0)
        self.assertEqual(str(actual_row[3] + 1), '10.98')


if __name__ == '__main__':
    unittest.main()
