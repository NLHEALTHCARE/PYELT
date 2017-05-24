import unittest

from tests.unit_tests_extra import _domeinmodel
from tests.unit_tests_extra._configs import test_system_config
from tests.unit_tests_extra._globals import *
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.mappings.validations import SorValidation
from pyelt.sources.databases import SourceQuery, SourceTable

# DONE: veld  _source_system is leeg in sor...

# DONE: tellingen automatiseren

# DONE: SoureQueries ook met md5?

source_db_sql = [
    """DELETE FROM handelingen""",
    """INSERT INTO handelingen(
            id, naam, soort, sleutel1, sleutel2, datum, kosten, fk_org, fk_spec)
    VALUES (1, 'ok1', 'ok', 1, null, '2016-8-18 10:25:00', 123.99, 1, 1),
    (2, 'ok2','ok', 1, null,  '2016-8-18 10:26:00', 500, 3, 3),
    (3, 'ok3 heup', 'ok', Null, null, '2016-8-18 11:25:00', 0, 2, 2)""",

    """UPDATE handelingen SET kosten = '-123' where id = 1""",
    """UPDATE handelingen SET kosten = NULL where id = 1""",
    """UPDATE handelingen SET kosten = '0.99' where id = 1""",
    """INSERT INTO handelingen(
                id, naam, datum, kosten, fk_org, fk_spec)
        VALUES (4, 'ok4', '2016-8-19 10:25:00', 123.99, 1, 1),
        (5, 'ok4', '2016-8-19 10:26:00', 500, 3, 3)""",
    """UPDATE handelingen SET naam = 'ok4' where id = 4""", ]


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
  specialisaties.naam as spec,
  specialisaties.fk_hoofd,
  handelingen.fk_org + 1 as fk_plus,
  handelingen.fk_spec
FROM
  public.handelingen,
  public.organisaties,
  public.specialisaties
WHERE
  organisaties.id = handelingen.fk_org AND
  specialisaties.id = handelingen.fk_spec;
""", 'view_1')
    source_qry.set_primary_key(['naam'])
    sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)
    mappings.append(sor_mapping)

    return mappings


def init_validations(pipe):
    validations = []
    sor = pipe.sor
    validation = SorValidation(tbl='handeling_hstage', schema=sor)
    validation.msg = 'Kosten mogen niet leeg of negatief zijn'
    validation.sql_condition = 'coalesce(kosten::numeric, 0.0) <= 0'
    validations.append(validation)

    # validation = SorValidation(tbl='handeling_hstage', schema=sor)
    # validation.msg = 'duplicate key error'
    # validation.set_check_for_duplicate_keys(['naam'])
    # validations.append(validation)
    return validations


class TestCase_RunSor(unittest.TestCase):
    is_init = False
    """Testen met databse als bron"""

    def setUp(self):
        if not TestCase_RunSor.is_init:
            print('init_db')
            init_db()
            TestCase_RunSor.is_init = True
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system', config=test_system_config)
        # self.pipe.register_domain(_domeinmodel)
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings(self.pipe))
        self.pipe.validations = []
        self.pipe.validations.extend(init_validations(self.pipe))

    def test_run1(self):
        print('======================================================')
        print('===        R U N  1                                ===')
        print('======================================================')
        # maak tabellen in bron leeg
        exec_sql(source_db_sql[0], test_system_config['source_connection'])
        # vul bron tabellen met initiele waarden
        exec_sql(source_db_sql[1], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 3)
        self.assertEqual(get_row_count('sor_extra.handeling_hstage', '_valid = False'), 1)
        msg = get_fields('sor_extra.handeling_hstage', [9], filter="id = '3'")[0][0]
        self.assertEqual('Kosten mogen niet leeg of negatief zijn; ', msg)

    def test_run2(self):
        print('======================================================')
        print('===        R U N  2                                ===')
        print('======================================================')
        # update kosten
        exec_sql(source_db_sql[2], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 4)
        self.assertEqual(get_row_count('sor_extra.handeling_hstage', '_valid = False'), 2)
        msg = get_fields('sor_extra.handeling_hstage', [9], filter="id = '3'")[0][0]
        self.assertEqual('Kosten mogen niet leeg of negatief zijn; ', msg)

    def test_run3(self):
        print('======================================================')
        print('===        R U N  3                                ===')
        print('======================================================')
        # update kosten
        exec_sql(source_db_sql[3], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 5)
        self.assertEqual(get_row_count('sor_extra.handeling_hstage', '_valid = False'), 3)

    def test_run4(self):
        print('======================================================')
        print('===        R U N  4                                ===')
        print('======================================================')

        # update kosten
        exec_sql(source_db_sql[4], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 6)
        self.assertEqual(get_row_count('sor_extra.handeling_hstage', '_valid = False'), 3)

    def test_run5(self):
        print('======================================================')
        print('===        R U N  5                                ===')
        print('======================================================')

        # insert twee duplicate keys: aantal moet 3 blijven
        exec_sql(source_db_sql[5], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 8)
        self.assertEqual(get_row_count('sor_extra.handeling_hstage', '_valid = True'), 3)

if __name__ == '__main__':
    unittest.main()
