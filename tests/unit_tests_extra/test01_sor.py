import unittest

from tests.unit_tests_extra._configs import *
from tests.unit_tests_extra._globals import *

from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.databases import SourceQuery, SourceTable

# DONE: veld  _source_system is leeg in sor...

# DONE: tellingen automatiseren

# DONE: SoureQueries ook met md5?

source_db_sql = [
    """DELETE FROM handelingen""",
    """INSERT INTO handelingen(
            id, naam, datum, kosten, fk_org, fk_spec)
    VALUES (1, 'ok', '2016-8-18 10:25:00', 123.99, 1, 1),
    (2, 'ok', '2016-8-18 10:26:00', 500, 3, 3),
    (3, 'ok heup', '2016-8-18 11:25:00', 123.99, 2, 2)""",

    """UPDATE handelingen SET naam = 'ok2' where id = 1""",
    """UPDATE handelingen SET naam = 'ok3' where id = 1""",
    """UPDATE handelingen SET naam = 'ok' where id = 1""",
    """INSERT INTO handelingen(
                id, naam, datum, kosten, fk_org, fk_spec)
        VALUES (4, 'ok4', '2016-8-19 10:25:00', 123.99, 1, 1),
        (5, 'ok5', '2016-8-19 10:26:00', 500, 3, 3)""",
    """UPDATE handelingen SET naam = 'ok4' where id = 4""",
    """UPDATE handelingen SET fk_org = 2 where id = 4"""]


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
""", 'view_2')
    source_qry.set_primary_key(['id'])
    sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)
    mappings.append(sor_mapping)

    source_tbl = SourceTable('handelingen', source_db.default_schema, source_db)
    source_tbl.set_primary_key(['id'])
    sor_mapping = SourceToSorMapping(source_tbl, 'handeling2_hstage', auto_map=True, ignore_fields=['fk_org'])
    mappings.append(sor_mapping)

    source_tbl = SourceTable('handelingen', source_db.default_schema, source_db)
    source_tbl.set_primary_key(['id'])
    sor_mapping = SourceToSorMapping(source_tbl, 'handeling_met_filter_hstage', auto_map=True, filter='kosten > 125')
    mappings.append(sor_mapping)

    return mappings


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
        self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 3)
        self.assertEqual(get_row_count('sor_extra.handeling_met_filter_hstage'), 1)

    def test_run2(self):
        print('======================================================')
        print('===        R U N  2                                ===')
        print('======================================================')
        # wijzig 1 waarde in bron tabel
        exec_sql(source_db_sql[2], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 4)
        self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 4)
        self.assertEqual(get_row_count('sor_extra.handeling_met_filter_hstage'), 1)

    def test_run3(self):
        print('======================================================')
        print('===        R U N  3                                ===')
        print('======================================================')
        # wijzig 1 waarde in bron tabel
        exec_sql(source_db_sql[3], test_system_config['source_connection'])

        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 5)
        self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 5)

    def test_run4(self):
        print('======================================================')
        print('===        R U N  4                                ===')
        print('======================================================')
        # wijzig waarde in bron tabel weer terug naar oorspronkelijke waarde
        exec_sql(source_db_sql[4], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 6)
        fields = get_fields('sor_extra.handeling_hstage', [7], filter='_runid = 1.03')
        revision = get_fields('sor_extra.handeling_hstage', [7], filter='_runid = 1.03')[0][0]
        self.assertEqual(revision, 3)
        self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 6)
        revision = get_fields('sor_extra.handeling2_hstage', [7], filter='_runid = 1.03')[0][0]
        self.assertEqual(revision, 3)

    def test_run5(self):
        print('======================================================')
        print('===        R U N  5                                ===')
        print('======================================================')
        # geen wijzigingen
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 6)
        self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 6)

    def test_run6(self):
        print('======================================================')
        print('===        R U N  6                                ===')
        print('======================================================')
        # twee nieuwe
        exec_sql(source_db_sql[5], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 8)
        revision = get_fields('sor_extra.handeling_hstage', [7], filter='_runid = 1.05')[0][0]
        self.assertEqual(revision, 0)
        self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 8)
        revision = get_fields('sor_extra.handeling2_hstage', [7], filter='_runid = 1.05')[0][0]
        self.assertEqual(revision, 0)
        self.assertEqual(get_row_count('sor_extra.handeling_met_filter_hstage'), 2)

    def test_run7(self):
        print('======================================================')
        print('===        R U N  7                                ===')
        print('======================================================')
        # update zonder wijziging
        exec_sql(source_db_sql[6], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 8)
        self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 8)

    def test_run8(self):
        print('======================================================')
        print('===        R U N  8                                ===')
        print('======================================================')
        # update fk_org
        exec_sql(source_db_sql[7], test_system_config['source_connection'])
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 9)
        # deze bevat ignore_fields
        self.assertEqual(get_row_count('sor_extra.handeling2_hstage'), 8)




if __name__ == '__main__':
    unittest.main()
