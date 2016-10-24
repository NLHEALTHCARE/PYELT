import unittest

from tests.unit_tests_extra._configs import test_system_config
from tests.unit_tests_extra._globals import *
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.databases import SourceQuery, SourceTable

source_db_sql = [
    """DELETE FROM handelingen""",
    """INSERT INTO handelingen(
            id, naam, datum, kosten, fk_org, fk_spec)
    VALUES (1, 'ok', '2016-8-18 10:25:00', 123.99, 1, 1),
    (2, 'ok', '2016-8-18 10:26:00', 500, 3, 3),
    (3, NULL , '2016-8-18 11:25:00', 123.99, 2, 2)""",

    """UPDATE handelingen SET naam = 'ok2' where id = 1""",
    """UPDATE handelingen SET naam = 'ok3' where id = 1""",
    """UPDATE handelingen SET naam = 'ok' where id = 1""",
    """INSERT INTO handelingen(
                id, naam, datum, kosten, fk_org, fk_spec)
        VALUES (4, 'ok4', '2016-8-19 10:25:00', 123.99, 1, 1),
        (5, 'ok5', '2016-8-19 10:26:00', 500, 3, 3)""",
    """UPDATE handelingen SET naam = 'ok4' where id = 4""", ]


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

    def test_run1(self):
        print('======================================================')
        print('===        R U N  1  test automap                  ===')
        print('======================================================')
        # maak tabellen in bron leeg
        exec_sql(source_db_sql[0], test_system_config['source_connection'])
        # vul bron tabellen met initiele waarden
        exec_sql(source_db_sql[1], test_system_config['source_connection'])

        # mappings
        source_db = self.pipe.source_db
        source_tbl = SourceTable('handelingen', source_db.default_schema, source_db, alias='handelingen2')
        source_tbl.set_primary_key(['id'])
        sor_mapping = SourceToSorMapping(source_tbl, 'handeling_no_auto_map_hstage', auto_map=False, filter='kosten > 125')
        sor_mapping.map_field('id', 'id')
        sor_mapping.map_field('naam', 'naam2')
        self.pipe.mappings.append(sor_mapping)

        # run
        self.pipeline.run()
        for pipe in self.pipeline.pipes.values():
            validation_msg = pipe.validate_mappings_before_ddl()
            validation_msg += pipe.validate_mappings_after_ddl()
        self.assertIn('Mapping <red>handelingen -> handeling_no_auto_map_hstage</> is niet geldig. Auto_map moet aan staan bij SourceToSorMapping.', validation_msg)

    def test_run2(self):
        print('======================================================')
        print('===        R U N  1  test no key                   ===')
        print('======================================================')
        # maak tabellen in bron leeg
        exec_sql(source_db_sql[0], test_system_config['source_connection'])
        # vul bron tabellen met initiele waarden
        exec_sql(source_db_sql[1], test_system_config['source_connection'])

        # mappings
        source_db = self.pipe.source_db
        source_tbl = SourceTable('handelingen', source_db.default_schema, source_db, alias='handelingen2')
        source_tbl.set_primary_key([''])
        sor_mapping = SourceToSorMapping(source_tbl, 'handeling_no_key_hstage', filter='kosten > 125')
        self.pipe.mappings.append(sor_mapping)

        # run
        self.pipeline.run()
        for pipe in self.pipeline.pipes.values():
            validation_msg = pipe.validate_mappings_before_ddl()
            validation_msg += pipe.validate_mappings_after_ddl()
        self.assertIn('Mapping <red>handelingen -> handeling_no_key_hstage</> is niet geldig. Geen geldige key opgegeven.', validation_msg)

    def test_run3(self):
        print('======================================================')
        print('===        R U N  3a  alter sor_tbl                 ===')
        print('======================================================')

        source_db = self.pipe.source_db
        self.pipe.mappings = []
        source_qry = SourceQuery(source_db, """
SELECT
  *
FROM
  public.handelingen
""", 'view_1')
        source_qry.set_primary_key(['id'])
        sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)

        self.pipe.mappings.append(sor_mapping)

        # run
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 3)

    def test_run4(self):
        print('======================================================')
        print('===        R U N  3b  alter sor_tbl                 ===')
        print('======================================================')

        self.pipe.mappings = []
        # bron uitbreiden met nieuw veld, mappen op dezelfde sor tabel
        source_db = self.pipe.source_db
        source_qry = SourceQuery(source_db, """
SELECT
  *, 'extra' as extra_veld
FROM
  public.handelingen
""", 'view_1')
        source_qry.set_primary_key(['id'])
        sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)
        self.pipe.mappings.append(sor_mapping)

        # run
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 6)

    def test_run5(self):
        print('======================================================')
        print('===        R U N  5 waardes wijzen van iets naar NULL                ===')
        print('======================================================')

        self.pipe.mappings = []
        # bron uitbreiden met nieuw veld, mappen op dezelfde sor tabel
        source_db = self.pipe.source_db
        source_qry = SourceQuery(source_db, """
SELECT
  *,  NULL as extra_veld
FROM
  public.handelingen
""", 'view_1')
        source_qry.set_primary_key(['id'])
        sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)
        self.pipe.mappings.append(sor_mapping)

        # run
        self.pipeline.run()
        self.assertEqual(get_row_count('sor_extra.handeling_hstage'), 9)

#     def test_run6(self):
#         print('======================================================')
#         print('===        R U N  6 twee tabellen mappen opzelfde sor                ===')
#         print('======================================================')
#
#         self.pipe.mappings = []
#         # bron uitbreiden met nieuw veld, mappen op dezelfde sor tabel
#         source_db = self.pipe.source_db
#         source_qry = SourceQuery(source_db, """
# SELECT
#   *,  NULL as extra_veld
# FROM
#   public.handelingen
# """, 'view_1')
#         source_qry.set_primary_key(['id'])
#         sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)
#         self.pipe.mappings.append(sor_mapping)
#
#         source_qry = SourceQuery(source_db, """
#         SELECT
#           *
#         FROM
#           public.organisaties
#         """, 'view_2')
#         source_qry.set_primary_key(['id'])
#         sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)
#         self.pipe.mappings.append(sor_mapping)
#         # run
#         self.pipeline.run()
#         for pipe in self.pipeline.pipes.values():
#             validation_msg = pipe.validate_mappings_before_ddl()
#             validation_msg += pipe.validate_mappings_after_ddl()
#         self.assertIn('SorMappings zijn niet geldig. Er wordt meer keer gemapt op dezelfde doel tabel.', validation_msg)


if __name__ == '__main__':
    unittest.main()
