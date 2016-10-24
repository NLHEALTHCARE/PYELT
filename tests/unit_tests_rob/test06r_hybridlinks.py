import unittest

from tests.unit_tests_rob import _domain_rob
from tests.unit_tests_rob.global_test_suite import test_system_config, get_global_test_pipeline, execute_sql, init_db
from tests.unit_tests_rob.mappings_rob_hybridlinks import init_source_to_sor_mappings, init_sor_to_dv_mappings
from tests.unit_tests_rob.test05r_process import test_row_count, get_field_value_from_table, \
    get_field_value_from_dv_table
from main import get_root_path


class TestCase_RunProces(unittest.TestCase):
    print('init_db:\n')
    init_db()

    def setUp(self):
        print("setup:")
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system', config=test_system_config)
        self.pipe.register_domain(_domain_rob)
        # hj: onderstaande regel toegevoegd. Bij elke initiatie dient mappings met lege lijst te beginnen
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings())
        pipe = self.pipe
        self.pipe.mappings.extend(init_sor_to_dv_mappings(pipe))

    def test01_pipeline_run(self):

        print("test_run1:\n")
        self.pipeline.run()

        test_row_count(self, 'sor_test_system.zorgverlener_hstage', 4)
        test_row_count(self, 'dv.zorgverlener_hub', 4)
        test_row_count(self, 'dv.zorgverlener_sat', 4)
        test_row_count(self, 'dv.zorgverlener_sat_personalia', 4)
        test_row_count(self, 'dv.zorgverlener_adres_link', 12)
        test_row_count(self, 'dv.adres_sat', 12)

    def test02_pipeline_rerun(self):
        print("test_run1_her:\n")
        self.pipeline.run()

        test_row_count(self, 'sor_test_system.zorgverlener_hstage', 4)
        test_row_count(self, 'dv.zorgverlener_hub', 4)
        test_row_count(self, 'dv.zorgverlener_sat', 4)
        test_row_count(self, 'dv.zorgverlener_sat_personalia', 4)
        test_row_count(self, 'dv.zorgverlener_adres_link', 12)
        test_row_count(self, 'dv.adres_sat', 12)

    def test03_dv_updates(self):

        self.pipe.mappings[0].file_name = get_root_path() + '/PYELT/tests/data/zorgverlenersB_rob.csv'
        self.pipeline.run()

        test_row_count(self, 'sor_test_system.zorgverlener_hstage', 7)
        test_row_count(self, 'dv.zorgverlener_hub', 4)
        test_row_count(self, 'dv.zorgverlener_sat', 4)
        test_row_count(self, 'dv.zorgverlener_sat_personalia', 4)
        # test_row_count(self, 'dv.zorgverlener_adres_link', 13)
        # test_row_count(self, 'dv.adres_sat', 15)

    def test04_update_Null_date(self):

        result = get_field_value_from_table('einddatum', 'pyelt_unittests.dv.zorgverlener_sat', """zorgverlenernummer = '567' and _active = TRUE""")
        result = result[0][0].strftime('%Y/%m/%d')
        print(result)
        self.assertEqual(result,'2003/03/03','ik verwachtte dat deze eind-datum van Null aangepast zou worden naar 2003-03-03')

    def test05_update_dv_field(self):
        # unittest voor waarde naar waarde
        #         result = get_field_value_from_dv_table('landcode','zorgverlener','contactgegevens','455',['_active = True', """type = 'mobiel'"""])
        sql = """select a.land from pyelt_unittests.dv.adres_sat a
                 inner join pyelt_unittests.dv.zorgverlener_adres_link za
                 on a._id = za.fk_adres_hub
                 inner join pyelt_unittests.dv.zorgverlener_hub z
                 on za.fk_zorgverlener_hub = z._id
                 where z.bk = '455' and za.type = 'bezoek' and a._active = True"""
        result = execute_sql(sql)

        result = result[0][0]
        self.assertEqual(result,'Australia','ik verwachte dat deze waarde van Nederland naar Australia aangepast was')

    def test06_update_dv_field_null_to_value(self):
        sql = """select a.straat from pyelt_unittests.dv.adres_sat a
                 inner join pyelt_unittests.dv.zorgverlener_adres_link za
                 on a._id = za.fk_adres_hub
                 inner join pyelt_unittests.dv.zorgverlener_hub z
                 on za.fk_zorgverlener_hub = z._id
                 where z.bk = '448' and za.type = 'bezoek' and a._active = True"""
        result = execute_sql(sql)

        result = result[0][0]
        self.assertEqual(result,'Dalstraat','ik verwachte dat deze waarde van Null naar Daalstraat aangepast was')

    def test07_update_dv_field_value_to_null(self):
        # print("test07_update_dv_field_value_to_null:")
        sql = """select a._active from pyelt_unittests.dv.adres_sat a
                 inner join pyelt_unittests.dv.zorgverlener_adres_link za
                 on a._id = za.fk_adres_hub
                 inner join pyelt_unittests.dv.zorgverlener_hub z
                 on za.fk_zorgverlener_hub = z._id
                 where z.bk = '567' and za.type = 'bezoek' and a.straat = 'Beaufort'"""
        result = execute_sql(sql)

        result = result[0][0]
        print(result)
        self.assertFalse('Ik verwachte dat deze waarde False zou zijn (de gezochte straatnaam verwijst naar niet langer geldende adresgegevens voor dit bezoekadres; in sor tabel als lege velden)')


if __name__ == '__main__':
    unittest.main()


