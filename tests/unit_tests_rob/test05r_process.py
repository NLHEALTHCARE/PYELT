import unittest

from tests.unit_tests_rob import _domain_rob
from tests.unit_tests_rob._domain_rob import Zorgverlener
from tests.unit_tests_rob.global_test_suite import test_system_config, get_global_test_pipeline, execute_sql, init_db
from tests.unit_tests_rob._mappings_rob import init_source_to_sor_mappings, init_sor_to_dv_mappings
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
        self.pipe.mappings.extend(init_sor_to_dv_mappings(self.pipe))

    def test01_pipeline_run(self):

        print("test_run1:\n")
        self.pipeline.run()

        test_row_count(self, 'sor_test_system.zorgverlener_hstage', 4)
        test_row_count(self, 'dv.zorgverlener_hub', 4)
        test_row_count(self, 'dv.zorgverlener_sat', 4)
        test_row_count(self, 'dv.zorgverlener_sat_personalia', 4)
        test_row_count(self, 'dv.zorgverlener_adres_link', 4)

    def test02_pipeline_rerun(self):
        print("test_run1_her:\n")
        self.pipeline.run()

        test_row_count(self, 'sor_test_system.zorgverlener_hstage', 4)
        test_row_count(self, 'dv.zorgverlener_hub', 4)
        test_row_count(self, 'dv.zorgverlener_sat', 4)
        test_row_count(self, 'dv.zorgverlener_sat_personalia', 4)
        test_row_count(self, 'dv.zorgverlener_adres_link', 4)

    def test03_dv_update(self):
        # return
        print("test_run2:\n")
        self.pipe.mappings[0].file_name = get_root_path() + '/PYELT/tests/data/zorgverleners2_rob.csv'
        self.pipe.mappings[0].file_name = get_root_path() + '/PYELT/tests/data/zorgverleners2_rob.csv'


        self.pipeline.run()

        test_row_count(self, 'sor_test_system.zorgverlener_hstage', 8)
        test_row_count(self, 'dv.zorgverlener_hub', 4)
        test_row_count(self, 'dv.zorgverlener_sat', 5)
        test_row_count(self, 'dv.zorgverlener_sat_personalia', 5)
        test_row_count(self, 'dv.zorgverlener_adres_link', 5)

    def test04_update_Null_date(self):
        print("test_run2a:\n")
        result = get_field_value_from_table('einddatum', 'pyelt_unittests.dv.zorgverlener_sat', """zorgverlenernummer = '567' and _active = TRUE""")
        result = result[0][0].strftime('%Y/%m/%d')
        print(result)
        self.assertEqual(result,'2003/03/03','ik verwachtte dat deze eind-datum van Null aangepast zou worden naar 2003-03-03')

    def test05_update_dv_field(self):
        # unittest voor waarde naar waarde
        result = get_field_value_from_dv_table('landcode', 'zorgverlener', 'contactgegevens', '455', ['_active = True', """type = 'mobiel'"""])
        result = result[0][0]
        self.assertEqual(result,'+61','ik verwachte dat deze waarde van +31 naar +61 aangepast was')




    def test06_invalid_data_entry(self):
        # huisnummer niet als getal, maar als letter ingevoerd:
        print("test_run3a:\n")


        expected_error = None
        try:
            self.pipe.mappings[0].file_name = get_root_path() + '/tests/data/zorgverleners3_rob.csv'
            self.pipeline.run()

        except Exception as err:
            expected_error = err

        self.assertIsNotNone(expected_error,'ik verwachte een error')

# todo: unittest maken voor testen van een update van een gevuld veld dat na de update een " Null" value bevat
#     def test07_value_to_null(self):
#         result = get_field_value_from_dv_table('datum', 'zorgverlener', 'contactgegevens', '567', ["""type ='mobiel2'""", """_active = True"""])
#
#         result2 = result[0][0]
#         print(result2)
#
#         expected_error = ''
#         if result2 == None:
#             expected_error = 'Null Value'
#
#
#         self.assertEqual(expected_error,'Null Value', 'ik verwachte een null waarde')



    def test08_postcode_correct(self):
        print("test_run4a:\n")
        self.pipe.mappings[0].file_name = get_root_path() + '/PYELT/tests/data/zorgverleners4_rob.csv'
        self.pipeline.run()
        result = get_field_value_from_table('postcode','pyelt_unittests.dv.adres_sat', """char_length(postcode) >7""")
# detecteer dat er geen strings zijn met een te lange postcode
        self.assertTrue(len(result) == 0)
# gewenst postcode format: "1111AA" of "1111 AA?

    def test09_null_and_hybridsat_update(self):
# deze unittest test 2 dingen: wordt een Null veld geupdate en wordt een hybride_sat geupdate
        print("test_run5:\n")
        self.pipe.mappings[0].file_name = get_root_path() + '/PYELT/tests/data/zorgverleners4_rob.csv'
        self.pipeline.run()

        result = get_field_value_from_dv_table('telnummer', 'zorgverlener', 'contactgegevens', '448', ["""type = 'mobiel2'""", """_active = True"""])

        if len(result)>0:
            result= result[0][0]
        else:
            result = None
        self.assertIsNotNone(result,'Ik verwacht dat een verandering in een oorspronkelijk Null veld wel in de DV laag terecht zou komen; bestaat de gebruikte bk wel?')



    def test10_dv_view_updated(self):
        pass

#todo[rob]: test de hybrid-links


def test_row_count(unittest, table_name, count):
    test_sql = "SELECT * FROM " + table_name
    result = execute_sql(test_sql)
    unittest.assertEqual(len(result), count, table_name)

def get_field_value_from_table(fieldname, table_name, sql_condition):
    sql = """select {0} from {1} where {2}""".format(fieldname, table_name, sql_condition)
    result = execute_sql(sql)
    return result

def get_field_value_from_dv_table(fieldname, entity_name, sat_name, bk, sql_conditions_list):
    if sat_name != '':
        sat_name2 = '_{}'.format(sat_name)
    else:
        sat_name2 = ''

    if sql_conditions_list == []:
        sql_condition = ''
    else:
        sql_condition = ''
        for i in sql_conditions_list:
            sql_condition = sql_condition + ' AND s.{}'.format(i)

    sql = """select s.{1}
             from {0}{2}_sat{3} s
             inner join {0}{2}_hub h on s._id = h._id
             where h.bk = '{4}'{5}""".format('pyelt_unittests.dv.', fieldname, entity_name, sat_name2, bk, sql_condition)
    result = execute_sql(sql)

    return result



if __name__ == '__main__':
    unittest.main()
