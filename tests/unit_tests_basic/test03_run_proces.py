from tests.unit_tests_basic import _domainmodel
from tests.unit_tests_basic.global_test_suite import get_global_test_pipeline, exec_sql, test_system_config, init_db
from tests.unit_tests_basic._mappings import init_source_to_sor_mappings, init_sor_to_dv_mappings
from tests.unit_tests_basic.test02_ref_mappings import init_test_ref_mappings
from main import get_root_path

__author__ = 'hvreenen'

import unittest




class TestCase_RunProces(unittest.TestCase):
    is_init = False

    # def __init__(self, args):
    #     super().__init__(args)
    #     self.

    def setUp(self):
        if not TestCase_RunProces.is_init:
            print('init_db')
            init_db()
            TestCase_RunProces.is_init = True
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system', config=test_system_config)
        self.pipe.register_domain(_domainmodel)
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings())
        self.pipe.mappings.extend(init_sor_to_dv_mappings(self.pipe.sor))
        #todo: onstaande rgel in unittests weer aanzetten
        # self.pipe.mappings.extend(init_test_ref_mappings())

    def test_run1(self):
        self.pipeline.run()
        get_row_count(self, 'sor_test_system.patient_hstage', 4)
        get_row_count(self, 'dv.patient_hub', 4)
        get_row_count(self, 'dv.patient_sat', 4)
        get_row_count(self, 'dv.patient_sat_personalia', 4)
        get_row_count(self, 'dv.patient_sat_adres', 4)
        get_row_count(self, 'dv.patient_sat_inschrijving', 4)
        get_row_count(self, 'dv.patient_sat_contactgegevens', 8)
        get_row_count(self, 'dv.handeling_hub', 4)
        get_row_count(self, 'dv.hulpverlener_hub', 2)
        get_row_count(self, 'dv.patient_handeling_link', 9)

    def test_run1_her(self):
        self.pipeline.run()
        get_row_count(self, 'sor_test_system.patient_hstage', 4)
        get_row_count(self, 'dv.patient_hub', 4)
        get_row_count(self, 'dv.patient_sat', 4)
        get_row_count(self, 'dv.patient_sat_personalia', 4)
        get_row_count(self, 'dv.patient_sat_adres', 4)
        get_row_count(self, 'dv.patient_sat_inschrijving', 4)
        get_row_count(self, 'dv.patient_sat_contactgegevens', 8)
        get_row_count(self, 'dv.handeling_hub', 4)
        get_row_count(self, 'dv.hulpverlener_hub', 2)
        get_row_count(self, 'dv.patient_handeling_link', 9)

    def test_run2_from_source_to_sor(self):
        self.pipe.mappings[0].file_name = get_root_path() + '/PYELT/tests/data/patienten2.csv'
        self.pipeline.run()
        get_row_count(self, 'sor_test_system.patient_hstage', 7)
        get_row_count(self, 'dv.patient_hub', 5)
        get_row_count(self, 'dv.patient_sat', 5)
        get_row_count(self, 'dv.patient_sat_personalia', 5)
        get_row_count(self, 'dv.patient_sat_adres', 7)
        get_row_count(self, 'dv.patient_sat_inschrijving', 6)
        get_row_count(self, 'dv.patient_sat_contactgegevens', 11)




    #
    # # ------------------------
    # # trajecten
    #
    # file_name_trajecten = get_root_path() + '/tests/data/zorgtrajecten{}.csv'.format(runid)
    # source_file_trajecten = CsvSourceFile('trajecten.csv',file_name_trajecten, header=False)
    # source_file_trajecten.columns[0].type = int
    # hstage_table_trajecten = pipe.sor.zorgtraject_hstage
    #
    # source_file_trajecten.load()
    # hstage_table_trajecten.look_for_changes(source_file_trajecten, runid, pipe.source_system)


# def test_etl2_from_sor_to_dv(pipe):
#     # pipe = unittest.pipeline.pipes['PatientenBron']
#     runid = pipe.runid
#     for mapping in pipe.etl2_mappings:
#         if isinstance(mapping.target, Entity):
#             entity = mapping.target
#             sor_table = mapping.source
#             entity.look_for_changes(sor_table, runid, pipe.source_system)
#             # mapping.source.transfer_to(runid, 'PatientenBron')
#     for mapping in pipe.etl2_mappings:
#         if isinstance(mapping.target, Link):
#             link = mapping.target
#             sor_table = mapping.source
#             link.look_for_changes(sor_table, runid, pipe.source_system)
#     # pipe.dv.save_all_to_csv()
#     # pipe.dv.import_all_csv_in_db()


def get_row_count(unittest, table_name, count):
    test_sql = "SELECT * FROM " + table_name
    result = exec_sql(test_sql)
    unittest.assertEqual(len(result), count, table_name)



# class TestCaseEtlProces(unittest.TestCase):
#     def setUp(self):
#         self.pipeline = get_global_test_pipeline()
#         pipe = self.pipeline.pipes['PatientenBron']
#         pipe.etl1_proces = test_etl1_from_source_to_sor
#         pipe.etl2_proces = test_etl2_from_sor_to_dv
#         self.pipe = pipe
#
#
#     def test_etl(self):
#         pipe = self.pipe
#         pipe.runid = 1
#         pipe.run()
#         # runid = 1
#         # test_etl1_from_source_to_sor(runid)
#         test_count(self, 'sor.patient_hstage', 4)
#         test_count(self, 'sor.zorgtraject_hstage', 4)
#         # test_etl2_from_sor_to_dv(runid)
#         test_count(self, 'dv.patient_hub', 4)
#         test_count(self,'dv.zorgtraject_hub', 4)
#         test_count(self,'dv.patient_zorgtraject_link', 4)
#
#         return
#         # tweede keer met zelfde file. resultaat moet ongewijzigd blijven
#         pipe.run()
#         # test_etl1_from_source_to_sor(runid)
#         test_count(self, 'sor.patient_hstage', 4)
#         test_count(self, 'sor.zorgtraject_hstage', 4)
#         # test_etl2_from_sor_to_dv(runid)
#         test_count(self, 'dv.patient_hub', 4)
#         test_count(self, 'dv.zorgtraject_hub', 4)
#         test_count(self, 'dv.patient_zorgtraject_link', 4)
#
#
#         # ###################################
#         pipe.runid = 2
#         pipe.run()
#         # runid = 2
#         # test_etl1_from_source_to_sor(runid)
#         test_count(self, 'sor.patient_hstage', 9)
#         test_count(self, 'sor.zorgtraject_hstage', 7)
#         # test_etl2_from_sor_to_dv(runid)
#         test_count(self, 'dv.patient_hub', 5)
#         test_count(self, 'dv.zorgtraject_hub', 5)
#         test_count(self, 'dv.patient_zorgtraject_link', 5)
#
#         # runid = 3
#         # test_etl1_from_source_to_sor(runid)
#         pipe.runid = 3
#         pipe.run()
#         test_count(self, 'sor.patient_hstage', 11)
#         test_count(self, 'sor.zorgtraject_hstage', 9)
#         # test_etl2_from_sor_to_dv(runid)
#         test_count(self, 'dv.patient_hub', 7)
#         test_count(self, 'dv.zorgtraject_hub', 6)
#         test_count(self, 'dv.patient_zorgtraject_link', 6)



if __name__ == '__main__':
    # init_db()
    unittest.main()
