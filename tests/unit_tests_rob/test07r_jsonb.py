import unittest

from psycopg2.extras import DictCursor
from sqlalchemy import create_engine, text

from pyelt.pipeline import Pipeline
from tests.unit_tests_rob.global_test_suite import test_system_config, get_global_test_pipeline, init_db
from tests.unit_tests_rob.test_mappings import init_source_to_sor_mappings, init_sor_to_dv_mappings
from main import get_root_path
from tests.unit_tests_rob import test_domain
from tests.unit_tests_rob.test_configs import test_config, jsontest_config


class TestCase_RunProces(unittest.TestCase):
    print('init_db:\n')
    # init_db()
    engine = create_engine(test_config['conn_dwh'])

    sql = """
    DROP SCHEMA IF EXISTS sor CASCADE;
    DROP SCHEMA IF EXISTS sor_test CASCADE;
    DROP SCHEMA IF EXISTS rdv CASCADE;
    DROP SCHEMA IF EXISTS dv CASCADE;
    DROP SCHEMA IF EXISTS sys CASCADE;

    """

    query = text(sql)
    engine.execute(query)


    def setUp(self):
        print("setup:")

        from tests.unit_tests_rob.test_configs import test_config, jsontest_config
        self.pipeline = Pipeline(test_config)
        self.pipe = self.pipeline.get_or_create_pipe('sor_test', config=jsontest_config)
        self.pipe.register_domain(test_domain)
        # hj: onderstaande regel toegevoegd. Bij elke initiatie dient mappings met lege lijst te beginnen
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings(self.pipe))
        self.pipe.mappings.extend(init_sor_to_dv_mappings(self.pipe))

    def test01_pipeline_run(self):

        print("test_run1:\n")
        self.pipeline.run()

        test_row_count(self, 'sor_test.patient_hstage', 1)
        test_row_count(self, 'dv.patient_hub', 1)
        test_row_count(self, 'dv.patient_sat', 1)

    def test01b_pipeline_run(self):

        print("test_run1_her:\n")
        self.pipeline.run()
        self.pipeline.run()
        self.pipeline.run()
        self.pipeline.run()
        self.pipeline.run()
        self.pipeline.run()
        self.pipeline.run()

        """ meerdere malen de pipeline gerund, want het leek erop dat de jsonb velden "extra" en "extra" random
            gevuld werden met de twee gedefineerde jsonb objecten, dat wordt opgemerkt door vaker te runnen"""

        test_row_count(self, 'sor_test.patient_hstage', 1)
        test_row_count(self, 'dv.patient_hub', 1)
        test_row_count(self, 'dv.patient_sat', 1)


def test02_dv_update(self):
        # return
        print("test_run2:\n")
        path = self.pipe.jsontest_config['data_path']
        self.pipe.mappings[0].file_name = path + '/test2.csv'


        self.pipeline.run()

        test_row_count(self, 'sor_test.patient_hstage', 2)
        test_row_count(self, 'dv.patient_hub', 2)
        test_row_count(self, 'dv.patient_sat', 2)




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

def execute_sql(sql):
    # conn = psycopg2.connect("""host='localhost' dbname='pyelt_unittests' user='postgres' password='{}'""".format(get_your_password()))
    engine = create_engine(test_config['conn_dwh'])  # je hebt hier een andere config nodig dan die in de global_test_suite staat!
    conn = engine.raw_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute(sql)
    result = cursor.fetchall()
    conn.commit()
    cursor.close()

    return result





if __name__ == '__main__':
    unittest.main()
