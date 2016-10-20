# from domainmodel_fhir import test_domain
# from domainmodel_fhir.test_mappings import init_source_to_sor_mappings, init_sor_to_dv_mappings
import unittest

from sqlalchemy import create_engine, text

from pyelt.pipeline import Pipeline
from tests.unit_tests_rob import test_domain
from tests.unit_tests_rob.test05r_process import test_row_count
from tests.unit_tests_rob.test_mappings import init_source_to_sor_mappings, init_sor_to_dv_mappings

""" voor testen van jsonb"""


def test01_pipeline_run(self):
    print("test_run1:\n")
    test_main()
    self.pipeline.run()


    test_row_count(self, 'sor_test_patient_hstage', 2)





def define_test_pipe(pipeline, test_config):
    pipe = pipeline.get_or_create_pipe('sor_test', test_config)
    pipe.register_domain(test_domain)

    mappings = init_source_to_sor_mappings(pipe)
    pipe.mappings.extend(mappings)


    pipe.mappings.extend(init_sor_to_dv_mappings(pipe))


def test_main(*args):
    from tests.unit_tests_rob.test_configs import test_config, jsontest_config
    pipeline = Pipeline(test_config)

    define_test_pipe(pipeline, jsontest_config)

    pipeline.run()

    def init2_db():
        engine = create_engine(test_config['conn_dwh'])

        sql = """
        DROP SCHEMA IF EXISTS sor CASCADE;
        DROP SCHEMA IF EXISTS sor_test_system CASCADE;
        DROP SCHEMA IF EXISTS rdv CASCADE;
        DROP SCHEMA IF EXISTS dv CASCADE;
        DROP SCHEMA IF EXISTS sys CASCADE;

        --CREATE SCHEMA hoeft niet!! Want wordt gedaan bij aanmaken van pipeline, als schema nog niet bestaat
        --CREATE SCHEMA sor;
        --CREATE SCHEMA rdv;
        --CREATE SCHEMA dv;
        """

        query = text(sql)
        engine.execute(query)


if __name__ == '__main__':
    test_main()