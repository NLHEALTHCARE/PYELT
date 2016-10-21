# from domainmodel_fhir import test_domain
# from domainmodel_fhir.test_mappings import init_source_to_sor_mappings, init_sor_to_dv_mappings
import unittest

from sqlalchemy import create_engine, text

from pyelt.pipeline import Pipeline
from tests.unit_tests_rob import test_domain
from tests.unit_tests_rob.test05r_process import test_row_count
from tests.unit_tests_rob.test_mappings import init_source_to_sor_mappings, init_sor_to_dv_mappings

""" voor testen van jsonb"""


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


if __name__ == '__main__':
    test_main()