from pyelt.pipeline import Pipeline
from tests.unit_tests_basic.global_test_suite import *
__author__ = 'hvreenen'

import unittest




class TestCase_Pipeline(unittest.TestCase):
    def test_pipeline_exists(self):
        pipeline = Pipeline({})
        self.assertIsNotNone(pipeline)
        self.assertIsNotNone(pipeline.dwh)
        self.assertIsNotNone(pipeline.dwh.dv)
        self.assertIsNotNone(pipeline.dwh.sys)

    def test_pipes(self):
        pipeline = Pipeline({})
        pipe = pipeline.get_or_create_pipe('test_system', config=test_system_config)
        self.assertIsNotNone(pipe)
        self.assertIsNotNone(pipe.pipeline)
        self.assertEqual(len(pipeline.pipes), 1)
        self.assertEqual(len(pipeline.dwh.sors), 1)

        self.assertIsNone(pipe.source_db)
        self.assertIsNotNone(pipe.source_path)
        self.assertIsNotNone(pipe.sor)
        self.assertEqual(pipe.sor.name, 'sor_test_system')

        pipe2 = pipeline.get_or_create_pipe('test_system2', config = {'sor_schema': 'sor_test_system2' })
        self.assertEqual(len(pipeline.pipes), 2)
        self.assertEqual(len(pipeline.dwh.sors), 2)

        pipe_met_zelfde_naam = pipeline.get_or_create_pipe('test_system2', config = {'sor_schema': 'sor_test_system2' })
        self.assertEqual(len(pipeline.pipes), 2)
        self.assertEqual(len(pipeline.dwh.sors), 2)

def run():
    unittest.main()

if __name__ == '__main__':
    unittest.main()
