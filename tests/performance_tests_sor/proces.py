from pyelt.pipeline import *
from pyelt.sources.databases import SourceTable
from tests.performance_tests_sor.configs import *


def init_source_to_sor_mappings(pipe):
    mappings = []
    validations = []
    source_db = pipe.source_db
    source_tbl = SourceTable('random_data', source_db.default_schema, source_db)
    source_tbl.set_primary_key(['uid'])
    sor_mapping = SourceToSorMapping(source_tbl, 'random_data_hstage', auto_map=True)
    mappings.append(sor_mapping)

    return mappings


def run():
    pipeline = Pipeline(general_config)
    pipe = pipeline.get_or_create_pipe('test_system', config=test_system_config)
    pipe.mappings.extend(init_source_to_sor_mappings(pipe))
    pipeline.run()

if __name__ == '__main__':
    run()