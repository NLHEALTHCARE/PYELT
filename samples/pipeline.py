from main import get_root_path
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.pipeline import Pipeline
from pyelt.sources.files import CsvFile
from samples.confg import config, source_config


def run_staging():

    pipeline = Pipeline(config)
    pipe = pipeline.get_or_create_pipe('test_source', source_config)

    source_file = CsvFile(get_root_path() + '/sample_data/patienten1.csv', delimiter=';')
    source_file.reflect()
    source_file.set_primary_key(['patientnummer'])
    mapping = SourceToSorMapping(source_file, 'persoon_hstage', auto_map=True)
    pipe.mappings.append(mapping)

    pipeline.run()


def run_datavault():
    pipeline = Pipeline(config)
    pipe = pipeline.get_or_create_pipe('test_source', source_config)
    from samples import customers_datavault
    module = customers_datavault
    pipe.register_domain(module)
    pipeline.run()




def run_all():
    pipeline = Pipeline(config)
    pipe = pipeline.get_or_create_pipe('test_source', source_config)
    module = None
    pipe.register_domain(module)

    source_file = CsvFile(get_root_path() + '/tests/data/patienten1.csv', delimiter=';')
    source_file.reflect()
    source_file.set_primary_key(['patientnummer'])
    mapping = SourceToSorMapping(source_file, 'persoon_hstage', auto_map=True)
    pipe.mappings.append(mapping)



    pipeline.run()


if __name__=='__main__':
    run_staging()
    run_datavault()
    # or run_all()
