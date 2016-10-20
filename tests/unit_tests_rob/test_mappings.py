from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.files import CsvFile
from tests.unit_tests_rob.test_domain import Patient

""" voor testen van jsonb"""

def init_source_to_sor_mappings(pipe):
    mappings = []
    validations = []
    path = pipe.config['data_path']
    source_file = CsvFile('{}{}'.format(path, 'test.csv'), delimiter=';')



    source_file.reflect()
    source_file.set_primary_key(['patient_nummer'])
    sor_mapping = SourceToSorMapping(source_file, 'patient_hstage', auto_map=True)
    mappings.append(sor_mapping)

    return mappings


def init_sor_to_dv_mappings(pipe):

    mappings = []
    sor = pipe.sor

    mapping = SorToEntityMapping('patient_hstage', Patient, sor)
    mapping.map_bk('patient_nummer')
    mapping.map_field("patient_nummer", Patient.Default.patient_nummer)
    # mapping.map_field("active::boolean", Patient.Default.active)

    # mapping.map_field("gender", Patient.Default.gender)

    mapping.map_field("extra::jsonb", Patient.Default.extra)


    mapping.map_field("deceased_boolean", Patient.Default.extra2)  # keyword "true" is lowercase voor JSON(B); deze wijze ook in bronbestand gebruikt
    mapping.map_field("deceased_datetime::date", Patient.Default.extra2)
    mapping.map_field("birthdate::date", Patient.Default.birthdate)
    # mapping.map_field("multiple_birth_boolean::boolean", Patient.Default.extra2)  # keyword als "True" niet de JSONB notatie dus casten naar boolean, anders krijg je een error
    # mapping.map_field("multiple_birth_integer", Patient.Default.extra2)


    mappings.append(mapping)

    return mappings
