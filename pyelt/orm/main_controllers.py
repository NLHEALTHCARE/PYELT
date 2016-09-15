from sample_domains.role_domain import Patient
from pipelines.clinics.clinics_configs import general_config
from pyelt.orm.base_controller import Controller
from pyelt.pipeline import Pipeline


class Patienten(Controller):
    entity_cls = Patient


def main():
    pipeline = Pipeline(general_config)
    pipe = pipeline.get_or_create_pipe('_test')
    patienten = Patienten(pipeline.dwh)

    list = patienten.load_by_hub()
    patient = list[0]
    a = patient.naamgegevens.initialen
    patient.naamgegevens.initialen = 'T.'
    patient.naamgegevens.geslachtsnaam = 'Test3'
    patient.inschrijving.inschrijfnummer = '123123'
    patienten.save_entity(patient)
    for r in list:
        print(r.__dict__)

    runid = pipeline.create_new_runid()
    new_patient = patienten.new(bk='12345a', runid=runid)
    new_patient.personalia.voornaam = '12837612387612'
    patienten.save_entity(new_patient)


if __name__ == '__main__':
    main()
