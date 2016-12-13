from sample_domains.role_domain import Patient
from pipelines.clinics.clinics_configs import general_config
from pyelt.orm.dv_objects import DbSession


def main():
    session = DbSession(config=general_config, runid=99)
    pat_hub = Patient.cls_get_hub()
    rows = pat_hub.load()
    for row in rows.values():
        print(row.bk, row._id)

    new_hub = pat_hub.new()
    new_hub.bk = 'ajshdgashdg4'
    pat_hub.save()

    Patient.cls_init()
    sat = Patient.Naamgegevens()
    rows = sat.load()
    for row in rows.values():
        print(row._id, row._runid, row._revision, row.geslachtsnaam)

    new_hub = pat_hub.new()
    new_hub.bk = 'ajshdgashdg11'
    pat_hub.save()
    print(new_hub._id)

    new_sat = sat.new()
    new_sat.geslachtsnaam = 'Reenen5'
    new_sat._id = new_hub._id
    sat.save()

    pat_ent = Patient()
    # regel hieronder laad de hub van de entity en zet in elke regel een sat-definitie
    # sat is dan nog niet geladen
    # op moment dat je vraagt om de sat wordt de data van de hele in __get_attr van de entity geladen
    rows = pat_ent.load()
    for row in rows.values():
        print(row._id, row.bk)
        # na aanroep van row.naamgegevens wordt die al geladen
        print(row.naamgegevens.db_status)
        print(row.naamgegevens.initialen)
        print(row.naamgegevens.db_status)
        print('--------')


main()
