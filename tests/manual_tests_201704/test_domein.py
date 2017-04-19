from pyelt.datalayers.dv import *
from pyelt.datalayers.valset import DvValueset

class EnqueteMeettraject(HubEntity):
    class Default(Sat):
        anatomie = Columns.TextColumn()
        episode = Columns.TextColumn() #episode_general
        zijde = Columns.TextColumn()
        datum_aanmaak = Columns.DateTimeColumn()  # reference_date
        datum_afgerond = Columns.DateTimeColumn()
        extra = Columns.TextColumn()

    class Proms(Sat):
        anatomie = Columns.TextColumn()
        episode_globaal = Columns.TextColumn()  # episode_general
        episode_specifiek = Columns.TextColumn()  # episode_specific
        zijde = Columns.TextColumn()
        subtraject_nummer = Columns.TextColumn()
        zorgtraject_nummer = Columns.TextColumn()
        kliniek_code = Columns.TextColumn()
        specialisme_code = Columns.TextColumn()
        diagnose_code = Columns.TextColumn()
        behandelaar_agb = Columns.TextColumn()
        datum_aanmaak = Columns.DateTimeColumn()

    class Telepsy(Sat):
        anatomie = Columns.TextColumn()
        episode = Columns.TextColumn()  # episode_general
        zijde = Columns.TextColumn()
        zorgtraject_nummer = Columns.TextColumn()
        subtraject_nummer = Columns.TextColumn()
        kliniek_code = Columns.TextColumn()
        specialisme_code = Columns.TextColumn()
        diagnose_code = Columns.TextColumn()
        behandelaar_agb = Columns.TextColumn()
        datum_aanmaak = Columns.DateTimeColumn()
        days_diff = Columns.IntColumn()
        validation_msg = Columns.TextColumn()

    class Identificatie(Sat):
        patient_nummer = Columns.TextColumn()
        meettraject_volgnummer = Columns.IntColumn()
        zorgtraject_nummer= Columns.TextColumn()
        subtraject_nummer = Columns.TextColumn()
        eerste_afspraak_nummer = Columns.TextColumn()
        kliniek_code = Columns.TextColumn()
        specialisme_code = Columns.TextColumn()
        diagnose_code = Columns.TextColumn()
        behandelaar_agb = Columns.TextColumn()

    class New(Sat):
        nummer = Columns.TextColumn()

    class New2(Sat):
        nummer = Columns.TextColumn()

    class New3(HybridSat):
        class Types(HybridSat.Types):
            telefoon = 'telefoon'
            mobiel = 'mobiel'
            werk = 'werk'
            nogmeer = 'nog meer'
        nummer = Columns.TextColumn()
