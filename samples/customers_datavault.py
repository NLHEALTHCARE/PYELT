from pyelt.datalayers.database import Columns
from pyelt.datalayers.dv import DvEntity, Sat, HybridSat, LinkReference, Link


class Handeling(DvEntity):
    class Default(Sat):
        naam = Columns.TextColumn()
        datum = Columns.DateColumn()
        nummer = Columns.IntColumn()
        soort = Columns.RefColumn('handeling_soorten')

    class Financieel(Sat):
        kostprijs = Columns.FloatColumn()
        vraagprijs = Columns.FloatColumn()
        korting = Columns.FloatColumn()
        btw = Columns.FloatColumn()


class Organisatie(DvEntity):
    class Default(Sat):
        naam = Columns.TextColumn()
        specialisatie = Columns.RefColumn('specialisaties')

    class Adres(HybridSat):
        class Types(HybridSat.Types):
            bezoek = 'bezoek'
            post = 'post'
            factuur = 'factuur'

        postcode = Columns.TextColumn()
        huisnummer = Columns.IntColumn()
        huisnummer_toevoeging = Columns.TextColumn()
        plaats = Columns.TextColumn()
        land = Columns.RefColumn('landen')


class OrganisatieHandelingLink(Link):
    Organisatie = LinkReference(Organisatie)
    Handeling = LinkReference(Handeling)

    class Details(Sat):
        datum = Columns.DateColumn()
        plek = Columns.TextColumn()