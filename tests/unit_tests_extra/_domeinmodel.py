from pyelt.datalayers.database import Columns, Column
from pyelt.datalayers.dv import HubEntity, Sat, HybridSat, Link, LinkReference
from pyelt.datalayers.valset import DvValueset


class Handeling(HubEntity):
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


class Organisatie(HubEntity):
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


class Valueset(DvValueset):
    valueset_naam = Columns.TextColumn()
    code = Columns.TextColumn()
    omschrijving = Columns.TextColumn()
