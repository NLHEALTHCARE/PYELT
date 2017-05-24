from pyelt.datalayers.database import Columns, Column
from pyelt.datalayers.dv import HubEntity, Sat, HybridSat, Link, LinkReference, LinkEntity


class Handeling(HubEntity):
    class Default(Sat):
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

class OrganisatieHandelingLinkEntity(LinkEntity):
    class Link(Link):
        organisatie = LinkReference(Organisatie)
        handeling = LinkReference(Handeling)

