from pyelt.datalayers.database import Columns, Column
from pyelt.datalayers.dv import DvEntity, Sat, HybridSat, Link, LinkReference


class Handeling(DvEntity):
    class Default(Sat):
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


# for k,v in Organisatie.Adres.__dict__.items():
#     print(k,v)

# import inspect
#
# for n in inspect.getmembers(Organisatie.Adres):
#     if n[1] == Column:
#         print(n[0], n[1].type)
# # print(Organisatie.Adres.__dict__)

for k, v in Organisatie.Adres.__ordereddict__.items():
    print(k, v)
