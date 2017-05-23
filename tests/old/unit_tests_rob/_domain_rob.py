from pyelt.datalayers.database import Column, Columns
from pyelt.datalayers.dv import Sat, DvEntity, Link, Hub, HybridSat, LinkReference


class Role:
    pass


class Act:
    pass


class Participation:
    pass


class Zorgverlener(DvEntity, Role):

    class Default(Sat):
        zorgverlenernummer = Columns.TextColumn()
        aanvangsdatum = Columns.DateColumn()
        einddatum = Columns.DateColumn()

    class Personalia(Sat):
        achternaam = Columns.TextColumn()
        tussenvoegsels = Columns.TextColumn()
        voorletters = Columns.TextColumn()
        voornaam = Columns.TextColumn()

    class ContactGegevens(HybridSat):
        class Types(HybridSat.Types):
            telefoon = 'telefoon'
            mobiel = 'mobiel'
            mobiel2 = 'mobiel2'
        telnummer = Columns.TextColumn()
        datum = Columns.DateColumn()
        landcode = Columns.TextColumn()

    default = Default()
    personalia = Personalia()
    contactgegevens = ContactGegevens()


### voor ensemble test toegevoegd: begin
class Huisarts(Zorgverlener): # ensemble test
    def __init__(self):
        Zorgverlener.__init__(self)

class Fysio(Zorgverlener): # ensemble test
    def __init__(self):
        Zorgverlener.__init__(self)

### voor ensemble test toegevoegd: eind

class Adres(DvEntity, Role):

    class Default(Sat):

        postcode = Columns.TextColumn()
        huisnummer = Columns.IntColumn()
        huisnummer_toevoeging = Columns.TextColumn()
        straat = Columns.TextColumn()
        plaats = Columns.TextColumn()
        land = Columns.TextColumn()

    default = Default()


class Zorginstelling(DvEntity, Role):

    class Default(Sat):
        zorginstellings_naam = Columns.TextColumn()
        zorginstellings_nummer = Columns.TextColumn()

    default = Default()


# Dit is een HybridLink:
class Zorgverlener_Adres_Link(Link):
    class Types:
        post = 'post'
        bezoek = 'bezoek'
        woon = 'woon'
    zorgverlener = LinkReference(Zorgverlener)
    adres = LinkReference(Adres)


class Zorgverlener_Zorginstelling_Link(Link, Participation):
    zorgverlener = LinkReference(Zorgverlener)
    zorginstelling = LinkReference(Zorginstelling)
    huisarts = LinkReference(Zorgverlener) # ensemble test
    fysio = LinkReference(Zorgverlener) #ensemble test


class Zorginstelling_Adres_Link(Link):
    zorginstelling = LinkReference(Zorginstelling)
    adres = LinkReference(Adres)



