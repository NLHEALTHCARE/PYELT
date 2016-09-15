from typing import List


from pyelt.datalayers.database import Column, Columns
from pyelt.datalayers.dv import Sat, DvEntity, Link, Hub, HybridSat, LinkReference, DynamicLinkReference


class Role:
    pass
class Act:
    pass
class Participation:
    pass



class Patient(DvEntity, Role):
    """Patienten zijn alle personen in de rol van patient.
    bk: registratie_syteem + inschrijvingsnummer"""

    # hub = Hub('patient')
    class Inschrijving(Sat):
        bsn = Columns.TextColumn()
        inschrijfnummer = Columns.TextColumn()
        inschrijfdatum = Columns.DateTimeColumn()

    class Default(Sat):
        geboortedatum = Columns.DateColumn()

    class Personalia(Sat):
        achternaam = Columns.TextColumn()
        tussenvoegsels = Columns.TextColumn()
        voorletters = Columns.TextColumn()
        voornaam = Columns.TextColumn()

    class Adres(Sat):
        postcode = Columns.TextColumn()
        huisnummer = Columns.IntColumn()
        huisnummer_toevoeging = Columns.TextColumn()
        straat = Columns.TextColumn()
        plaats = Columns.TextColumn()
        land = Columns.TextColumn()

    class ContactGegevens(HybridSat):
        class Types():
            telefoon = 'telefoon'
            mobiel = 'mobiel'
            mobiel2 = 'mobiel2'
        telnummer = Columns.TextColumn()
        datum = Columns.DateColumn()

    default = Default()
    personalia = Personalia()
    adres = Adres()
    inschrijving = Inschrijving()
    contactgegevens = ContactGegevens()




class Traject(DvEntity, Act):
    class Default(Sat):
        naam = Columns.TextColumn()
        start = Columns.DateTimeColumn()
        eind = Columns.DateTimeColumn('sluit_datum')
        nummer = Columns.IntColumn()
        status =  Columns.TextColumn()

    sat = Default()


class SubTraject(Traject):
    class SubTraject(Sat):
        declaratie_nummer = Columns.IntColumn()



class Patient_Traject_Link(Link, Participation):
    Patient = LinkReference(Patient)
    Traject = LinkReference(Traject)

    class Default(Sat):
        start = Columns.DateTimeColumn()
        eind = Columns.DateTimeColumn('sluit_datum')
        status = Columns.TextColumn()


class Organisatie(DvEntity):
    class Default(Sat):
        naam = Columns.TextColumn()


class Zorgverzekeraar(Organisatie):
    class Zorgverzekeraar(Sat):
        nummer = Columns.TextColumn()


class Zorginstelling(Organisatie):
    class Zorginstelling(Sat):
        agbnummer = Columns.TextColumn()


class Hulpverlener(DvEntity):
    class Default(Sat):
        naam = Columns.TextColumn()
        agbnummer = Columns.TextColumn()


class Locatie(DvEntity):
    class Default(Sat):
        naam = Columns.TextColumn()
        kamernummer = Columns.TextColumn()
        lat_long = Columns.TextColumn()


class Handeling(DvEntity):
    class Default(Sat):
        naam = Columns.TextColumn()
        dbc_activiteit_code = Columns.TextColumn()
        datum = Columns.DateTimeColumn()


class PatientHandelingLink(Link):
    class Types():
        hulpverlener = Hulpverlener
        zorginstelling = Zorginstelling
        zorgverzekeraar = Zorgverzekeraar
        locatie = Locatie

    Patient = LinkReference(Patient)
    Handeling = LinkReference(Handeling)
    Dynamic = DynamicLinkReference()
