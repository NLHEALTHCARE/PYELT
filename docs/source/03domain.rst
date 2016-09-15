Domeinmodel
===========

De eerste stap ((#todo: van wat?)) is het aanmaken van een domeinmodel. Dit is het model van de entiteiten en hun relaties.
Een entiteit is in datavault termen een hub met een of meerdere sats; een relatie is een link. Een link is altijd tussen
twee entiteiten, oftewel tussen twee hubs.
Door het model (de python module) te registreren bij de pipe en de pipeline te runnen zullen de tabellen in de database
worden aangemaakt (zie onderaan).


Definiëren Entiteiten
=====================

#todo: ik neem aan dat de todo's uit onderstaand voorbeeld voor de definitieve versie weg moeten.
Het definiëren van een entiteit gaat als volgt::

    class Medewerker(DvEntity):
        class Default(Sat):
            geboortedatum = Columns.DateColumn()
            geslacht_code = Columns.TextColumn()
            overlijdens_indicator = Columns.TextColumn()
            datum_overlijden = Columns.DateTimeColumn()

        class Naamsgegevens(Sat):
            inschrijfnummer = Columns.TextColumn()
            initialen = Columns.TextColumn()
            voornamen = Columns.TextColumn()
            roepnaam = Columns.TextColumn()
            naamgebruik_code = Columns.TextColumn() #todo: ref kolom van maken? ;ik neem aan dat deze todo weg moet
            achternaam = Columns.TextColumn()
            geslachtsnaam_voorvoegsels = Columns.TextColumn()
            partnernaam = Columns.TextColumn()
            partnernaam_voorvoegsels = Columns.TextColumn()

        class Identificatie(Sat):
            nummer = Columns.TextColumn()
            geldig_tot = Columns.DateColumn()

        #todo: om te initialiseren zijn onderstaande regels nodig, nog uitzoeken of dat anders kan
        default = Default()
        naamsgegevens = Naamsgegevens()
        indentificatie = Indentificatie()



De class Medewerker erft over van DvEntity en krijgt 3 inner classes als sats.
In de database zullen dus 4 tabellen worden aangemaakt:

- medewerker_hub
- medewerker_sat (De Default inner class levert een sat op met een naam samengesteld uit de entiteit-naam met het
  achtervoegsel sat)
- medewerker_sat_naamsgegevens
- medewerker_sat_indentificatie

Je kunt overerven van de Medewerker class::

    class Arts(Medewerker):
        class Artsgegevens(Sat):
            agbcode = Columns.TextColumn()
            specialisatie = Columns.TextColumn()

        artsgegevens = Artsgegevens()

De arst krijgt hierdoor ook de 3 hubs van de medewerker.
Er zal geen arts_hub verschijnen in de database, want deze erft niet rechtstreeks over van DvEntity. De sat zal wel
verschijnen. Er komen dus de volgende tabellen:

- medewerker_hub
- medewerker_sat
- medewerker_sat_naamsgegevens
- medewerker_sat_indentificatie
- medewerker_sat_artsgegevens


Links
=====

Het definiëren van een link gaat als volgt::

    class Medewerker_Werkgever_Link(Link):
        medewerker = LinkReference(Medewerker)
        werkgever = LinkReference(Organisatie)

Deze class zal een link maken tussen de Medewerker en de Werkgever(Organisatie). Er komt 1 tabel, met de naam
medewerker_werkgever_link, in de database te staan en deze bevat 2 foreign keys::

  - _fk_medewerker_hub (reference -> medewerker_hub)
  - _fk_werkgever_hub (reference -> organisatie_hub)


Hiërarchische link (link met zichzelf)
======================================

Voorbeeld::

    class Medewerker_Medewerker_Link(Link):
        manager = LinkReference(Medewerker)
        werknemer = LinkReference(Medewerker)



Opmerking: Zorg ervoor dat alle velden (klasse-variabelen) een unieke naam krijgen.


HybridSats
==========

Hybrid sats zijn sats die als sleutel een combinatie van 3 ipv 2 velden hebben. Dus naast de standaard id en runid
(datum) velden hebben ze een 3de sleutelveld; namelijk het veld 'type'.
Je kunt dit bijvoorbeeld toepassen op telefoonnummers::

    class Persoon(DvEntity):
        class Telefoonnummers(HybridSat):
            class Types:
                vast = 'vast'
                mobiel = 'mobiel'
                fax = 'fax'
            nummer = Columns.TextColumn()
            geldig_tot = Columns.DateColumn()

De class erft over van HybridSat. Hierdoor krijgt het al het extra veld 'type'
De inner class Types is een handigheid (niet verplicht) en bevat alleen strings. Deze waardes worden in de database
opgeslagen in het veld type.
Om de standaard databaseview (query) aan te maken voor deze entititeit, met daarin voor elke telefoonnummer-type de twee
kolommen 'nummer' en 'geldig_tot' is het wel nodig van te voren te definiëren welke types er kunnen zijn in de inner class
Types.


HybridLinks
===========

Bij hybrid links maakt net als bij hybrid sats het type veld onderdeel uit van de link. Je past het toe op bijvoorbeeld adressen.
Een organisatie kan verschillende adressoorten hebben: een bezoekadres, een postadres en een factuuradres.
In plaats van drie links te definieren tussen de organisatie en de adres entiteit, maken we er een, waarbij het type-veld aangeeft om welke adressoort het gaat.

Een hybrid link wordt gelijk aan een gewone link gedefinieerd, erft over van Link. Je kun eventueel types in constanten stoppen::

    class Medewerker_Adres_Link(Link):
        class Types:
            post = 'post'
            bezoek = 'bezoek'
            woon = 'woon'
        medewerker = LinkReference(Medewerker)
        adres = LinkReference(Medewerker)

DynamicLinks
===========
Een dynamic link is een link waarbij dezelfde fk naar verschillende hubs kan verwijzen. Je definieert hem als volgt::

    class PatientHandelingLink(Link):
        class Types():
            hulpverlener = Hulpverlener
            zorginstelling = Zorginstelling
            zorgverzekeraar = Zorgverzekeraar
            locatie = Locatie

        Patient = LinkReference(Patient)
        Handeling = LinkReference(Handeling)
        Dynamic = DynamicLinkReference()


Let er op dat de Types nu geen strings zijn, maar naar entiteiten verwijzen.
Laatste regel bevat de dynamische link ref. De tabel zal oa. de volgende velden krijgen:
  - _fk_patient_hub (met index en fk constraint naar patient_hub._id)
  - _fk_handeling_hub (met index en fk constraint naar handeling_hub._id)
  - _fk_dynamic_hub (met index maar zonder fk constraint)

Referenties
===========

Een sat mag geen links (foreign keys) bevatten naar andere tabellen. Hierop bestaat 1 uitzondering: er mag een join zijn
met de referentietabel.
Standaard wordt een dv schema uitgerust met een referentie tabel. Dat is 1 tabel met daarin alle referentie velden.

De referentie tabel heeft de volgende stuctuur:

- type (text, key)
- code (text, key)
- description (text)
- description2 (text)

De sleutel is de combinatie van type met code. Bij deze combinatie kan vervolgens de omschrijving gevonden worden.

Je definieert in de Sat een RefColumn als volgt::

    class RefTypes:
        # aanmaken van enkele gebundelde string constanten
        geslacht = 'geslacht'
        specialisme_codes = 'specialisme'

    class Medewerker(DvEntity):
        class Default(Sat):
            geboortedatum = Columns.DateColumn()
            geslacht_code = Columns.RefColumn(RefTypes.geslacht)

Geslachtcode is hier een ref-column. Deze heeft dus een code-omschrijving combinatie in de referentie tabel en zal in de
standaard view te zien zijn.

Wanneer de database wordt aangemaakt, worden behalve de tabellen ook views aangemaakt. Per entity 1 view.
Bovenstaand voorbeeld zal een medewerker_view aanmaken met de velden:

- bk
- geboortedatum
- geslacht_code
- geslacht_desc

Ensemble_view
=============

Een ensemble_view is een view van een verzameling etiteiten met de daarbij horende link(s). Voordat een ensemble_view gemaakt kan worden is het wel noodzakelijk dat de views van gebruikte
entiteiten en van de links tussen die entiteiten al gemaakt zijn. De klasse van de gewenste ensemble_view maak je als
volgt::

    class TestEnsemble(Ensemble_view):
    def __init__(self,name='',entity_and_link_list=[]):
        Ensemble_view.__init__(self, name='',entity_and_link_list=[])
        self.name = 'test_view'
        self.add_entity_or_link(Zorginstelling)
        self.add_entity_or_link(Zorgverlener, 'huisarts')
        self.add_entity_or_link(Zorgverlener, 'fysio')
        self.add_entity_or_link(Zorgverlener_Zorginstelling_Link, 'link')

Je dient dus een naam voor de ensemble op te geven in 'self.name'. Verder moet moet aangeven worden welke entiteiten met
daarbijhorende link(s) deze ensemble bevat. In plaats van alleen de entiteit naam ('Zorginstelling') kan aan de
entititeiten of links ook een alias gegeven worden zoals bijvoorbeeld de veel kortere alias 'link' in plaats van
'Zorgverlener_Zorginstelling_Link'.


Aanmaken tabellen
=================

Door het domeinmodel (de python module) te registreren bij de pipe en vervolgens de pipeline te runnen zullen de
tabellen in de database worden aangemaakt::

    from domainmodels from main import get_root_path_domain

    pipeline = Pipeline(config)
    pipe = pipeline.get_or_create_pipe('test')
    pipe.register_domain(main_domain)
    pipeline.run()
