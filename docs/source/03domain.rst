Domeinmodel
===========

De eerste stap van het creeren van een datavault is het aanmaken van een domeinmodel. Dit is het model van de entiteiten en hun relaties.
Een entiteit is in datavault termen een hub met een of meerdere sats; een relatie is een link. Een link is altijd tussen
twee entiteiten, oftewel tussen twee hubs.
Door het model (de python module) te registreren bij de pipe en de pipeline te runnen zullen de tabellen in de database
worden aangemaakt (zie onderaan).


Definiëren Entiteiten
=====================

Het definiëren van een entiteit gaat als volgt. Maak in de map /domainmodel een bestand aan main_domain.py. Zet hierin de code::

    class Medewerker(HubEntity):
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



In de database zullen straks 4 tabellen worden aangemaakt:

- medewerker_hub
- medewerker_sat (De Default inner class levert een sat op met een naam samengesteld uit de entiteit-naam met het
  achtervoegsel sat)
- medewerker_sat_naamsgegevens
- medewerker_sat_indentificatie

We kunnen de tabellen aanmaken door de volgende code toe te voegen en te runnen. Maak hiertoe echter eerst een lege database aan in postgres. Gooi het schema public weg.
Pas vervolgens de connectie string aan in de configs.py::

    from domainmodel import main_domain
    pipeline.register_domain(main_domain)
    pipeline.run()

Na het runnen zul je zien dat er 5 schema's zijn aangemaakt en in het dv schema staan de 4 tabellen.

Je kunt vanzelfsprekend meerdere domein-modules maken. Je die elke te registreren bij de pipeline.

Overerving in domein
--------------------
Je kunt overerven van de Medewerker class::

    class Arts(Medewerker):
        class Artsgegevens(Sat):
            agbcode = Columns.TextColumn()
            specialisatie = Columns.TextColumn()

De arst krijgt hierdoor ook de hub en de 3 sat van de medewerker.
Er zal dus geen arts_hub verschijnen in de database, want deze erft niet rechtstreeks over van HubEntity. De extra sat zal wel
verschijnen. Na opnieuw runnen zullen zich de volgende tabellen in de database bevinden:

- medewerker_hub
- medewerker_sat
- medewerker_sat_naamsgegevens
- medewerker_sat_indentificatie
- medewerker_sat_artsgegevens


Links
=====

Het definiëren van een link gaat als volgt::

    class MedewerkerWerkgeverLinkEntity(LinkEntity):
        class Link(Link):
            medewerker = LinkReference(Medewerker)
            werkgever = LinkReference(Organisatie)

Deze class zal een link maken tussen de Medewerker en de Werkgever(Organisatie). Er komt 1 tabel, met de naam
medewerker_werkgever_link, in de database te staan en deze bevat 2 foreign keys::

  - fk_medewerker_hub (reference -> medewerker_hub)
  - fk_werkgever_organisatie_hub (reference -> organisatie_hub)

Opmerking: de naam van de fk wordt automatisch aangemaakt. Hierbij krijgt het alleen de naam van de hub waarnaar die verwijst in zich als het veld afwijkt van die naam.
Met andere woorden: medewerker = LinkReference(Medewerker) wordt fk_medewerker_hub, omdat medewerker niet afwijkt van LinkReference(Medewerker), terwijl werkgever = LinkReference(Organisatie) de naam fk_werkgever_organisatie_hub geeft


Hiërarchische link (link met zichzelf)
======================================

Voorbeeld::

    class MedewerkerMedewerkerLinkEntity(LinkEntity):
        class Link(Link):
            manager = LinkReference(Medewerker)
            werknemer = LinkReference(Medewerker)



Opmerking: Zorg ervoor dat alle velden (klasse-variabelen) een unieke naam krijgen.


HybridSats
==========

Hybrid sats zijn sats die als sleutel een combinatie van 3 ipv 2 velden hebben. Dus naast de standaard _id en _runid
hebben ze een 3de sleutelveld, namelijk het veld 'type'.
Je kunt dit bijvoorbeeld toepassen op telefoonnummers::

    class Persoon(HubEntity):
        class Telefoonnummers(HybridSat):
            class Types (HybridSat.Types):
                vast = 'vast'
                mobiel = 'mobiel'
                fax = 'fax'
            nummer = Columns.TextColumn()
            geldig_tot = Columns.DateColumn()

De class erft over van HybridSat. Hierdoor krijgt het al het extra veld 'type'
De inner class Types bevat alleen strings. Deze waardes worden in de database
opgeslagen in het veld type, afhankelijk van hoe er wordt gemapt.

Je kunt nu in een tabel 3 soorten telefoonnummer met hun geldigheidsdatum kwijt en hoeft niet per type een eigen veld aan te maken.


HybridLinks
===========

Bij hybrid links maakt net als bij hybrid sats het type veld onderdeel uit van de link. Je past het toe op bijvoorbeeld adressen.
Een organisatie kan verschillende adressoorten hebben: een bezoekadres, een postadres en een factuuradres.
In plaats van drie links te definieren tussen de organisatie en de adres entiteit, maken we er een, waarbij het type-veld aangeeft om welke adressoort het gaat.

Een hybrid link wordt gelijk aan een gewone link gedefinieerd, erft over van Link. Je kun eventueel types in constanten stoppen::

    class MedewerkerAdresLink(LinkEntity):
        class Link(HybridLink):
            class Types:
                post = 'post'
                bezoek = 'bezoek'
                woon = 'woon'

            medewerker = LinkReference(Medewerker)
            adres = LinkReference(Adres)

Link met een sat
================

Het is toegestaan bij een link een sat toe te voegen. Hierin dien alleen informatie te staan die echt over de link gaat en niet over een van de hubs. De link moet geen verkapte hub worden. In de praktijk zal dit maar weinig voorkomen.

Voorbeeld::

    class MedewerkerWerkgeverLinkEntity(LinkEntity):
        class Link(Link):
            medewerker = LinkReference(Medewerker)
            werkgever = LinkReference(Organisatie)

        class Geldigheid(Sat):
            vanaf = Columns.DateColumn()
            tot = Columns.DateColumn()

DynamicLinks
===========
OUT OF ORDER. Hier wordt voorlopig niet aan gewerkt
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

Ensemble_view
=============
OUT OF ORDER. Hier wordt voorlopig niet aan gewerkt
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

