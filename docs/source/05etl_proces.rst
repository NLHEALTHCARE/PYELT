ETL Proces
==========

Nadat de mappings gedefinieerd en aan de pipe toegevoegd zijn kan het proces worden gerund::

        pipeline = Pipeline(config)
        pipe = pipeline.get_or_create_pipe('_test')
        pipe.register_domain(domainmodels.test_domain)
        pipe.mappings.extend(init_source_to_sor_mappings())
        pipe.mappings.extend(init_sor_to_dv_mappings())
        pipeline.run()

Achter de schermen gebeuren dan globaal de volgende stappen:

1. Voorbereidende stappen:
        - aanmaken van een nieuw run id
        - aanmaken log-files
        - validatie van domein classes en van mappings.
2. DDL: Structurele database wijzigingen in de lagen: sor, dv.
3. Validatie mappings
4. ETL: kopieren, transformeren en valideren van data tussen de lagen
    A. van source naar sor
        - etl van bron naar sor
        - validaties van sor data
        - aanmaken nieuwe sor tabellen
    B. van sor naar valsets:
        - etl van sor naar dv
        - validaties van dv data
    C. van sor naar dv:
        - etl van sor naar dv
        - validaties van dv data
5. Controle en afsluiting



Pipeline.Run Detail stappen
---------------------------

Deze procedure kan ook per type los uitgevoerd worden door een parameter op te geven. Bijvoorbeeld de paramter'sor' waardoor alleen de stap van bron naar sor draait en alle andere stappen niet.


1. Voorbereidende stappen
^^^^^^^^^^^^^^^^^^^^^^^^^

**create_schema_if_not_exists:** controleert of vaste schema's (sys en/of dv) bestaan (via reflection). Zo niet, maak deze aan.

**validate_domains**: Valideert of de geregistreerde domeinen van alle pipes geldig zijn. Mochten er fouten worden ontdekt dan wordt hier melding van gemaakt en stopt het proces nog voordat de ddl en etl zijn opgestart.

**create_new_runid**: maakt een nieuw *runid* aan. De runid wordt met 0,01 verhoogd voor iedere volgende run op *dezelfde* dag. De eerste run op een nieuwe dag krijgt het eerste gehele getal volgend op de maximale aanwezige *runid* in de tabel. Mocht een run twee keer draaien op de zelfde dag dan wordt runid met 0.01 opgehoogd. Runid wordt bewaard in de database in het sys-schema. Laatste runid wordt opgehaald, met laatste rundatum.

**create_logger(MAIN):** hier wordt het overzicht van het ETL proces gelogd (vergelijkbaar met wat op het scherm verschijnt).

**create_logger(SQL):** hier worden de afgevuurde SQL statements gelogd.


2. DDL: Structurele database wijzigingen in de lagen (ddl.py)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
*Per pipe de onderstaande stappen:*

**create_schema_if_not_exists(sor):** controleert met behulp van reflect of sor-schema bestaat, maakt deze anders aan.

**create_sor_from_mappings:** Voert ddl uit van de sor-laag. Maakt eventuele nieuwe sor-tabellen aan aan de hand van de gedefiniëerde mappings.
Per bron-bestand, -query of -tabel worden telkens drie tabellen toegevoegd:
- sor.[naam]_temp (unlogged, wordt telkens geleegd)
- sor.[naam]_temp_hash (unlogged, wordt telkens geleegd)
- sor.[naam]_hstage (bevat alle historie)

Tabellen/ kolommen worden uitsluitend toegevoegd, nooit verwijderd.

**create_dv_from_domain**: Voert ddl van de dv laag in het bestand 'domains.py' uit. Maakt eventuele nieuwe tabellen aan (hubs, sats en links) gebaseerd op de gedefiniëerde domeinen.

**run_extra_sql**: runt eventueel extra sql code direct op de database, zoals bijvoorbeeld aanmaken van een postgres-type, of eventuele updates direct op de database, mochten er echt fouten zijn. Voorbeeld::

    extra_sql = [(0, 'CREATE TYPE sor_timeff.huisnummer_type AS (huisnummer Int, huisnummer_toevoeging Text);')]
    # list van tuples met twee velden er tuple
    # - actief: 0 of 1
    # - sql statement


**create_db_functions**: maakt database functies aan die zijn gedefinieerd per pipe, voor transformaties van data, zie 05transformaties. Controleert met behulp van reflect of functie bestaat en maakt het aan/ wijzigt de functie.


3. Valideert mappings
^^^^^^^^^^^^^^^^^^^^^

**validate_mappings**: Valideert of de mappings van alle pipes geldig zijn. Wordt uitgevoerd voorafgaande aan de etl, maar nadat de ddl van de dv-laag heeft gerund. Immers, pas als de tabellen en velden bestaan kun je weten of de mappings geldig zijn.


4. ETL: kopieren, transformeren en valideren van data tussen de lagen
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*Per pipe de onderstaande stappen:*

**pipe.run(parts)**: De etl-stappen voor de pipe worden uitgevoerd. Zie piperun_


5. Controle en afsluiting
^^^^^^^^^^^^^^^^^^^^^^^^^

.. _piperun:



Pipe.Run Detail stappen
--------------------------


A. van source naar sor
^^^^^^^^^^^^^^^^^^^^^^
*per sor mapping*

**by_md5:** Is de bron een databasetabel of een bestand? Als het een tabel is dan worden tijdens het inlezen van bron naar sor-laag, de wijzigingen bepaald met een md5-hash. Als het een bestand is dan gebruikt het proces geen md5-hash.

**source_to_sor_by_hash:** *alleen voor databse tabellen: by_md5 = True*
Om minder gegevens over de lijn te hoeven trekken gebruiken we voor databasebestanden een MD5 checksum van de rij om te vergelijken of een rij is gewijzigd ipv dat we alle velden uit de rij vergelijken. Deelstappen:

 1. Voor alle rijen word de sleutel en de md5-hash van alle velden tezamen opgehaald en in een csv file weggesschreven. De md5-hash wordt op de bron database in eigen dialect berekend.
 2. Maak tijdelijke tabel *sor.[naam]_hash* leeg
 3. Data wordt ingelezen vanaf de csv uit stap 1 naar *sor.[naam]_hash*
 4. Er wordt gekeken of het een eerste keer inlezen betreft of dat er al vaker is ingelezen, door te kijken of er data aanwezig is de de tabel *sor.[naam]_hstage*. Mocht het de eerste keer zijn, dan moeten namelijk zowiezo alle gegevens worden opgehaald en kunnen stap 4a en 4b worden overgeslagen:
    a. *inlezen is niet eerste keer* Data uit de [naam]_hash tabel wordt vergeleken met de data die in de sor.[naam]_hstage tabel zit. Is er een wijziging dan wordt de rij gemarkeerd.
    b. Van alle gemarkeerde rijen (=nieuwe en gewijzigde rijen) wordt een sql-filter aangemaakt waarmee alle velden kunnen worden opgehaald(mbv where in statement).
 5. Data met alle velden wordt opgehaald en in csv bestand gezet aan de hand van het filter uit stap 4b (filter is leeg in geval van eerste keer inlezen).
 6. Maak tijdelijke tabel *sor.[naam]_temp* leeg
 7. Data wordt ingelezen vanaf de csv uit stap 1 naar *sor.[naam]_temp*
 8. De data wordt van _temp naar _hstage gekopieerd
 9. Oude rijen in _hstage worden inactief gemaakt en revisie nummer van nieuwe rijen wordt berekend dmv twee update sql statements

**source_to_sor:** *alleen voor bestanden: by_md5 = False*

Is het zelfde als source_to_sor_by_hash vanaf stap 5:

 1. Data met alle velden wordt opgehaald en in csv bestand gezet.
 2. Maak tijdelijke tabel *sor.[naam]_temp* leeg
 3. Data wordt ingelezen vanaf de csv uit stap 1 naar *sor.[naam]_temp*
 4. De data wordt van _temp naar _hstage gekopieerd
 5. Oude rijen in _hstage worden inactief gemaakt en revisie nummer van nieuwe rijen wordt berekend dmv twee update sql statements

*per sor validatie*

**validate_sor:**:
validate_sor. Er worden vaste validaties gedaan en validatie die door de gebruiker zijn gedefinieerd.
- Vast: Er wordt gecontroleerd op dubbele sleutels. Als deze voorkomen wordt dit weggeschreven naar een exceptions tabel.
- User defined: Alle gedefinieerde SQL validatieregels (zoals bijvoorbeel LEN(BSN) BETWEEN 8 AND 9) worden toegepast, fouten worden weggeschreven naar een exceptions tabel.

In de sor laag worden invalide velden wel gewoon opgenomen, maar niet doorgekopieerd naar de datavault.  Zie ook 05validaties


B. van sor naar dv
^^^^^^^^^^^^^^^^^^
De sor laag wordt hier verwerkt in refs, hubs, sats en links in de datavault. De sor laag blijft hierbij ongewijzigd.

**sor_to_ref:** hier worden de referentietabellen (valuesets) aangevuld.

**sor_to_entity:**

*per sor to entity mapping*

 1. inlezen nieuwe hub-data:

    a. Voor records in de sor die valide zijn en waarvoor nog geen bk in de hub bestaat worden nieuwe regels in de hub aangemaakt. (Bij nieuwe hubs worden alle gegevens uit de SOR verwerkt. Anders alleen die van de laatste runid.)
    b. De primary key van deze hub (_id) wordt als foreign key terug weggeschreven in de sor om hierna makkelijker de de sats en links te kunnen vullen.

 2. inlezen sats:

    a. per rij wordt bekeken of er wijzigingen zijn. Zo ja: dan wordt een nieuwe regel aangemaakt.
    b. hierna worden oudere records in-actief gemaakt en wordt het revisie nummer van nieuwe record geupdate

*per sor to entity validation*
 3. validaties van de entities: per validatie regel door gebruiker opgegeven zie 05validaties

**sor_to_link**

*per sor to link mapping*

 1. inlezen nieuwe link-data:
    De fk's die naar de hubs verwijzen uit de sor tabellen worden gebruikt om te kijken of er nieuwe links zijn bijgekomen. Als die er zijn worden nieuwe rijen geinsert.
