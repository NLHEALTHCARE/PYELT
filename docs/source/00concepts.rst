Begrippen DWH 2.0 & Pyelt
=========================

Gelaagde opbouw
---------------

DWH2.0 is opgebouwd in lagen. De data verhuist meerdere keren van de ene naar het andere database schema.
We kennen de volgende lagen:

Sources:
    Bron systemen; dit kunnen databases zijn maar ook bestanden zoals csv files.

Sor:
    Dit is de staging area. Hierin staan temp tabellen en historische staging tabellen.
    Het wordt aangeraden om per bronsysteem een eigen sor database (schema) aan te maken.

Rdv:
    Raw data Vault. Vanuit de sor laag gaat sommige data eerst naar de rdv. Dit treedt op wanneer er bewerkingen nodig
    zijn (met name voor keys) om de data op de juiste wijze te kunnen integreren in de volgende laag.

Dv:
    Datavault. De eigenlijke datavault met hubs, sat en links.

Datamart:
    Datamart met rapporten


Begrippen
---------

Pipeline:
    De pipeline omvat de gehele etl van alle bronsystemen en alle database lagen. Een pipeline bestaat uit 1 of meerdere
    pipes.

Pipe:
    #todo: afbeelding toevoegen?
    In de afbeelding hierboven is een pipe in het oranje omcirkeld.  Per bron is er een eigen pipe. De pipe omvat een
    bronlaag, een sorlaag, een deel van de rdv tabellen en een deel van de dv tabellen.


Datavault begrippen
-------------------

Hub:
    Een tabel met een betekenisvolle sleutel.

BK:
    Business key. Het enige veld in de hub tabel naast systeem velden zoals bijvoorbeeld "insert_date" en de technische
    sleutel.

Sat:
    Een satelite tabel. Deze is gekoppeld aan de hub en bevat voor iedere wijziging in de data een rij. De sleutel is
    samengesteld uit de technische sleutel van de hub ((en de import runid)).

    #todo: wat bedoel je precies met het deel tussen de dubbele haken aan het eind van bovenstaande regel? wordt de
    runid uit geimporteerd uit de hub of komt deze toch ergens anders vandaan?

Link:
    Een link tussen 2 of meer hubs.

Hybrid Sat:
    Een sat met een extra type-veld in de sleutel (bijvoorbeeld telefoonnummers bij een persoon).

Hybrid links:
    Een link met extra type veld.

Hierarchical Links:
    Een link die een hub met zichzelf verbindt.

Entity:
    De verzameling van 1 hub met bijbehorende sats (dit is geen tabel).

Ensemble:
    Verzameling van meerdere entiteiten met links.

Domainmodels
------------

Representatie van de datavault in python classes. De hubs, sats en links worden in python gedefinieerd en aan de hand
hiervan worden de datavault tabellen aangemaakt.


Mappings
--------

TableMapping:
    Mapping van een brontabel of -file op een doeltabel.

FieldMapping:
    Mapping van een bronveld of -kolom op een doelkolom.

Transformation:
    Mapping van een bron- naar doelveld waarbij de bron eerst nog bewerkt wordt.

BkMapping:
    #todo:

LinkMapping:
    #todo:

SatMapping:
    #todo:





