Valuesets
===========

Een sat mag geen links (foreign keys) bevatten naar andere tabellen. Hierop bestaat 1 uitzondering: er mag een join zijn met een referentietabel, of te wel een valueset of code lijst.
In een sat staat alleen de (beteknisvolle code) bijvoorbeeld geslacht: m. In de valuseset tabel staat bij de code m de omschrijving man.

De link tussen een code veld in een sat en een valuset tabel mag nooit integriteit afdwingen. De datavault moet ook niet bestaande of nieuwe codes zonder probleem kunnen verwerken.

Standaard wordt de datavault database uitgerust met een schema 'valset'. Hierin staan code lijsten.

We maken tabellen aan in dit schema door de onderstaande code. Maak eerst in /domainmodel een bestand aan valsets.py::

    from pyelt.datalayers.database import Columns
    from pyelt.datalayers.valset import *

    class Geslacht(DvValueset):
        code = Columns.TextColumn()
        omschrijving = Columns.TextColumn()

Voeg deze python module toe aan de pipeline::

    from domainmodel import valsets
    pipeline.register_valset_domain(valsets)
    pipeline.run()

Na het runnen zal er een tabel zijn aangemaakt in het valset schema.

Een valuesettable heeft standaard al de volgende velden:

 - valueset_naam (text, unique)
 - code (text, unique)
 - omschrijving (text)

(combinatie van valueset_naam en code is uniek)

We hadden de geslachten tabel dus ook punnen definieren als::

    class Geslacht(DvValueset):
        pass


Periodieke valueset
==================

Sommige valueset code hebben een periode van geldigheid. Deze definieer je door::

    class Zorgproduct(DvPeriodicalValueset):
        omschrijving_latijn = Columns.TextColumn()
        omschrijving_consument = Columns.TextColumn()
        declaratie_code_verzekerde_zorg = Columns.TextColumn()

Hierin zitten de volgende vaste standaard velden:

 - valueset_naam (text, unique)
 - code (text, unique)
 - omschrijving (text)
 - ingangsdatum (date, unique)
 - einddatum (date)

(combinatie van valueset_naam, code en ingangsdatum is uniek)

Maar als je goed kijkt zie je in de tabel ook de standaard systeem velden (beginnend met een underscore). Hierin zitten ook al een _insert_date en _finish_date. Is dat niet dubbel op zul je denken?
Deze zijn er om de historie van de historie bij te houden. Dus stel dat de ingansdatum van een code een keer wijzigt, dan wil je weten wat de oude ingangsdatum was en wanneer die is gewijzigd. Dat kun je dan zien aan de _finish_date.


Verwijzen naar een valueset vanuit een sat
==========================================

Het is handig om in je /domainmodel/valsets.py een enum op te nemen met string constanten::

    class ValuesetsEnum:
        # aanmaken van enkele gebundelde string constanten
        geslacht_codes = 'geslachten'
        specialisme_codes = 'specialismen'

Je definieert in de Sat een RefColumn als volgt::

    class Default(Sat):
        geboortedatum = Columns.DateColumn()
        geslacht_code = Columns.RefColumn(ValuesetsEnum.geslacht_codes)



Bij het aanmaken van de tabel zal niks bijzonders te zien zijn. het veld 'gelsacht_code' is van het type text en er is geen constraint. Door het veld toch als RefColumn te definieren en niet als TextColum, ondervindt je straks bij het aanmaken van de views voordeel
