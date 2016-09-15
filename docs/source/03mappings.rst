Mappings
========

Een etl proces bevat mappings, waarmee bronvelden kunnen worden gemapt aan doelvelden.


Van bron naar sor
-----------------

We mappen een brontabel(view) aan een hstage tabel waarbij alle kolommen automatisch worden gemapt op basis van de
veldnamen::

    pipeline = Pipeline()
    pipe = pipeline.get_or_create_pipe('timeff', timeff_config)
    source_db = pipe.source_db

    source_tbl = SourceTable('V_INTF_CONTACT', source_db.default_schema, source_db )
    source_tbl.set_primary_key(['id'])
    mapping = SorMapping(source_tbl,'persoon_hstage', auto_map = True)


Let wel: het is noodzakelijk dat je aangeeft wat in de brontabel de primary key is.

Door "auto_map = True" te gebruiken worden de veldnamen uit de bron identiek overgenomen in de hstage tabel van de sor.
Alle veldtypen in de sor laag worden als text met onbeperkte lengte gedefinieerd zodat er nooit dataverlies zal zijn
door onjuiste casts.
In plaats van auto_map = True kun je ook individuele velden mappen...#todo? want ...? of typo?

De mapping voeg je vervolgens toe aan een pipe::

    pipe.mappings.append(mapping)


Van sor naar dv-entities
------------------------

Een datavault bestaat uit hubs, sat en links. Een hub met 1 of meerdere sats wordt een entity genoemd.
Eerst mappen we de sor tabel op een entity::

    sor = pipe.sor
    mapping = EntityMapping('persoon_hstage', Persoon, sor)
    mapping.map_bk("'timeff_' || relatienr")
    mapping.map_field('achternaam', Persoon.Personalia.achternaam)
    mapping.map_field('voornaam', Persoon.Personalia.voornaam')
    mapping.map_field('voorletters', Persoon.Personalia.voorletters')
    mapping.map_field('tussenvoegsels', Persoon.Personalia.tussenvoegsels')

    mapping.map_field('legitimatietype::integer', Persoon.Identificatie.legitimatietype')
    mapping.map_field('legitimatieid', Persoon.Identificatie.legitimatieid')
    mapping.map_field('legi_geldigtot::timestamp', Persoon.Identificatie.legitimatie_geldigtot')
    mapping.map_field('rijbewijsnummer', Persoon.Identificatie.rijbewijsnummer')
    mapping.map_field('rijb_geldigtot::timestamp', Persoon.Identificatie.rijbewijs_geldigtot')
    mapping.map_field('bsn', Persoon.Identificatie.bsn')

Een mapping tussen een sor tabel en een entity bevat meerdere field mappings. Deze worden aangemaakt door de functie
map_field() aan te roepen.
In bovenstaand voorbeeld wordt de business key van de hub bepaald door de tweede regel '.map_bk()'.
Vervolgens worden twee sats gemapt vanuit deze entity (hub), namelijk personalia en identificatie.

Het bronveld kan in beide gevallen worden vervangen door een geldig stukje sql, zoals bijvoorbeeld de concat of de casts die
in bovenstaand voorbeeld gebruikt zijn.


Filter
^^^^^^

Aan een tabel-mapping is een filter mee te geven. Alleen de gegevens uit de bron die voldoen aan de filter worden dan
geïmporteerd. Filter is een geldige sql-where-string::

    patient_filter = "soort = '9'"
    mapping = EntityMapping('persoon_hstage', Patient, pipe.sor, filter=patient_filter)

Hybrid sats
^^^^^^^^^^

Bij een hybidsat map je meer keer ophetzelfde doel veld, maar telkens van een ander type::

    mapping.map_field('telefoon', Patient.ContactGegevens.telnummer, type=Patient.ContactGegevens.Types.telefoon)
    mapping.map_field('mobiel', Patient.ContactGegevens.telnummer, type=Patient.ContactGegevens.Types.mobiel)

De constanten Patient.ContactGegevens.Types zijn hier strings. Onderstaande code zou dus ook geldig zijn, hoewel minder robuust::

    mapping.map_field('fax', Patient.ContactGegevens.telnummer, 'fax')


Van sor naar dv-links
^^^^^^^^^^^^^^^^^^^^^

Een link mapping betreft de mapping van een historische stage tabel (SorTable) op een link-tabel.
Een link tabel bestaat uit een aantal vaste velden en foreign keys naar 2 of meer hubs. In de linkmapping moeten we
definiëren hoe de brongegevens mappen op de bk's van deze hubs::

Stel dat de tabel persoon_hstage naast persoonsgegevens (zie boven) ook adres gegevens bevat die horen bij deze persoon,
maar dat we die in een aparte entiteit "adres" willen hebben::

    mapping = EntityMapping('persoon_hstage', Adres, pipe.sor)
    mapping.map_bk("COALESCE(postcode, '') || COALESCE(huisnr, '')")
    mapping.map_field('straat', Adres.Default.straat)
    mapping.map_field('huisnr', Adres.Default.huisnummer)
    mapping.map_field('postcode', Adres.Default.postcode)
    mapping.map_field('plaats', Adres.Default.plaats)
    pipe.mappings.append(mapping)

    link_mapping = LinkMapping('persoon_hstage', Persoon_Adres_Link, pipe.sor)
    link_mapping.map_entity(Persoon_Adres_Link.patient)
    link_mapping.map_entity(Persoon_Adres_Link.adres)
    pipe.mappings.append(link_mapping)

De tabel persoon_hstage wordt dus op twee entiteiten (hubs; Persoon en Adres) gemapt. Het etl proces zal zelf de link
vullen.

Link tabel gaat de velden krijgen:

- _fk_persoon_hub
- _fk_adres_hub


Hybrid links
------------
Het is ook mogelijk dat een persoon meerdere adressen heeft. Hiervoor kunnen we hybrid-links definiëren. Hybrid-links
zijn links, waarin er naast de foreign keys ook een type-veld is gedefenieerd.

We doen dat als volgt::

    mapping = EntityMapping('persoon_hstage', Adres, pipe.sor, type='woon')
    mapping.map_bk("COALESCE(postcode_w, '') || COALESCE(huisnr_w, '')")
    mapping.map_field('straat_w', Adres.Default.straat)
    mapping.map_field('huisnr_w', Adres.Default.huisnummer)
    mapping.map_field('postcode_w', Adres.Default.postcode)
    mapping.map_field('plaats_w', Adres.Default.plaats)
    pipe.mappings.append(mapping)

    link_mapping = LinkMapping('persoon_hstage', Persoon_Adres_Link, pipe.sor, type='woon')
    link_mapping.map_entity(Persoon_Adres_Link.patient)
    link_mapping.map_entity(Persoon_Adres_Link.adres, type='woon')
    pipe.mappings.append(link_mapping)

    mapping = EntityMapping('persoon_hstage', Adres, pipe.sor, type='post')
    mapping.map_bk("COALESCE(postcode_p, '') || COALESCE(huisnr_p, '')")
    mapping.map_field('straat_p', Adres.Default.straat)
    mapping.map_field('huisnr_p', Adres.Default.huisnummer)
    mapping.map_field('postcode_p', Adres.Default.postcode)
    mapping.map_field('plaats_p', Adres.Default.plaats)
    pipe.mappings.append(mapping)

    link_mapping = LinkMapping('persoon_hstage', Persoon_Adres_Link, pipe.sor, type='post')
    link_mapping.map_entity(Persoon_Adres_Link.patient)
    link_mapping.map_entity(Persoon_Adres_Link.adres, type='post')
    pipe.mappings.append(link_mapping)

De Link tabel gaat de velden krijgen:

- type       -> (woon of post)
- _fk_persoon_hub
- _fk_adres_hub


Dynamic links
------------

Bij een dynamische link verwijst dezelfde fk naar verschillende hubs. Je mapt hem als volgt::

    link_mapping = SorToLinkMapping('handeling_hstage', PatientHandelingLink, pipe.sor)
    link_mapping.map_bk("patientnummer", PatientHandelingLink.Patient)
    link_mapping.map_entity(PatientHandelingLink.Handeling)
    link_mapping.map_entity(PatientHandelingLink.Dynamic, type=PatientHandelingLink.Types.hulpverlener)

type verwijst naar een entiteit.
In bovenstaande voorbeeld is vereiste dat de handeling_hstage tabel al eerder is gemapt op den hulpverlener. Hierdoor is er namelijk in de hstage tabel een _fk_hulpverlener_hub verschenen.
Is dat niet het geval dan dien je als volgt te mappen::

    link_mapping = SorToLinkMapping('handeling_hstage', PatientHandelingLink, pipe.sor)
    link_mapping.map_bk("patientnummer", PatientHandelingLink.Patient)
    link_mapping.map_entity(PatientHandelingLink.Handeling)
    link_mapping.map_bk("hulpverlener_agb_code", PatientHandelingLink.Dynamic, type=PatientHandelingLink.Types.hulpverlener)

of::

    link_mapping.map_entity(PatientHandelingLink.Dynamic, bk="hulpverlener_agb_code", type=PatientHandelingLink.Types.hulpverlener)


Links met joins over meerdere brontabellen
------------------------------------------

In de bovenstaande voorbeelden werden links uit dezelfde brontabel gehaald. Het is ook mogelijk dat er links over
meerdere bron tabellen moeten worden gedefinieerd. Hiervoor gebruiken we de join syntax::

    link_mapping = LinkMapping('subtraject_hstage', Patient_Zorgtraject_Link, pipe.sor)
    link_mapping.map_bk("'timeff_' || relatienr", Patient_Zorgtraject_Link.patient)
    link_mapping.map_entity(Patient_Zorgtraject_Link.zorgtraject)
    mappings.append(link_mapping)


Transformaties
--------------

Tijdens het etl proces kunnen transformaties nodig zijn. Transformations zijn stukjes sql of verwijzigen naar sql
functies. Een transformatie heeft betrekking op het bronveld.

zie 05transformaties



Validaties
----------

Een validatie heeft betrekking op het doelveld. Nadat een dataveld is geïmporteerd, kan het worden gevalideerd. Indien
blijkt dat een doelveld niet voldoet aan een bepaalde eis dan wordt er een markering gemaakt in het record
'_valid=False'. De reden hiervoor wordt weggeschreven in het veld '_validation'.

zie 05validaties






