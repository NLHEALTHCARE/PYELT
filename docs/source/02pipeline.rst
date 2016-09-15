Pipeline
========

De pipeline omvat alle lagen en alle bronnen. De pipeline bevat de databaseverbinding naar de datavault-datawarehouse,
die bestaat uit de verschillende lagen:

- sor(s)
- rdv
- dv
- datamart(s)

Voorbeeld::

    pipeline = Pipeline()
    datawarehouse = pipeline.dwh
    rdv = datawarehouse.rdv
    dv = datawarehouse.dv

Opmerking:
Een pipeline is een singleton implementatie; er is altijd maar 1 pipeline object voor alle processen. Elke
nieuwe pipeline die wordt aangemaakt zal dezelfde waardes bevatten.


Pipe
====

Een pipeline bevat 1 of meerdere pipes. Een pipe is een verbinding met 1 bronsysteem, maar bevat wel alle lagen in de
datavault.

Bijvoorbeeld, we maken een pipe aan met de naam 'timeff', met als bronsysteem een oracle database en 'sor_timeff' als de
naam van het sor schema::

        timeff_config = {
            'source_connection': 'oracle://SID:pwd@server/db',
            'default_schema': 'MTDX',
            'sor_schema': 'sor_timeff'
        }
        pipe = pipeline.get_or_create_pipe('timeff', timeff_config)
        sor = pipe.sor
        print(sor.name)
        >> sor_timeff
        print(sor.source_db.default_schema.name)
        >> MTDX

Indien bij het aanmaken van de pipe het sor schema nog niet bestaat, dan wordt deze eerst aangemaakt.
