Pipeline
========

De pipeline is de basis van pyelt. In de pipeline worden de processen klaargezet waarmee de datavault wordt aangemaakt en wordt gevuld.
Het bevat de verbinding naar de datavault-batabase, en ook alle lagen en alle bronnen.
De datavault bestaat uit verschillende lagen:

- sor(s)
- rdv
- dv
- datamart(s)

Voorbeeld::

    from pyelt.pipeline import Pipeline
    from configs import general_config

    pipeline = Pipeline(general_config)
    datawarehouse = pipeline.dwh
    dv = datawarehouse.dv
    #dv is een Schema object
    print(dv.name)
    >> dv

Opmerking:
Een pipeline is een singleton implementatie; er is altijd maar 1 pipeline object voor alle processen. Elke
nieuwe pipeline die wordt aangemaakt zal dezelfde waardes bevatten.





Pipe
====

Een pipeline bevat 1 of meerdere pipes. Een pipe is een verbinding met 1 bronsysteem, maar bevat wel alle lagen in de
datavault.

Bijvoorbeeld, we maken een pipe aan met de naam 'timeff', met als bronsysteem een oracle database en 'sor_timeff' als de
naam van het sor schema. Voeg in configs.py de configs toe van de bronlaag die je wilt ontsluiten::


        timeff_config = {
            'source_connection': 'oracle://SID:pwd@server/db',
            'default_schema': 'MTDX',
            'sor_schema': 'sor_timeff'
        }

We maken een pipe aan door ge get_or_create_pipe() aan te roepen van de pipeline::
        pipe = pipeline.get_or_create_pipe('timeff', timeff_config)
        sor = pipe.sor
        print(sor.name)
        >> sor_timeff
        source_db = pipe.source_db
        print(source_db.default_schema.name)
        >> MTDX

Indien bij het aanmaken van de pipe het sor schema nog niet bestaat, dan wordt deze eerst aangemaakt.
