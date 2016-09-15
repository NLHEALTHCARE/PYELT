Config
======

Pyelt wordt opgeleverd met twee mappen:

- pyelt:             
  De map met de core van pyelt. Deze map wordt onderhouden door het pyelt core ontwikkelteam.
- etl_processes:     
  De map waarin de etl-processen van je eigen project staan. Per bronsysteem maak je een submap aan,
  waarin je in python code je eigen pyelt etl-proces definieert.

In de root van etl_processes staat het bestand config.py. Hierin zet je o.a. de verbinding(en) naar de database(s)::

    config = {
        'log_path': '\\logs\\',
        'conn_dwh': 'postgresql:/user:pwd@server/database',
        'debug': False, #Zet debug op true om een gelimiteerd aantal rijen op te halen
        'datatransfer_path': '/tmp/pyelt/datatransfer' #voor linux server: datatransfer pad mag niet in /home folder
                                                       # zijn, want anders kan postgres er niet bij.
    }

De database bestaat uit meerdere schema's. Per laag is er een eigen schema (zoals bijvoorbeeld sor, dv, sys). Iedere laag maakt
gebruik van dezelfde database verbinding.

