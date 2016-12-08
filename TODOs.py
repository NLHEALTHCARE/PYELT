#ALGEMENE WERKING
#DONE views
#DONE alter db: sat fields
#DONE ddl & database schema versions
# DONE add columns bij bestaande sats
# DONE add columns bij bestaande links
# DONE validaties
# - veldtype validatie
# - veldwaarde validatie
# - link validatie (een op veel)
# - complexe validatie
# DONE uitvallijsten
# DONE hybrid sats
# DONE parent links / is_links
# DONE transformations & functions als database functies afmaken
# DONE uitsluiten van bron velden naar sor
# DONE sats bij links
# DONE mappings voor sats bij links
# DONE valideren mappings uitbreiden met check of sorveld bestaat en goed is gespeld.
# DONE: general_configs: alleen de gekoppelde aan pipeline geldt, harde imports verwijderen
# todo views voor ensemble
# todo datamarts
# todo keys-sat
# DONE docu transformatie en functies

# todo optioneel formatted columns voor kolommen met altijd vast format nodig, mogelijk ook restictedColumns
# todo link mappings refactor (betere naamgeving functies)
# todo koppeltabel voor refs tussen bronwaarden en doelwaarden
# todo oplossing voor Subproject.Kenmerken_sat. Daarin staan afgeleide waarden
# todo: Costiaan ervoor zorgen dat extra sql gelogd wordt in txt bestand (of in database)
# todo: Costiaan exceptions verhuizen naar per laag
# todo unit test framework beter ontwikkelen en zorgen dat het runt bij deployen naar server




##########################
#REFERENTIE
#DONE ref tables
#DONE refs in views waarbij dezelfde ref-type twee keer wordt gebruikt in dezelfde view

##########################
#SYS & LOGGING
#Done runid
#DONE log verder afmaken met tbl name en rowcount
#DONE sysdv.runs eindtijd
#DONE git info in sys.run
#DONE ddl & database schema versions
# optioneel todo sysdv.runs logs ook in database?
# DONE dhw versie ook in runs table
# DONE excepties in aparte log
# DONE DDL-log in aparte dir


##########################
#TESTEN
#DONE UNIT TESTS!!!!!!!!!
#todo Unit tests doorontwikkelen

##########################
#DOCUMENTATIE
#DONE doc
#todo api doc
# DONE transformations & functions als database functies

##########################
#EXTRA UITBREIDINGEN
#DONE validatie van mappings voor runnen (bk moet bestaan, veld type bij bk opgeven)
#DONE validaties binnen de python classes
# todo optioneel proces stappen als losse functies (dan ook conditions!)
# todo optioneel table comments

#########################
#Opdrachten voor ROB
#DONE ROB in pyelt/helpers een python file maken die logs/ map leegmaakt:
# zie pyelt.helpers.log_deleters.py

#########################
#Opdrachten voor HJ
# todo: unittests rob checken
