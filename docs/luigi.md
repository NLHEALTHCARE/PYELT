



Luigi skeduler framework implementeren in pyelt
==============================================

Doelstelling
------------

  - onderdelen van pyelt (pipes) afzonderlijk per laag kunnen opstarten of herstarten als er iets fout gaat
  - flexibiliteit in skeduler: sommige pipes dienen eens per maand te runnen, anderen eens per dag.
  - overzicht creeren in afhankelijkheden, welke taak moet eerst zijn gedaan, voordat andere kan worden gedaan
  - run proces inzichtelijk maken via web user interface 
  - inzicht in performance
  - processen parallel kunnen aftrappen
  
Plan van Aanpak
--------------

1. ontdekkingsfase: via tutorials en tryouts kijken wat luigi framework kan
2. globaalontwerp fase: bovenstaande doelstellingen aanscherpen en globaal FO
3. detail ontwerp fase: FO in detail uitwerken
4. technisch ontwerp fase met ureninschatting

Bovenstaande zal cyclisch verlopen. Dus telkens weer beginnen bij 1.

1. ontdekkingsfase
----------------

In luigi kun je taken aan elkaar knopen en deze runnen in een skeduler. Via een webinterface wordt inzichtelijk gemaakt welke taken hebben gerund en welke hebben gefaald. Via output files houdt luigi bij wat de status is van de taak (moet je zelf programmeren) waardoor je een proces eenvoudig twee keer kunt opstarten zonder dat de wel geslaagde taken nog een keer runnen.
Via de webinterface wordt een graph getoond van de afhankelijkheden tussen taken.

 - history wordt standaard niet bijgehouden in web interface. Als de server down is geweest is history kwijt. Luigi biedt hiervoor oplossing. Kan alleen webdeel niet vinden. Ook is dit niet stabiel, op een gegeven moment toch weer alle history kwijt.

- je kunt geen schema opgeven in de connectie waar history tabellen moeten worden aangemaakt. postgress lijkt niet te werken (??)
 
 - om omschrijvingen aan een taak te geven, kun je een paramater aanmaken met een default waarde. Je kunt hier html in stoppen. De parameter wordt getoond in de log.
 Misschien kunnen we hier een link aanmaken naar de log met substappen van de taak 

- Het is niet mogelijk om taken te nesten in processen en te drilldownen in de webinterface

- Het is niet mogelijk om per run te groeperen. Alle taken worden onder elkaar getoond.

- Debuggen gaat lastig of niet (stap werkt niet...?)

- info op het web is moeilijk te vinden. 

- Bij spotify zijn ze voor deltail taken over gestapt op een andere framework, maar gebruiken ze dit voor de grote onderdelen (bron ?)

- Ik zou zelf taken nog extra andere properties willen geven: is_active, log(history), description, duration (start&end-time), config (laatste kan als parameter)

- En specifiek voor pyelt de info uit de config (bron, doel, sql, bestand enz) en de mappings  

- Errors worden goed weer gegeven



2. globaal ontwerp
----------------

pipeline en pipe stappen opknippen in globale taken. Dit worden luigi taken

- pipeline voor bereiding
- per pipe de ddl voor de sor laag
- per pipe de etl voor de sor laag
- ddl van de valset laag
- ddl van de dv laag
- per pipe de etl van de valset laag
- per pipe de etl van de dv laag
- pipeline sluiting


De detail stappen (wat naar welke tabel gaat enz), zullen we niet als luigi-taken definieren. (?)

De configs (globale en per pipe) zijn dicts, deze wordt als parameter meegegeven, of wordt geopend in de taak. Zou misschien mooi zijn deze te kunnen editen via de web interface. Voor mappings idem. Dat doen we voorlopig niet. (?)

3. detail ontwerp
----------------

- nadenken over runid. hoe vormgeven?
- nadenken over logging, in db of niet
- nadenken parrallel processen
- enz


4. TO
----

Huidige runproces zal moeten worden opgeknipt in stukjes. Hieronder globaal het huidige runproces:

send_start_mail -> create_schemas_if_not_exists -> create_sys_tables -> create_new_runid -> create_loggers -> validate_domains (if OK) -> validate_mappings_before_ddl (if OK) -> create_valueset_from_domain (create tables) -> create_dv_from_domain (create tables)  >
[[[PER PIPE: -> DDL sor (create tables) -> run_extra_sql -> create_db_functions]]]  ->
(create_datamarts) -> validate_mappings_after_ddl (if OK) ->
[[[PER PIPE: run_before_sor -> source_to_sor -> validate_duplicate_keys -> validate_sor -> run_after_sor ->
sor_to_valuesets -> sor_to_entity -> sor_to_link]]] ->
end_run -> send_log_mail -> END
