


Luigi skeduler framework implementeren in pyelt
==============================================

Doelstelling
------------

  - onderdelen van pyelt (pipes) afzonderlijk per laag kunnen opstarten of herstarten als er iets fout gaat
  - flexibiliteit in skeduler: sommige pipes dienen eens per maand te runnen, anderen eens per dag.
  - overzicht creeren in afhankelijkheden, welke taak moet eerst zijn gedaan, voordat andere kan worden gedaan
  - run proces inzichtelijk maken via web user interface 
  
Plan van Aanpak
--------------

1. ontdekkingsfase: via tutorials en tryouts kijken wat luigi framework kan
2. globaalontwerp fase: bovenstaande doelstellingen aanscherpen en globaal FO
3. detail ontwerp fase: FO in detail uitwerken
4. technisch ontwerp fase met ureninschatting

Bovenstaande zal cyclisch verlopen. Dus telkens weer beginnen bij 1.

1 ontdekkingsfase
----------------

In luigi kun je taken aan elkaar knopen en deze runnen in een skeduler. Via een webinterface wordt inzichtelijk gemaakt welke taken hebben gerund en welke hebben gefaald. Via output files houdt luigi bij wat de status is van de taak (moet je zelf programmeren) waardoor je een proces eenvoudig twee keer kunt opstarten zonder dat de wel geslaagde taken nog een keer runnen.
Via de webinterface wordt een graph getoond van de afhankelijkheden tussen taken.


Beperkingen
Het is niet mogelijk om taken te nesten in elkaar en te drilldownen in de webinterface

2 globaal ontwerp
----------------


4 TO
----

Huidige runproces zal moeten worden opgeknipt in stukjes. HIeronder globaal het huidige runproces:

send_start_mail -> create_schemas_if_not_exists -> create_sys_tables -> create_new_runid -> create_loggers -> validate_domains (if OK) -> validate_mappings_before_ddl (if OK) -> create_valueset_from_domain (create tables) -> create_dv_from_domain (create tables)  >
[[[PER PIPE: -> DDL sor (create tables) -> run_extra_sql -> create_db_functions]]]  ->
(create_datamarts) -> validate_mappings_after_ddl (if OK) ->
[[[PER PIPE: run_before_sor -> source_to_sor -> validate_duplicate_keys -> validate_sor -> run_after_sor ->
sor_to_valuesets -> sor_to_entity -> sor_to_link]]] ->
end_run -> send_log_mail -> END
