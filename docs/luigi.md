



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

  1. pipeline voorbereiding
  2. per pipe de ddl voor de sor laag
  3. per pipe de etl voor de sor laag
  4. ddl van de valset laag
  5. ddl van de dv laag
  6. per pipe de etl van de valset laag
  7. per pipe de etl van de dv laag
  8. pipeline sluiting

Omdat de focus voorlopig alleen op de sor-laag ligt zullen we stappen 1,2,3 en 8 de hoogste prioriteit geven; hierna de stappen 4 en 6 en hierna de stappen 5 en 7.

De detail stappen (wat naar welke tabel gaat enz), zullen we **niet** als luigi-taken definieren.

**LET WEL** We houden ons in de eerste cycle alleen aan luigi zoals het nu is, aanpassen van het luigi framework zelf doen we niet.

3. detail ontwerp
----------------

  - **runid**:
  in pyelt wordt de bestaat de runid uit twee compontenten: een getal voor de komma en een getal achter de komma (bijv 2.01). Het getal voor de komma wordt iedere dag verhoogd. Het getal achter de komma wordt verhoogd als de run de zelfde dag opnieuw draait.
  Dus: run 3.00 is de eerste run op dag 3. Run 4.02 is de tweede run op dag 4 <br/>
  In luigi maakt de runid de taken uniek en hiermee weet luigi welke taken aan elkaar gekoppeld zijn. We laten hierom de bovenstaande logica in principe intact. Is er echter een fout opgetreden in het proces dan dient hetzelfde proces met dezelfde runid nog een keer te worden gestart. <br/>
  Wanneer we assyncroon gaan draaien door per dag parallelle processen op te starten zullen deze te zien zijn aan het getal achter de komma. 

  - **initialisatie van de runid**: omdat, zoals hierboven is gezegd dat de runid een taak uniek maakt, zal het ophogen van de runid zelf geen luigi taak worden, maar zal de nieuwe runid worden aangemaakt, voordat het luigi-proces wordt aangeroepen.
  Als er een fout is opgetreden dan dient het proces met dezelfde runi opnieuw te worden gestart. Dit wordt een parameter die handmatig gezet moet worden, omdat het proces bij fouten sowieso handmatig moet worden gestart.

  - **eerste initialisatie van de database** In pyelt wordt voordat een nieuwe runid wordt aangemaakt eerst de database geinitialiseerd. Immers, als er nog geen sys.runs tabel bestaat moet deze eerst door het pyelt framework worden aangemaakt. Gezien het bovenstaande valt de eerste initialisatie van de database buiten luigi.
  
  - **logging van detail stappen**: Logging gaat momenteel in txt files. Dit wordt gedaan door pyelt. Globale logging per task wordt gedaan door luigi. Een aanpassing aan het pyelt framework die moet gebeuren is dat iedere pipe een eigen log krijgt, in plaats van een global log.
  De sql log en ddl-log mogen wel globaal blijven, mits dat niet bots met parrallelle processen.
  Luigi-taskId meegeven aan pyelt-log zou mooi zijn (indien mogelijk nice to have)

  - **parrallel processen**: parallelle processen start je op door meerdere entries in crontab te zetten. Afhankelijkheden onderling regelt luigi zelf al shet goed is. 
Database locks zullen in de sor laag niet optreden omdat iedere sorlaag een eigen schema krijgt en de parallelle processen nooit over dezelfde sorlaag zullen gaan.

  - **foutafhandeling**: als er in pyelt een fout optreedt bij een substapje, dan wordt deze gelogd en wordt doorgegaan met devolgende stap. In luigi willen we kunnen zien of een fout is opgetreden. Het pyeltframework zal dus fouten moeten opsparen om aan het einde van de hele taak de fout te kunnen doorgeven aan luigi, zodat deze de taak al fout kan markeren. 



4. TO
----

Huidige runproces zal moeten worden opgeknipt in stukjes. Hieronder globaal het huidige runproces:

send_start_mail -> create_schemas_if_not_exists -> create_sys_tables -> create_new_runid -> create_loggers -> validate_domains (if OK) -> validate_mappings_before_ddl (if OK) -> create_valueset_from_domain (create tables) -> create_dv_from_domain (create tables)  >
[[[PER PIPE: -> DDL sor (create tables) -> run_extra_sql -> create_db_functions]]]  ->
(create_datamarts) -> validate_mappings_after_ddl (if OK) ->
[[[PER PIPE: run_before_sor -> source_to_sor -> validate_duplicate_keys -> validate_sor -> run_after_sor ->
sor_to_valuesets -> sor_to_entity -> sor_to_link]]] ->
end_run -> send_log_mail -> END

Dit wordt verdeeld in de volgende stukken/taken:

- pipeline voorbereiding
- per pipe de validaties voor de sor laags
- per pipe de ddl voor de sor laag
- per pipe de etl voor de sor laag
- pipeline validaties
- ddl van de valset laag
- ddl van de dv laag
- per pipe de etl van de valset laag
- per pipe de etl van de dv laag
- pipeline sluiting

Ieder stuk moet zelf zijn op te starten; ieder stuk moet eigen logging krijgen.

Je kunt in luigi helaas geen objecten als parameter doorgeven van de ene aan de andere taak. Hierom moeten we het pipeline object (het object dat de verbinding naar de database bevat en de andere globale instellingen, zoals global config en globale logging) telkens opnieuw aanmaken bij iedere taak.


Bekende tekortkomingen van luigi
--------------------------------

Zoals eerder genoemd zullen we indeze eerste sprint geen aanpassingen doen aan luigi zelf. Hierom is het onzeker of onderstaande bevindingen zullen zijn opgelost. Ik heb namelijk ondervonden dat goede documentatie moeilijk is te vinden.

  - History van taken wordt standaard niet getoond. Je bent na een dag de history weer kwijt. En ook na herstarten van de server.
  - Onzeker of de run-duur van een taak kan worden weergegeven.
  - Luigi biedt geen inzicht in detail stappen, soms bestaat een taak namelijk uit 10 stappen waarbij er meerdere tabellen worden gevuld en geupdate. Je krijgt dus geen inzicht in hoelang het vullen/wijzigen van een specifieke tabel duurt.
  - Je kunt in luigi geen taken disablen, omdat ze bijvoorbeeld niet gerund hoeven worden. Deze taken kun je alleen maar uitsluiten en dan zie je deze taak niet in het diagram.
  - zie verder ontdekkingsfase voor andere tekortkomingen.


Om bovenstaande features wel te implementeren moeten we mogelijk overgaan op een ander framework dan luigi of zelfbouw. Vooral omdat documentatie beperkt is als het gaat om specifieke features.








