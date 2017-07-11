Tweede mogelijkheid
===================

500.000 rijen, id = guid, telkens tussendoor update van 10.000 rijen.
Nu in code halverwege proces indexes droppen en opnieuw aanmaken


**Conclusie:** win je niks mee. Het opnieuw aanmaken van de indexes kost 18 seconden

**let wel:** Er zitten nu > 9 revisions in de database. Alles ging dus slomer dan tests2

test 1
------
inlezen zonder indexes

10.000 rijen geupdate (2 velden; text en num veld)

duur: 1:16 sec


test 2
------
inlezen met indexes eerst uitgezet voor insert daarna opnieuw aangemaakt voor update revision

10.000 rijen geupdate (2 velden; text en num veld)

duur: 1:42 sec
