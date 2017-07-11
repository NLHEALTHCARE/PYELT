Eerste mogelijkheid
===================

100.000 rijen, id = volgnummer, telkens tussendoor update van 10.000 rijen


**Conclusie:** nauwelijks verschil met of zonder, zonder is iets sneller


test 1
-------
inlezen zonder indexes

10.000 rijen geupdate (2 velden; text en num veld)

duur: 12 sec

her (0-nieuwe rijen erbij) 4 sec

test2
-----
indexes maken (5 keer op temp table en op hstage table)

duur 2 sec

test3
-----
inlezen met 5 indexes

10.000 rijen geupdate (2 velden; text en num veld)

duur                        13 sec

her (0-nieuwe rijen erbij)  6 sec

test4
-----
tweede keer inlezen zonder indexes

10.000 rijen geupdate (2 velden; text en num veld)

duur                        10 sec

her (0-nieuwe rijen erbij)  4 sec

test5
-----
inlezen met alleen indexes op temp

10.000 rijen geupdate (2 velden; text en num veld)

duur                        13.5 sec

her (0-nieuwe rijen erbij)  5 sec

test6
-----
inlezen met alleen indexes op _hash velden in temp table en hstage

10.000 rijen geupdate (2 velden; text en num veld)

duur                        12 sec

her (0-nieuwe rijen erbij)  5 sec

test7
-----
weer helemaal zonder

10.000 rijen geupdate (2 velden; text en num veld)

duur                        12 sec

her (0-nieuwe rijen erbij)  6 sec