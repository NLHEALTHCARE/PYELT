Tweede mogelijkheid
===================

500.000 rijen, id = guid, telkens tussendoor update van 10.000 rijen


**Conclusie:** nauwelijks verschil met of zonder, zonder indexes is iets sneller


test 1
------
inlezen zonder indexes

10.000 rijen geupdate (2 velden; text en num veld)

duur: 56 sec

her (0-nieuwe rijen erbij) 21 sec

test 2
------
inlezen met indexes op id veld in _temp_hash en hstage

10.000 rijen geupdate (2 velden; text en num veld)

duur: 1:05 sec

her (0-nieuwe rijen erbij) 25 sec

test 3
------
inlezen met indexes op _temp(2 keer) in _runid in  hstage

10.000 rijen geupdate (2 velden; text en num veld)

duur: 1:06 sec

test 4
------
inlezen zonder indexes

10.000 rijen geupdate (2 velden; text en num veld)

duur: 1:01 sec

her (0-nieuwe rijen erbij) 25 sec

test 5
------
inlezen zonder indexes

10.000 rijen geupdate (2 velden; text en num veld)

duur: 1:01 sec

her (0-nieuwe rijen erbij) 25 sec