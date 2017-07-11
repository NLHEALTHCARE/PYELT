Performance tests SOR
=====================

*Uitzoeken wat indexes kunnen betekenen*

Een aantal tests uitgevoerd. Op locaal systeem, bron database was een andere postgres db.

 - [tests1] (/results/tests1.md): diversie vaste indexes op verschillende velden met 100.000 rijen en telkens 10.000 rijen geupdate voor een volgende run.
 De velden die een index betroffen zijn _hash, {sleutelveld} en _runid.
 - [results/tests2.md]: als test 1 maar nu met 500.000 rijen en een guid ipv een volgnummer
 - [results/tests3.md]: als test 2 maar indexes voor insert eerst verwijderen en hierna wee aanmaken.

 Naast gedocumenteerde tests heb ik meer uitgeprobeerd, maar telkens geen resultaat.


 Conclusie
 ----------
 Met indexes is telkens iets langzamer dan zonder.

 Indexes leveren pas veel op bij veel zoek activiteit en zijn nadelig bij insert activiteit.

 Dus als mensen gaan querieen op de osr dan kunnen we indexes aanmaken, maar dan niet op de techniesche velden maar op de beteknisvolle velden.


