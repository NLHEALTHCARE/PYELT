Validaties
==========


Het principe van datavault is dat alle data wordt ingelezen en dat er pas achteraf validaties worden gedaan.
Data die niet voldoet wordt in een foutoverzicht gerapporteerd aan de gebruikers zodat in de bron correcties kunnen worden gedaan.
Tijdens een volgende run zal de nieuwe correcte data als actueel gaan gelden waarbij de historie met de fout toch blijft bewaard.

Er zijn twee momenten van validaties mogelijk
  1. na inlezen in sor
  2. na inlezen in dv

1. validaties na inlezen in sor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Hier zal men alleen de validaties moeten toepassen waardoor inlezen in de dv laag wordt bemoeilijkt, zoals dubbelingen in de business keys.
We definieren dat als volgt::

    validations = []
    validation = SorValidation(tbl='patient_hstage', schema=self.pipe.sor)
    validation.msg = 'Ongeldige postcode'
    validation.sql_condition = "postcode like '0000%'"
    validations.append(validation)

    self.pipe.validations.extend(validations)

Stel in bovenstaande voorbeeld wordt postcode gebruikt als onderdeel van de bk voor de adres-hub, en in de bron gegevens bestaan meerdere straatnamen met deze 'lege' postcode,
dan zal dit tijdens het inlezen van sor naar dv problemen gaan geven, omdat postcode onderdeel uitmaakt van de bk.
De gegevens die niet voldoen krijgen in de database een markering dat ze niet geldig zijn, zodat ze niet worden meegenomen in de etl.
De database velden die hier betrekking op hebben zijn:

 - _valid           -> FALSE
 - _validation_msg  -> 'Ongeldige postcode'

Bij afronding van de etl worden alle waarden uit alle tabellen die niet voldoen naar de _exception tabel gecopieerd, zodat ze kunnen worden gerapporteerd.

Andere mogelijkheid om te testen op dubbelingen is::

    validation = SorValidation(tbl='persoon_hstage', schema=pipe.sor)
    validation.msg = 'Dubbel bsn'
    validation.set_check_for_duplicate_keys('ifct_bsn')

Een van de twee dubbelen zullen worden gemarkeerd als ongeldig en de andere kan nog wel doorgaan naar de dv.

2. validaties na inlezen in dv
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In plaats van sql, gebruiken we python syntax (set_condition()) om de conditie te definieren, zoals in het fictieve voorbeeld hieronder::

    validations = []
    validation = DvValidation()
    validation.msg = 'te jong'
    validation.set_condition(Patient.Default.geboortedatum >= '2000-01-01')
    validations.append(validation)

    self.pipe.validations.extend(validations)





