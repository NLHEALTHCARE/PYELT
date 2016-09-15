Transformaties
==============

Tijdens een ETL proces zijn doorgaans vele transformaties nodig van velden. Velden moeten worden gesplits, samengevoegd, of types dienen te worden gewijzigd.
Hiertoe hebben we (postgres pl) SQL tot onze beschikking. Het kan op drie manieren:

 1. direct-inline: als SQL opnemen in de mappings
 2. indirect-inline: als Pyelt-transformatie opnemen in de mappings (wordt vertaald naar inline sql)
 3. indirect-dbfunction: als PgPl- functie opnemen in de database

1. direct-inline
^^^^^^^^^^^^^^^^

Zie onderstaande voorbeeld::

    mapping.map_field('LEFT(hstg.postcode, 4)', Patient.Adres.postcode)
    mapping.map_field('geboortedatum::date', Patient.Default.geboortedatum)

In de eerste regel wordt de SQL functie LEFT() aangeroepen tijdens de etl-stap waarin de data van de hstage tabel naar de dv-sat wordt geschreven.
In de tweede regel wordt een cast gedaan van text naar date

2. indirect-inline
^^^^^^^^^^^^^^^^^^

Stel we krijgen te maken met onderstaande voorbeeld::

    mapping.map_field("CASE WHEN to_date(lpad(geboorte_datum, 8, '0'), 'DDMMYYYY') = '1901-01-01' OR to_date(lpad(geboorte_datum, 8, '0'), 'DDMMYYYY') = '0001-01-01 BC' THEN NULL ELSE to_date(lpad(geboorte_datum, 8, '0'), 'DDMMYYYY') END", Patient.Default.geboortedatum)

Dit zou nauwelijks meer te lezen zijn. Hierom kunnen we het aanpassen naar hetvolgende::

    def text_to_date_transform(field_name):
        transform = FieldTransformation()
        transform.field_name = field_name
        transform.new_step("lpad({fld}, 8, '0')")
        transform.new_step("to_date({fld}, 'DDMMYYYY')")
        transform.new_step("CASE WHEN {fld} = '1901-01-01' OR {fld} = '0001-01-01 BC' THEN NULL ELSE {fld} END ")
        return transform

    mapping.map_field(text_to_date_transform('geboorte_datum'), Patient.Default.geboortedatum);

We maken een transformatie die bestaat uit drie stappen. Met new_step() definieren we een stap. De eerste is het aanvullen van nullen aan het veld tot 8 posities.
Daarna wordt het text veld naar datum type omgezet en ten slotte worden de datums 1901-01-01 en 0001-01-01 omgezet naar NULL.
In de mapping mappen we van transformatie op het doelveld. Tijdens de etl zal deze mapping worden omgezet tot de 'onleesbare' regel hierboven.


3. indirect-dbfunction
^^^^^^^^^^^^^^^^^^^^^^

Om nog ingewikkeldere bewerkingen te doen kunnen we ook functies aanmaken op de database. De functies dienen in etl_processes te worden gedefinieerd zodat de ddl ervan traceerbaar zal zijn.
Het definieren van een functie in de python-module test_db_functions gaat als volgt::

    class CreateAgb(DbFunction):
        def __init__(self, zorgtype_calling_field='', agb_calling_field=''):
            super().__init__()
            param1 = DbFunctionParameter('zorgtype', 'text', zorgtype_calling_field)
            param2 = DbFunctionParameter('agb', 'text', agb_calling_field)
            self.func_params.append(param1)
            self.func_params.append(param2)
            self.sql_body = """DECLARE
                complete_agb TEXT := '';
            BEGIN
                IF zorgtype != '' THEN
                    zorgtype := lpad(zorgtype, 2, '0')
                    agb := lpad(agb, 6, '0')
                    complete_agb := zorgtype || agb;
                ELIF len(agb) = 8 THEN
                    complete_agb := agb;
                ELSE
                    zorgtype := '99'
                    agb := lpad(agb, 6, '0')
                    complete_agb := zorgtype || agb;
                END IF
                RETURN complete_agb
            END;"""
            self.return_type = 'text'
            self.schema = 'sor_test_system'

    self.pipe.register_db_functions(test_db_functions, self.pipe.sor)

Dit is iets ingewikkelder. We maken een class aan die overerft van een DbFunction. In de init() definieren we:
  - de functie parameters
  - de sql body in pg pl sql
  - de return type
  - het schema waarin de functie moet worden aangemaakt.

De classes die overerven van DbFunction registreren we bij de pipe::

    self.pipe.register_db_functions(test_db_functions, self.pipe.sor)

In de mapping instantieren we deze class met de veldnamen die in de aanroep moeten komen::

    mapping.map_field(CreateAgb('zorgtype', 'zorgverlener_nummer'), Zorgverlener.Identificatie.agb)

Tijdens de ddl zal de functie worden aangemaakt::

    CREATE OR REPLACE FUNCTION sor_test_system.create_agb(zorgtype text, agb text)
    ....

tijdens de etl zal de functie worden aangeroepen::

    INSERT INTO zorgverlener_sat_identificatie (agb) SELECT sor_test_system.create_agb(zorgtype, zorgverlener_nummer) FROM ...hstage;

