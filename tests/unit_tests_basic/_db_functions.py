from pyelt.datalayers.database import DbFunction, DbFunctionParameter


class CreateAgb(DbFunction):
    def __init__(self, zorgtype_calling_field='', agb_calling_field=''):
        super().__init__()
        param1 = DbFunctionParameter('zorgtype', 'text', zorgtype_calling_field)
        param2 = DbFunctionParameter('agb', 'text', agb_calling_field)
        self.func_params.append(param1)
        self.func_params.append(param2)
        self.sql_body = """DECLARE
        BEGIN
        RETURN zorgtype || '0' || agb;
        END;"""
        self.return_type = 'text'
        self.schema = 'sor_test_system'


class SplitHuisnummer(DbFunction):
    def __init__(self, huisnummer=''):
        super().__init__()
        param1 = DbFunctionParameter('huisnummer_input', 'text', huisnummer)
        self.func_params.append(param1)

        self.sql_body = """DECLARE
        pos_split INT := 0;
        huisnummer_deel TEXT := '';
        huisnummer INT;
        huisnummer_toevoeging TEXT;
        chars CHAR[];
        chr CHAR;
BEGIN
--TESTS:
--select sor_timeff.split_huisnummer('10a');
--select sor_timeff.split_huisnummer('10 a');
--select sor_timeff.split_huisnummer('10-a');
--select sor_timeff.split_huisnummer('10-1hoog');
--select sor_timeff.split_huisnummer('10');
--select sor_timeff.split_huisnummer('a');
--select sor_timeff.split_huisnummer('');
--select sor_timeff.split_huisnummer(NULL);
--select sor_timeff.split_huisnummer(' 10    a ' );
--select sor_timeff.split_huisnummer('  10 - a  ');
huisnummer_input := coalesce(huisnummer_input, '');
    huisnummer_input := trim(both ' ' from huisnummer_input);
    chars := regexp_split_to_array(huisnummer_input, '');

    FOREACH chr IN ARRAY chars
    LOOP
       IF chr >= '0' and chr <= '9' THEN
            huisnummer_deel := huisnummer_deel || chr;
           -- RAISE NOTICE 'test%', pos_split;
       ELSE
            exit;
       END IF;
       pos_split := pos_split + 1;
    END LOOP;
    -- RAISE NOTICE 'split at %', pos_split;

    huisnummer_toevoeging := substring(huisnummer_input from pos_split + 1);
    huisnummer_toevoeging := trim(both ' ' from huisnummer_toevoeging);
    huisnummer_toevoeging := trim(leading  '-' from huisnummer_toevoeging);
    huisnummer_toevoeging := trim(both ' ' from huisnummer_toevoeging);

    IF huisnummer_deel != '' THEN
	huisnummer := huisnummer_deel::integer;
    END IF;

    RETURN (huisnummer, huisnummer_toevoeging);
END;"""
        self.return_type = 'text'
        self.schema = 'sor_test_system'
