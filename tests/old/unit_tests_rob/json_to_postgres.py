import psycopg2
import json
from sqlalchemy import create_engine, text
from domainmodel_fhir.test_config import test_configs, test_config


"test om jsonfile naar een postgrestabel te krijgen"

# path_to_file = input('Enter path to json file:')
path_to_file = 'C:/!OntwikkelDATA/jsontest/test.json'
with open(path_to_file, 'r') as data_file:
    data = json.load(data_file)

collection_array = []
# collection_array.append(json.dumps(data)) # hele file in 1 veld
for item in data:
    collection_array.append(json.dumps(item))



try:
    engine = create_engine(test_configs['conn_dwh'])
    conn = engine.raw_connection()

    print ("opened  database successfully")
    cur = conn.cursor()
    cur.execute("""CREATE TABLE sor_test.json_hstage
                    (
                      jsonobject json
                    )
                    WITH (
                      OIDS=FALSE,
                      autovacuum_enabled=true
                    );
                    ALTER TABLE sor_test.json_hstage
                      OWNER TO postgres; """)

    for element in collection_array:
        cur.execute("INSERT INTO sor_test.json_hstage (jsonobject) VALUES (%s)", (element,))
    print("successfully inserted records")  # data wordt naar bovenstaande tabel geschreven naar jsonobject


except psycopg2.Error as e:
    raise

finally:
    conn.commit()
    conn.close()
    print("connection is closed")