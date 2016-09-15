from pyelt.helpers.encrypt import SimpleEncrypt

config = {
    'log_path': '/logs/',
    'ddl_log_path': '/logs/ddl/',
    'sql_log_path': '/logs/sql/',
    'conn_dwh': 'postgresql://postgres:' + SimpleEncrypt.decode('pwd', 'wrTCrMOGw4DCtcKzwr3ClsKjwoF4e2k=')+ '@localhost:5432/dwh',
    'debug': True,  # Zet debug op true om een gelimiteerd aantal rijen op te halen
    'datatransfer_path': '/tmp/pyelt/datatransfer',  # voor linux server: datatransfer pad mag niet in /home folder zijn, want anders kan postgres er niet bij
    'on_errors': 'log',
# vul 'throw' in als je wilt dat het programma stopt zodra er een error optreedt, zodat je die kunt afvangen. Kies log voor als je wilt dat de error gelogt wordt en doorgaat met de volgende stap
    'datatransfer_path': 'c:/tmp/',
    'data_root': '/',
    'ask_confirm_on_db_changes': False
}


source_config = {
    'sor_schema': 'sor_sample',
    'data_path': config['data_root'] + '/sample_data/',
    'active': True,
    'connection_string': '',
    'other_configs': None
}