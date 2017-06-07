import os
import sys
sys.path.insert(0, 'C:/!Ontwikkel/DWH2/PYELT' )

from pyelt.datalayers.dv import *
from pyelt.pipeline import Pipeline, Pipe

general_config = {
    'log_path': '/logs/',
    'ddl_log_path': '/logs/ddl/',
    'sql_log_path': '/logs/sql/',
    'conn_dwh': 'postgresql://postgres:pgsuadmin2016@127.0.0.1:5432/dwh2',
    'debug': True,
    'ask_confirm_on_db_changes': False,
    'on_errors': 'log',
    'datatransfer_path': 'c:/temp/pyeltdatatransfer',
    'data_root': 'C:/!OntwikkelDATA',
    'create_views': False,
    #'log_to_console': True,
    'email_settings': {
	'send_mail_before_run': True,
        'send_log_mail_after_run': True,
        'from': 'server <henk-jan.van.reenen@nlhealthcareclinics.com>',
        # 'to':'jan.van.lith@nlhealthcareclinics.com;',
        'to': 'henk-jan.van.reenen@nlhealthcareclinics.com;',
        'subject': 'PYELT DEV-TEST RUN ',
        'msg': 'Beste mensen,//n//nHierbij de pyelt log(s)//n//n//nGroet Claude S.//n'
    },
    'current_version': '123'
}


dummy_pipe_config = {
    'sor_schema': 'sor_dummy',
    'data_path': general_config['data_root'] + '/dummy/',
    'source_connection': 'postgresql://user:pass@127.0.0.1:5432/source',
    'default_schema': 'source_data',
    'active': True
}
if __name__ == '__main__':

    pipeline = Pipeline(general_config)
    db = pipeline.dwh
    sql = """\COPY {} (ifmw_id, _hash) FROM '{}/{}' DELIMITER ';' CSV HEADER ENCODING 'utf8';""".format('sor_timeff.medewerker_temp_hash', general_config['datatransfer_path'], 'NLHP/view_medewerker_hash.csv')
    # db.execute(sql)
    os.chdir('C:/Program Files/PostgreSQL/9.4/bin')
    psql = """psql -h {} -p {} -U {} -W {} {} """.format('127.0.0.1', '5432', 'postgres', 'pgsuadmin2016', 'dwh2', )
    psql = psql # + sql
    something = os.popen(psql).read()


