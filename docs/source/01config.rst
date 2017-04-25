Config
======

Om Pyelt te gebruiken is een config bestand nodig.

Maak een nieuw project aan bijvoorbeeld myDatavault en definieer hierin twee mappen::

 \domainmodel
 \mappings

In de root maak je een bestand aan configs.py::


    general_config = {
        'log_path': '/logs/',
        'ddl_log_path': '/logs/ddl/',
        'sql_log_path': '/logs/sql/',
        'conn_dwh': 'postgresql://user:pass@127.0.0.1:5432/dwh2',
        'debug': True,
        'ask_confirm_on_db_changes': False,
        'on_errors': 'log',
        'datatransfer_path': '/tmp',
        'data_root': '/var/data',
        'create_views': False,
        'email_settings': {
        'send_mail_before_run': True,
            'send_log_mail_after_run': True,
            'from': 'server <henk-jan.van.reenen@nlhealthcareclinics.com>',
            'to': 'henk-jan.van.reenen@nlhealthcareclinics.com;',
            'subject': 'PYELT DEV-TEST RUN ',
            'msg': 'Beste mensen,//n//nHierbij de pyelt log(s)//n//n//nGroet Claude S.//n'
        }
    }

