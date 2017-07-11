# conn_string_test_db = 'postgresql://postgres:pgsuadmin2016@localhost:5432/pyelt_unittests'

general_config = {
    'log_path': '/logs/perform_tests',
    'conn_dwh': 'postgresql://postgres:pgsuadmin2016@localhost:5432/pyelt_unittests',
    'debug': False,
    'on_errors': 'throwx',
    # 'log_to_console': False
    'create_views': False

}

test_system_config = {
    'source_connection': 'postgresql://postgres:pgsuadmin2016@localhost:5432/test_data',
    'default_schema': 'public',
    'sor_schema': 'sor_extra'
}
