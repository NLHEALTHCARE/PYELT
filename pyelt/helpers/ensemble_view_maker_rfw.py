# from sqlalchemy.engine import reflection
from tests.unit_tests_rob import _domain_rob
# from tests.unit_tests_rob._domain_rob import *
from sample_domains import _ensemble_views
from pyelt.process.ddl import DdlDv
from pyelt.pipeline import Pipeline
from tests.unit_tests_rob.global_test_suite import test_system_config, get_global_test_pipeline, execute_sql, init_db
from tests.unit_tests_rob._mappings_rob import init_source_to_sor_mappings, init_sor_to_dv_mappings

#deze en onderstaande opmerkingen gaan verwijderd worden, zodra testen van code is afgelopen
# class Ensemble_view() is verplaatst naar pyelt.datalayers.dv.py
# aanmaken van ensemble onderdelen is verplaatst naar _ensemble_views.py


# maak een pipeline aan
init_db()
pipeline = get_global_test_pipeline()
pipe = pipeline.get_or_create_pipe('test_system', config=test_system_config)
pipe.register_domain(_domain_rob)
pipe.register_domain(_ensemble_views)
pipe.mappings = []
pipe.mappings.extend(init_source_to_sor_mappings())
pipe.mappings.extend(init_sor_to_dv_mappings(pipe))
pipeline.run()

# pipeline = Pipeline()
# pyelt_unittests = pipeline.dwh
# dv = pyelt_unittests.dv
# dv.reflect()

# DdlDv.create_or_alter_ensemble_view(pyelt_unittests)

# inspector = reflection.Inspector.from_engine(dv.db.engine)
# table_names = inspector.get_table_names(dv.name)
# # schema_names = inspector.get_schema_names()
# # print(schema_names)
#
#
#
# sql_ensemblename ='dv.ensembleview'
# sql_columns = ''
# sql_selectedtables = ''
# sql_conditions = ''
# params = {'schema_name': 'dv'} #todo functie voor "get_schema_name"?
#
# for alias, cls in ensemble.entity_dict.items():
#     print(alias, cls.get_name())
#     cls.init_cls()
#     cls.entity_name = cls.get_name().strip('_entity')
#     cls.view_name = cls.get_name().strip('_entity') + '_view'
#
#     if cls.__base__ == Link:
#         sql_selectedtables += ', {schema_name}.'.format(**params) + cls.get_name()
#     else:
#         if alias == str(cls.get_name()): # geen alias opgegeven dus alias is hetzelfde als de entity_name
#             sql_selectedtables += ', {schema_name}.{} as {}'.format(cls.view_name, cls.entity_name,**params)
#             sql_conditions += ' AND {0}._id = fk_{0}_hub'.format(cls.entity_name)
#             sql_ensemblename = sql_ensemblename.replace('v.', 'v.{}_'.format(cls.entity_name))
#
#             cols = inspector.get_columns(cls.view_name, dv.name)
#             for col in cols:
#                 col_name = col['name']
#                 sql_columns += ', {0}.{1} as {0}_{1}'.format(cls.entity_name,col_name)
#
#         else: # alias voor entity_name opgegeven
#             sql_selectedtables += ', {schema_name}.{} as {}'.format(cls.view_name,alias,**params)
#             sql_conditions += ' AND {0}._id = fk_{0}_{1}_hub'.format(alias,cls.entity_name)
#             sql_ensemblename = sql_ensemblename.replace('v.', 'v.{}_'.format(alias))
#
#             cols = inspector.get_columns(cls.view_name, dv.name)
#             for col in cols:
#                 col_name = col['name']
#                 sql_columns += ', {0}.{1} as {0}_{1}'.format(alias,col_name)
#
# sql_columns = sql_columns[2:] # verwijder de "' " van het begin van de string
# sql_selectedtables = sql_selectedtables[2:]
# sql_conditions = sql_conditions[4:]
#
# if ensemble.name: # als ensemble.name niet een lege string is dan wordt de aangemaakte string sql_ensemblename gebaseerd op de gebruikte entiteiten vervangen door de vooraf opgegeven alternatieve naam (bv 'test_view')
#      sql_ensemblename = '{schema_name}.'.format(**params) + ensemble.name
#
# params.update ({'ensemble': sql_ensemblename, 'columns': sql_columns, 'selected tables': sql_selectedtables, 'conditions': sql_conditions})
# sql_end_result = """CREATE OR REPLACE VIEW {ensemble} AS SELECT {columns} FROM {selected tables} WHERE {conditions}; ALTER TABLE {ensemble} OWNER TO postgres;""".format(**params)

# print('sql_columns: {}'.format(sql_columns))
# print('sql_selectedtables: {}'.format(sql_selectedtables))
# print('sql_conditions: {}'.format(sql_conditions))
# print('sql_ensemblename: {}'.format(sql_ensemblename))
# print('sql_end_result: {}'.format(sql_end_result))

