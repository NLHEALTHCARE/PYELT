from typing import Dict, List, Union, Any

import time
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import reflection
from sqlalchemy.sql.sqltypes import NullType
from pyelt.helpers.global_helper_functions import camelcase_to_underscores



class Database():
    def __init__(self, conn_string: str = '', default_schema: str = 'public') -> None:
        self.engine = create_engine(conn_string)
        conn_string_parts = conn_string.split('/')
        self.name = conn_string_parts[-1]  # type: str
        self.default_schema = Schema(default_schema, self)
        self.driver = 'postgres' #type: str
        self.reflected_schemas = {} #type: Dict[str, Schema]

    def reflect_schemas(self):
        inspector = reflection.Inspector.from_engine(self.engine)
        schema_names = inspector.get_schema_names()
        for schema_name in schema_names:
            self.reflected_schemas[schema_name] = Schema(schema_name, self)

    def execute(self, sql: str, log_message: str=''):
        self.log('-- ' + log_message.upper())
        self.log( sql)

        start = time.time()
        connection = self.engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()

        rowcount = cursor.rowcount
        self.log('-- duur: ' + str(time.time() - start) +  '; aantal rijen:' + str(cursor.rowcount))
        self.log('-- =============================================================')
        cursor.close()
        return rowcount

    def execute_without_commit(self, sql: str, log_message: str=''):
        self.log('-- ' + log_message.upper())
        self.log( sql)

        start = time.time()
        if not self.__cursor:
            self.start_transaction()
        self.__cursor.execute(sql)

        self.log('-- duur: ' + str(time.time() - start) +  '; aantal rijen:' + str(self.__cursor.rowcount))
        self.log('-- =============================================================')

    def start_transaction(self):
        connection = self.engine.raw_connection()
        self.__conn = connection
        self.__cursor = connection.cursor()

    def commit(self, log_message: str = ''):
        # connection = self.engine.raw_connection()
        self.__conn.commit()
        self.__conn.close()

    def execute_returning(self, sql: str, log_message: str = ''):
        self.log('-- ' + log_message.upper())
        self.log(sql)

        start = time.time()
        connection = self.engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()

        result = cursor.fetchall()
        self.log('-- duur: ' + str(time.time() - start) + '; aantal rijen:' + str(cursor.rowcount))
        self.log('-- =============================================================')
        cursor.close()
        return result

    def execute_read(self, sql, log_message='') -> List[List[Any]]:
        self.log('-- ' + log_message.upper())
        self.log(sql)

        start = time.time()
        # plan = text(sql)
        # result = engine.execute(sql)

        connection = self.engine.raw_connection()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql)
        result = cursor.fetchall()

        self.log('-- duur: ' + str(time.time() - start) + '; aantal rijen:' + str(cursor.rowcount))
        self.log('-- =============================================================')
        cursor.close()
        return result

    def confirm_execute(self, sql: str, log_message: str='') -> None:
        ask_confirm = False
        if 'ask_confirm_on_db_changes' in self.config:
            ask_confirm = self.config['ask_confirm_on_db_changes']
        else:
            ask_confirm = 'debug' in self.config and self.config['debug']
        if ask_confirm:
            sql_sliced = sql
            if len(sql) > 50:
                sql_sliced = sql[:50] + '...'
            else:
                sql_sliced = sql
            result = input('{}\r\nWil je de volgende wijzigingen aanbrengen in de database?\r\n{}\r\n'.format(log_message.upper(), sql_sliced))
            if result.strip().lower()[:1] == 'j' or result.strip().lower()[:1] ==  'y':
                self.execute(sql, log_message)
                # print(log_message, 'uitgevoerd')
            else:
                raise Exception('afgebroken')
        else:
            self.execute(sql, log_message)




class Schema():
    def __init__(self, name: str, db: 'Database', schema_type: str = '') -> None:
        self.name = name  #type: str
        self.db = db  #type: Database
        self.schema_type = schema_type
        self.tables = {} #type: Dict[str, Table]
        self.views = {} #type: Dict[str, View]
        self.functions = {}  # type: Dict[str, DbFunction]
        self.is_reflected = False
        self.version = 1.0

    def reflect(self):
        inspector = reflection.Inspector.from_engine(self.db.engine)
        table_names = inspector.get_table_names(self.name)
        view_names = inspector.get_view_names(self.name)
        for view_name in view_names:
            cols = inspector.get_columns(view_name, self.name)
            self.views[view_name] = inspector.get_view_definition(view_name, self.name)

        self.functions = self.reflect_functions()

        # for table_name in table_names:
        #     tbl = Table(table_name, self, self.db)
        #     cols = inspector.get_columns(table_name, self.name)
        #     for col in cols:
        #         col = Column(col['name'], str(col['type']), tbl)
        #         # col.is_key = sa_col.primary_key
        #         tbl.columns.append(col)
        #     indexes = inspector.get_indexes(table_name, self.name)
        tmp_meta = MetaData()
        tmp_meta.reflect(bind=self.db.engine, schema=self.name)
        for sa_tbl in tmp_meta.sorted_tables:
            tbl = Table(sa_tbl.name, self)
            for sa_col in sa_tbl.columns:
                if isinstance(sa_col.type, NullType):
                    col_type = 'custom'
                else:
                    col_type = str(sa_col.type)
                col = Column(sa_col.name, col_type, tbl)
                col.is_key = sa_col.primary_key
                tbl.columns.append(col)
            self.tables[tbl.name] = tbl
            for sa_idx in sa_tbl.indexes:
                # self.indexes[sa_idx.name] = sa_idx.name
                tbl.indexes[sa_idx.name] = sa_idx.name
            for sa_constraint in sa_tbl.constraints:
                # constraint_type = sa_constraint.__class__.__name__
                # name = sa_tbl.name + '_' + constraint_type
                # self.constraints[sa_constraint.name] = sa_constraint.name
                tbl.constraints[sa_constraint.name] = sa_constraint.name
            for sa_constraint in sa_tbl.foreign_key_constraints:
                # constraint_type = sa_constraint.__class__.__name__
                # name = sa_tbl.name + '_' + constraint_type
                # self.constraints[sa_constraint.name] = sa_constraint.name
                tbl.constraints[sa_constraint.name] = sa_constraint.name
        self.is_reflected = True

    def reflect_functions(self) -> Dict[str, 'DbFunction']:
        self.functions = {}
        cursor = self.db.engine.raw_connection().cursor()
        sql = """SELECT routine_name as name, data_type, type_udt_name as data_type2, external_language as lang, routine_definition as body FROM information_schema.routines WHERE SPECIFIC_SCHEMA='{}' and routine_type = 'FUNCTION'""".format(self.name)
        sql = """SELECT n.nspname AS schema_name
      ,p.proname AS function_name
      ,pg_get_function_arguments(p.oid) AS args
      ,pg_get_functiondef(p.oid) AS func_def
      ,pg_get_function_result(p.oid) AS return_type
FROM   (SELECT oid, * FROM pg_proc p WHERE NOT p.proisagg) p
JOIN   pg_namespace n ON n.oid = p.pronamespace
WHERE  n.nspname = '{}'""".format(self.name)
        for row in self.db.engine.execute(sql):
            # db_func = DbFunction(row['name'])
            # db_func.lang = row['lang']
            # db_func.return_type = row['data_type2']
            # db_func.sql_body = row['body']
            # db_func.schema = self

            db_func = DbFunction()
            db_func.name = row['function_name']
            db_func.parse_args(row['args'])
            db_func.return_type = row['return_type']
            db_func.parse_sql_body(row['func_def'])
            db_func.schema = self
            complete_name = db_func.get_complete_name()
            self.functions[complete_name] = db_func
        return self.functions

    def __contains__(self, item: str) -> bool:
        if isinstance(item, str):
            for name in self.tables:
                if item == name:
                    return True
            for name in self.views:
                if item == name:
                    return True
            for name in self.functions:
                if item == name:
                    return True
        return False

class Table():
    def __init__(self, name: str, schema: 'Schema' = None, db: 'Database' = None) -> None:
        self.name = name #type: str
        self.schema = schema #type: Schema
        # sommige bron databases hebben geen schema, maar alleen db
        self.db = db
        if schema:
            self.db = schema.db  # type: Database
        self.key_names = [] #type: List[str]
        self.columns = [] #type: List[Column]
        self.indexes = {} #type: Dict[str, str]
        self.constraints = {} #type: Dict[str, str]
        self.is_reflected = False #type: bool
        self.type = '' #type: str

    def __str__(self) -> str:
        return self.name

    def set_primary_key(self, key_names:List[str]=[]) -> None:
        key_names = [key.lower() for key in key_names]
        if not self.is_reflected:
            self.reflect()
        self.key_names = key_names
        for col in self.columns:
            col.is_key = (col.name.lower() in key_names)

    def primary_keys(self) -> List[str]:
        return self.key_names #[col.name for col in self.columns if col.is_key]

    def field_names(self) -> List[str]:
        return [col.name for col in self.columns]




    def reflect(self) -> None:
        self.columns = [] #type: List[Column]
        inspector = reflection.Inspector.from_engine(self.db.engine)
        columns = inspector.get_columns(self.name, self.schema.name)
        pks = inspector.get_primary_keys(self.name, self.schema.name)
        indexes = inspector.get_indexes(self.name, self.schema.name)
        self.key_names = []
        for col in columns:
            #col = Column(col['name'], str(col['type']), self)
            col = Column(col['name'], col['type'], self)
            col.is_key = (col.name in pks)
            if col.is_key:
                self.key_names.append(col.name)
            # if self.key_names:
            #     col.is_key = (col.name in self.key_names)
            self.columns.append(col)
        for index in indexes:
            self.indexes[index['name']] = index
        self.is_reflected = True

        # sql = """SELECT * FROM {}.{} WHERE 1=0 """.format(self.schema.name, self.name)
        # result = self.db.engine.execute(sql)
        # oracle = True
        # for sa_col in result.cursor.description:
        #     if oracle:
        #         type = sa_col[1].__name__
        #         col = Column(sa_col[0], type, self)
        #     else:
        #         col = Column(sa_col.name, str(sa_col.type_code), self)
        #         col.is_key = sa_col.primary_key
        #     self.columns.append(col)
        # self.db.__dict__[self.name] = self

    def __contains__(self, item: Union[str, 'Column']) -> bool:
        item_name = str(item)
        if isinstance(item, Column):
            item_name = item.name
        for col in self.columns:
            if item_name.lower() == col.name.lower():
                return True
        for name in self.indexes:
            #name van indexes in db maar max 63 posities lang
            if item_name[:63] == name:
                return True
        for name in self.constraints:
            if item_name == name:
                return True
        return False

        # meta = MetaData()
        # meta.bind = self.db.engine
        # complete_name = '{}.{}'.format(self.schema.name, self.name)
        # sa_tbl = Table(complete_name, meta, autoload=True)
        # tbl = SourceTable(self.db, self, sa_tbl.name)
        # for sa_col in sa_tbl.columns:
        #     col = SourceColumn(tbl, sa_col.name, str(sa_col.type))
        #     col.is_key = sa_col.primary_key
        #     tbl.columns.append(col)
        # self.db.__dict__[tbl.name] = tbl

    # def to_csv(self, path = ''):
    #     if not path:
    #         from pyelt.__main__ import get_path
    #         path = get_path() + '/data/transfer/' + self.schema.name
    #     else:
    #         path = path
    #     if not os.path.exists(path):
    #         os.makedirs(path)
    #     file_name = '{}/{}.csv'.format(path, self.name)
    #
    #     head = [col.name for col in self.columns]
    #     with open(file_name, 'w', newline='', encoding='utf8') as fp:
    #         csv_writer = csv.writer(fp, delimiter=';')
    #         csv_writer.writerow(head)
    #         data = self.load()
    #         csv_writer.writerows(data)
    #     return file_name
    #
    # def load(self):
    #     rows = []
    #     sql = """SELECT * FROM {}.{}""".format(self.schema.name, self.name)
    #     sql += " OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY"
    #     result = self.db.engine.execute(sql)
    #     for row in result:
    #         rows.append(row)
    #     return rows
    #
    # def create_python_code_mappings(self, remove_prefix =''):
    #     """helper functie om mappings aan te maken in string formaat"""
    #     if len(self.columns) == 0: self.reflect()
    #     s = """mapping = EntityMapping('{0}_hstage', '{0}_entity')\r\n""".format(self.name)
    #     for col in self.columns:
    #         source = col.name.lower()
    #         target = col.name.lower().replace(remove_prefix, '')
    #         s += """mapping.map_field('{0:<30} => {1} {2}')\r\n""".format(source, target, col.type)
    #     print(s)


# class Query(Table):
#     def __init__(self, db, sql):
#         self.db = db
#         self.sql = sql
#         self.name = 'query'
#         self.columns = []
#
#     def reflect(self):
#         result = self.db.engine.execute(self.sql + ' WHERE 1 = 0')
#         for sa_col in result.cursor.description:
#             col = Column(self, sa_col.name, str(sa_col.type_code))
#             self.columns.append(col)
#
#
#     def to_csv(self, path=''):
#         if not path:
#             from pyelt.__main__ import get_path
#             path = get_path() + '/data/transfer/' + self.db.name
#         return super().to_csv(path)
#
#     def load(self):
#         rows = []
#         sql = self.sql
#         result = self.db.engine.execute(sql)
#         for row in result:
#             rows.append(row)
#         return rows

class View(Table):
    def __init__(self, name: str, schema: 'Schema', db: 'Database') -> None:
        super().__init__(name, schema, db)
        self.sql = ''


class Column():
    def __init__(self, name: str, type: str = 'text', tbl: 'Table' = None, default_value = '', fhir_name = '') -> None:
        name = name.strip()
        # while '  ' in name:
        #     name = name.replace('  ', ' ')
        # if ' ' in name:
        #     name, type, *rest_args = name.split(' ')
        self.name = name.strip().lower()
        if not isinstance(type, str):
            if type == 23:
                type = 'integer'
            else:
                type = 'text'
        self.type = type.strip()
        #todo length
        self.length = ''
        self.table = tbl
        self.is_key = False
        self.default_value = default_value
        self.fhir_name = fhir_name

    def __str__(self) -> str:
        return self.name.lower()

    def __repr__(self) -> str:
        return "{} ({})".format(self.name.lower(), self.type.lower())
        # return self.default_value

    @staticmethod
    def clear_name(name: str) -> str:
        name = name.replace('(', '').replace(')', '')
        name = name.replace('?', '')
        name = name.replace('-', '')
        name = name.replace(',', '')
        name = name.replace('.', '')
        name = name.replace(':', '')
        name = name.replace(' ', '_')
        name = name.lower()
        return name

    def get_table(self):
        return self.table

    def __eq__(self, other):
        # zet om in sql
        return Condition(self.name, '=', other, table=self.table)

    def __ne__(self, other):
        # zet om in sql
        return Condition(self.name, '!=', other, table=self.table)
        # return Condition('({} != {})'.format(self.name,other))

    def __gt__(self, other):
        # zet om in sql
        return Condition(self.name, '>', other, table=self.table)
        # return Condition('({} > {})'.format(self.name,other))

    def __ge__(self, other):
        # zet om in sql
        return Condition(self.name, '>=', other, table=self.table)
        # return Condition('({} >= {})'.format(self.name,other))

    def __lt__(self, other):
        # zet om in sql
        return Condition(self.name, '<', other, table=self.table)
        # return Condition('({} < {})'.format(self.name,other))

    def __le__(self, other):
        # zet om in sql
        return Condition(self.name, '<=', other, table=self.table)
        # return Condition('({} <= {})'.format(self.name,other))

    def is_in(self, item):

        return Condition(self.name, 'in', item, table=self.table)

    def between(self, item1, item2):
        item = '{} AND {}'.format(item1, item2)
        return Condition(self.name, 'between', item, table=self.table)




class Columns():
    class TextColumn(Column):
        def __init__(self, name:str = '', default_value='', fhir_name=''):
            super().__init__(name, 'text', default_value=default_value, fhir_name=fhir_name)

    class TextArrayColumn(Column):
        #voorlopig deze gewoon op type text laten staan. Misschien later aanpassen naar text[]
        def __init__(self, name:str = '', default_value=None, fhir_name=''):
            super().__init__(name, 'text', default_value=default_value, fhir_name=fhir_name)

    class RefColumn(Column):
        def __init__(self,  valueset_name:str,name:str = '', default_value='', fhir_name=''):
            super().__init__(name, 'text', default_value=default_value, fhir_name=fhir_name)
            self.valueset_name = valueset_name

    class DateTimeColumn(Column):
        def __init__(self, name:str = '', default_value='', fhir_name=''):
            super().__init__(name, 'timestamp', default_value=default_value, fhir_name=fhir_name)

    class DateColumn(Column):
        def __init__(self, name:str = '', default_value='', fhir_name=''):
            super().__init__(name, 'date', default_value=default_value, fhir_name=fhir_name)

    class IntColumn(Column):
        def __init__(self, name:str = '', default_value='', fhir_name=''):
            super().__init__(name, 'integer', default_value=default_value, fhir_name=fhir_name)

    class FloatColumn(Column):
        def __init__(self, name:str = '', default_value='', fhir_name=''):
            super().__init__(name, 'numeric', default_value=default_value, fhir_name=fhir_name)

    class BoolColumn(Column):
        def __init__(self, name:str = '' , default_value='', fhir_name=''):
            super().__init__(name, 'bool', default_value=default_value, fhir_name=fhir_name)

    class JsonColumn(Column):
        def __init__(self, name:str = '' , default_value={}, fhir_name=''):
            super().__init__(name, 'jsonb', default_value=default_value, fhir_name=fhir_name)

    class FHIR():
        # voorlopig onderstaande  gewoon op type text laten staan. Misschien later aanpassen naar fhire types
        class PeriodColumn(Column):
            def __init__(self, name: str = '', default_value=None):
                super().__init__(name, 'text', default_value=default_value)
                # super().__init__(name, 'fhir.period', default_value=default_value)

        class CodingColumn(Column):
            def __init__(self, name: str = '', default_value=None):
                super().__init__(name, 'text', default_value=default_value)
                # super().__init__(name, 'fhir.coding', default_value=default_value)

        class CodeableConceptColumn(Column):
            def __init__(self, name: str = '', default_value=None):
                super().__init__(name, 'text', default_value=default_value)
                # super().__init__(name, 'fhir.codeable_concept', default_value=default_value)



####################################
class Condition():
    def __init__(self, field_name='', operator='', value=None, table=None):
        self.table = table
        if operator:
            if isinstance(value, str):
                sql_condition = "{} {} '{}'".format(field_name, operator, value)
            elif isinstance(value, list):
                value = str(value)
                value = value.replace('[', '(').replace(']', ')')
                sql_condition = "{} {} {}".format(field_name, operator, value)
            else:
                sql_condition = "{} {} {}".format(field_name, operator, value)
        else:
            sql_condition = field_name
        sql_condition = sql_condition.replace(' = None', ' IS NULL')
        sql_condition = sql_condition.replace(' != None', ' IS NOT NULL')
        self.name = sql_condition

    def __and__(self, other):
        # zet om in sql
        if isinstance(other, Condition):
            return Condition('({} AND {})'.format(self.name, other.name), table=self.table)
        else:
            return Condition('({} AND {})'.format(self.name, other), table=self.table)

    def __or__(self, other):
        # zet om in sql
        if isinstance(other, Condition):
            return Condition('({} OR {})'.format(self.name, other.name), table=self.table)
        else:
            return Condition('({} OR {})'.format(self.name, other), table=self.table)


################
# class DatabaseFunction():
#     def __init__(self, name: str) -> None:
#         self.name = name #type: str
#         self.lang = '' #type: str
#         self.return_type = None #type: bool
#         self.body = '' #type: str
#         self.parameters = [] #type: List[str]


class DbFunctionParameter:
    def __init__(self, name='', type='', calling_field=''):
        self.name = name
        self.type = type
        self.calling_field = calling_field


class DbFunction:
    def __init__(self, *args):
        self.name = camelcase_to_underscores(self.__class__.__name__)
        self.func_params = []
        # self.func_declares = []
        for arg in args:
            self.func_params.append(arg)
        self.sql_body = ''
        self.return_type = ''
        self.schema = None
        self.lang = 'PLPGSQL'

    def get_complete_name(self):
        func_args = ''
        for func_param in self.func_params:
            func_args += '{} {}, '.format(func_param.name, func_param.type)
        func_args = func_args[:-2]
        params = {'schema': self.schema.name, 'name': self.name, 'args': func_args}
        return '{schema}.{name}({args})'.format(**params)

    def to_create_sql(self):
        func_params = ''
        for func_param in self.func_params:
            func_params += '{} {}, '.format(func_param.name, func_param.type)
        # sql_declares = ''
        # for func_declare in self.func_declares:
        #     if not func_declare.endswith(';'):
        #         sql_declares += '{};\r\n '.format(func_declare)
        #     else:
        #         sql_declares += '{}\r\n '.format(func_declare)
        func_params = func_params[:-2]
        params = {'schema': self.schema.name, 'name': self.name, 'params': func_params, 'return_type': self.return_type, 'sql_body': self.sql_body, 'lang': self.lang}

        sql = """CREATE OR REPLACE FUNCTION {schema}.{name}({params})
  RETURNS {return_type}
    AS
    $BODY$
        {sql_body}
    $BODY$
  LANGUAGE {lang} VOLATILE;""".format(**params)
        return sql

    def get_sql(self, alias=''):
        params = self.__dict__
        fields = ''
        for param in self.func_params:
            if alias:
                fields += '{}.{},'.format(alias, param.calling_field)
            else:
                fields += '{},'.format(param.calling_field)
        fields = fields[:-1]
        params['fields'] = fields
        sql = "{schema}.{name}({fields})".format(**params)
        return sql

    def parse_args(self, args_str):
        args = args_str.split(',')
        self.func_params = []
        for arg in args:
            arg = arg.strip()
            arg_name = arg.split(' ')[0]
            arg_type = arg.split(' ')[1]
            self.func_params.append(DbFunctionParameter(arg_name, arg_type))

    def parse_sql_body(self, sql_body=''):
        start = sql_body.find('$function$') + len('$function$')
        end = sql_body.rfind('$function$')
        body = sql_body[start:end]
        self.sql_body = body

    def get_stripped_body(self):
        body = self.sql_body
        body = body.replace('  ', '')
        body = body.replace('  ', '')
        body = body.replace('\t', '')
        body = body.replace('\r\n', '')
        body = body.replace('\n', '')
        if not body.startswith('DECLARE'):
            body = 'DECLARE' + body
        return body
