import inspect
from collections import OrderedDict

from pyelt.datalayers.database import *
from pyelt.datalayers.dv import *
from pyelt.datalayers.dv_metaclasses import *
from pyelt.datalayers.valset import DvValueset


class Join():
    def __init__(self, sql_snippet):
        self.sql_snippet = sql_snippet
        self.own_table_name = sql_snippet.split('=')[0].strip().split('.')[0].strip()
        self.own_field_name = sql_snippet.split('=')[0].strip().split('.')[1].strip()
        self.reference_table_name = sql_snippet.split('=')[1].strip().split('.')[0].strip()
        self.reference_field_name = sql_snippet.split('=')[1].strip().split('.')[1].strip()
        self.own_table = None
        self.reference_table = None

    def __str__(self):
        return self.sql_snippet

    def __repr__(self):
        return self.sql_snippet





class QueryMaker():
    def __init__(self, name):
        self.name = name
        self.__fields = OrderedDict()
        self.__joins = []
        self.__filter = ''
        self.start_table = None

    def select(self, val, col_alias='', type=''):
        if isinstance(val, str):
            if not col_alias:
                raise Exception('Bij gebruik van sql in select moet je een alias opgeven')
            self.__fields[col_alias] = val + ' AS ' + col_alias
            return
        elif not isinstance(val, Column):
            raise Exception(' gebruik column voor select')
        column = val
        table = column.table

        stack = inspect.stack()
        caller = stack[1].code_context[0]
        caller = caller[caller.find('(') + 1: caller.find(')')]
        if ',' in caller:
            caller = caller.split(',')[0]
        if '_hub' in table.__dbname__:
            tbl_alias = caller.split('.')[0].lower() + '_hub'
        elif '_sat' in table.__dbname__:
            hub_alias = caller.split('.')[0].lower() + '_hub'
            tbl_alias = caller.split('.')[0].lower() + '_sat_' + caller.split('.')[1].lower() + type
            tbl_alias = tbl_alias.replace('_default', '')
        #elif DvValueset in table.__class__.__mro__:
        else:
            #valset table
            #in table.__class__.__mro__:
            tbl_alias = caller.split('.')[0].lower()

        if not col_alias:
            col_alias = caller.lower().replace('.', '_')
            col_alias = col_alias.replace('default_', '')
        if col_alias in self.__fields:
            raise Exception(col_alias + ' bestaat al in query. Gebruik een andere alias.')
        else:
            params = {}
            params['tbl_alias'] = tbl_alias
            params['col_name'] = column.name
            params['column_alias'] = col_alias
            sql = '{tbl_alias}.{col_name} as {column_alias}'.format(**params)
            self.__fields[col_alias] = sql

            if '_sat' in table.__dbname__:
                join_sql = '{0}._id = {1}._id AND {0}._active AND {0}._valid'.format(tbl_alias, hub_alias)
                join_sql = '{0}._id = {1}._id AND {0}._active'.format(tbl_alias, hub_alias)

                if type:
                    join_sql += " AND {}.type = '{}'".format(tbl_alias, type)
                join = Join(join_sql)
                join.own_table = table
                join.reference_table = table.hub.ref_table
                self.__joins.append(join)



    def join(self, left, right, left_table = None, right_table= None):


        stack = inspect.stack()
        caller = stack[1].code_context[0]
        caller = caller[caller.find('(') + 1: caller.find(')')]
        if ',' in caller:
            arg1 = caller.split(',')[0]
            arg2 = caller.split(',')[1].strip()


        left_alias = arg1.split('.')[0].lower()
        if type(left) is HubEntityMetaClass:
            left_table= left.Hub
            left_alias += '_hub'
            left_field = '_id'
        elif type(left) is LinkReference:
            left_table = left.link
            left_alias = left.link.__dbname__
            left_field = left.fk
        elif type(left) is Column or type(left) is Columns.RefColumn:
            left_table = left.table
            left_alias = left_alias
            left_field = left.name
            if '_sat' in left_table.__dbname__:
                left_alias = arg1.split('.')[0].lower() + '_sat_' + arg1.split('.')[1].lower()
                left_alias = left_alias.replace('_default', '')
        elif isinstance(left, str):
            left_alias = left
            left_table = left_table
            left_field = ''

        right_alias = arg2.lower().strip()
        if type(right) is HubEntityMetaClass:
            right_table = right.Hub
            right_alias += '_hub'
            right_field = '_id'
        elif type(right) is LinkReference:
            right_table = right.link
            right_alias = right.link.__dbname__
            right_field = right.fk
        elif type(right) is Column or type(right) is Columns.RefColumn:

        # elif Column in right.__class__.__mro__:
            right_table = right.table
            right_alias = right_alias
            right_field = right.name
            if '_sat' in right_table.__dbname__:
                right_alias = arg2.split('.')[0].lower() + '_sat_' + arg2.split('.')[1].lower()
                right_alias = right_alias.replace('_default', '')

        sql = '{}.{} = {}.{}'.format(left_alias, left_field, right_alias, right_field)
        if not left_field:
            sql = '{} = {}.{}'.format(left_alias, right_alias, right_field)
        join = Join(sql)
        join.own_table = left_table
        join.reference_table = right_table
        self.__joins.append(join)

    def where(self, filter):
        self.__filter = filter

    def order_by(self, column):
        pass

    def to_sql(self):
        select = ''
        if not self.__fields.items():
            select = '*'
        else:
            for field, sql in self.__fields.items():
                select += '  ' + sql + ',\n'
        select = select[:-2]

        frm = ''
        tables_in_from = {}
        if self.start_table:
            alias = self.start_table.__dbname__
            #onderstaande is te voorkomen als andere aliassen gaan met CamelCaseToUnderscore
            alias = alias.replace('_hub', '').replace('_', '') + '_hub'
            table_name = alias
            frm = ' {}.{} AS {}\n'.format(self.start_table.__dbschema__, self.start_table.__dbname__, alias)
        else:
            for join in self.__joins:
                #we beginnen met een hub
                if '_hub' in join.own_table.__dbname__ and not frm:
                    frm += ' {}.{} AS {}\n'.format(join.own_table.__dbschema__, join.own_table.__dbname__, join.own_table_name)
                    table_name = join.own_table_name
                    break
                if '_hub' in join.reference_table.__dbname__ and not frm:
                    frm += ' {}.{} AS {}\n'.format(join.reference_table.__dbschema__, join.reference_table.__dbname__, join.reference_table_name)
                    table_name = join.reference_table_name
                    break

        tables_in_from[table_name] = table_name
        join, table_name, table = self.__find_in_joins(table_name, tables_in_from)
        while join != None:
            frm += ' LEFT JOIN {}.{} AS {} ON {}\n'.format(table.__dbschema__, table.__dbname__, table_name, join.sql_snippet)
            tables_in_from[table_name] = table_name
            join, table_name, table = self.__find_in_joins(table_name, tables_in_from)
            if not join:
                for table_name in tables_in_from.keys():
                    join, table_name, table = self.__find_in_joins(table_name, tables_in_from)
                    if join:
                        break
        sql = 'SELECT\n' + select + '\nFROM' + frm
        if self.__filter:
            sql += '\nWHERE ' + self.__filter
        return sql

    def __find_in_joins(self, table_name, tables_in_from):
        for join in self.__joins:
            if table_name == join.own_table_name and join.reference_table_name not in tables_in_from:
                return join, join.reference_table_name, join.reference_table
            elif table_name == join.reference_table_name and join.own_table_name not in tables_in_from:
                return join, join.own_table_name, join.own_table
        return None, None, None