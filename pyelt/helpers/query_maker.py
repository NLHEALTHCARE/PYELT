from pyelt.datalayers.database import *
from pyelt.datalayers.dv import *
from pyelt.mappings.transformations import FieldTransformation

class QueryMaker():
    class Alias():
        def __init__(self, cls, alias):
            self.cls = cls
            self.alias = alias

    def __init__(self, name):
        self.name = name
        self.__fields = {}
        self.__tables = {}
        self.__joins = {}

    def select(self, column, alias = ''):
        if not isinstance(column, Column):
            raise Exception(' gebruik column ref voor select')


        stack = inspect.stack()
        caller = stack[1].code_context[0]
        caller = caller[caller.find('(') + 1: caller.find(')')]
        if ',' in caller:
            caller = caller.split(',')[0]
        tbl_alias = caller.split('.')[0].lower()
        if not alias:
            alias = caller.lower().replace('.', '_')
            alias = alias.replace('default_', '')

        sat = column.table
        sat.hub = tbl_alias
        sat_alias = caller.split('.')[0] + '_' + caller.split('.')[1]
        sat_alias = sat_alias.lower()


        if alias in self.__fields:
            raise Exception(alias + ' bestaat al in query. Gebruik een andere alias.')
        else:
            params = {}
            params['tbl_alias'] = sat_alias
            params['col_name'] = column.name
            params['column_alias'] = alias
            sql = '{tbl_alias}.{col_name} as {column_alias}'.format(**params)
            self.__fields[alias] = sql

            self.__tables[sat_alias] = sat
            self.__tables[tbl_alias] = sat
            sql = '{0}._id = {1}._id and {0}._active'.format(sat_alias, tbl_alias)
            self.__joins[tbl_alias + sat_alias] = sql



    def frm(self, entity):
        stack = inspect.stack()
        caller = stack[1].code_context[0]
        tbl_alias = caller[caller.find('(') + 1: caller.find(')')]
        tbl_alias = tbl_alias.lower()
        self.__tables[tbl_alias] = entity

    def join(self, link_ref, hub_entity):
        stack = inspect.stack()
        caller = stack[1].code_context[0]
        caller = caller[caller.find('(') + 1: caller.find(')')]
        if ',' in caller:
            arg1 = caller.split(',')[0]
            arg2 = caller.split(',')[1].strip()
        link_alias = arg1.split('.')[0].lower()
        hub_alias = arg2.lower().strip()
        fk_name = link_ref.fk

        join_name = caller.replace('.', '_').lower()
        sql = '{}.{} = {}._id'.format(link_alias, fk_name, hub_alias)
        self.__joins[join_name] = sql

    def where(self, filter):
        pass

    def to_sql(self, dwh):
        select = ''
        for alias, sql in self.__fields.items():
            select += sql + ',\n'
        select = select[:-2]
        frm = ''
        first = True
        for tbl_alias, ent in self.__tables.items():
            if HubEntity in ent.__mro__:
                frm += '{}.{} as {},\n'.format(ent.Hub.__dbschema__, ent.Hub.__dbname__, tbl_alias)
            elif LinkEntity in ent.__mro__ :
                frm += '{}.{} as {},\n'.format(ent.Link.__dbschema__, ent.Link.__dbname__, tbl_alias)
            else:
                frm += '{}.{} as {},\n'.format(ent.__dbschema__, ent.__dbname__, tbl_alias)
        frm = frm[:-2]
        where = ''
        for join in self.__joins.values():
            where += join + ' AND \n'
        where = where[:-5]


        sql = 'SELECT ' + select + ' \nFROM ' + frm + ' \nWHERE ' + where
        return sql


    def orderby(self, *args):
        pass

