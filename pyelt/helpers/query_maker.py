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

    def select(self, column, alias = '', type = ''):
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

class QueryMaker2():
    def __init__(self, name):
        self.name = name
        self.__fields = {}
        self.__tables = {}
        self.__joins = {}

    def init_test(self, fields, tables, joins):
        self.__fields = fields
        self.__tables = tables
        self.__joins = joins


    def select(self, column, alias = '', type = ''):
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

    def to_sql2(self, param):
        select = ''
        for alias, field in self.__fields.items():
            sql = '{}.{} as {}'.format(field.table.__dbname__, field.name, alias)
            select += sql + ',\n'
        select = select[:-2]

        frm = ''
        for alias, tbl in self.__tables.items():
            first = alias, tbl
            frm += self.get_joins(alias, tbl)
            break

            if isinstance(tbl, dict):
                hub = tbl['hub']

                hub_alias = hub.__dbname__
                if not frm:
                    frm += '{}.{} as {}\n'.format(hub.__dbschema__, hub.__dbname__, hub_alias)
                else:
                    link_ref = self.__get_link_ref_by_hub(hub)
                    params = {}
                    params['schema'] = 'dv'
                    params['link'] = 'link'
                    params['link_alias'] = 'lnk'
                    params['fk'] = link_ref.fk
                    params['hub_alias'] = hub_alias
                    frm += ' LEFT JOIN {schema}.{link} as {link_alias} ON {link_alias}.{fk} = {hub_alias}._id\n'.format(**params)
                for sat_alias, sat in tbl['sats'].items():
                    frm += ' LEFT JOIN {0}.{1} as {1} ON {1}._id = {2}._id and {1}._active\n'.format(sat.__dbschema__, sat.__dbname__, hub_alias)
            elif isinstance(tbl, Link):
                pass

        where = ''
        sql = 'SELECT ' + select + ' \nFROM ' + frm + ' \nWHERE ' + where
        return sql

    def __get_link_ref_by_hub(self, hub) -> LinkReference:
        for name, join in self.__joins.items():
            ref = join #type: LinkReference
            if ref.ref_table.__dbname__ == hub.__dbname__:
                return ref

    def get_joins(self, alias, tbl):
        frm = ''
        for alias, tbl in self.__tables.items():
            frm += self.get_joins(alias, tbl)