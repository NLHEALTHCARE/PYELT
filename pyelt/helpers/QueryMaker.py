from pyelt.datalayers.database import *
from pyelt.datalayers.dv import *
from pyelt.mappings.transformations import FieldTransformation


class QueryMaker():



    def __init__(self, name = ''):
        self.name = name
        self.__elements = {}

    def append(self, elm, alias = ''):
        if isinstance(elm, Column):
            if not alias:
                alias = elm.name
        else:
            if not alias:
                alias = elm.__dbname__
            if not alias:
                alias = elm.__name__
        if alias in self.__elements:
            raise Exception(alias + ' bestaat al in query. Gebruik een andere alias.')
        else:
            self.__elements[alias] = elm


    def to_sql(self, dwh):
        params = {}
        # dv_schema = entity.cls_get_schema(dwh).name
        # params['schema'] = dv_schema

        params['fields'] = self.__get_fields()
        params['from'] = self.__get_from()
        params['filter'] = self.__get_filter()

        sql = "SELECT {fields} \nFROM {from}\nWHERE {filter}".format(**params)
        return sql


    def __get_fields_new(self):
        fields = ""
        sats_in_query = self.__get_sats()
        for map in self.field_mappings:
            if not map.source:
                fields += "NULL AS {}, \n".format(map.target.name)
                continue
            source_table = map.source.get_table()
            source_field = map.source  # type: Column
            # if type(source_table) is Hub:
            if 'hub' in source_table.__dbname__:
                fields += "hub.{} AS {}, \n".format(source_field.name, map.target.name)
            else:
                sat_name = map.source.get_table().name
                key = sat_name
                if map.source_type:
                    # voor hybride sats, type toevoegen aan sat naam
                    key += map.source_type
                sat_alias = sats_in_query[key]['sat_alias']
                if isinstance(map.source, FieldTransformation):
                    fields += str(map.source).replace('{tbl_alias}', sat_alias)
                    fields += " AS {}, \n".format(map.target.name)
                else:
                    fields += "{}.{} AS {}, \n".format(sat_alias, source_field.name, map.target.name)
        fields = fields[:-3]
        return fields

    def __get_join(self):
        sql_join = ""
        sats = self.__sats
        for sat_name, sat in sats.items():
            params = {}
            params['dv_schema'] = sat.__dbschema__
            params['sat'] =sat_name
            params['sat_alias'] = sat_name
            params['sat_hybrid_type'] = '' #sat_props['hybrid_sat_type']
            params['hub'] = sat.hub_name
            if params['sat_hybrid_type']:
                sql_join += """LEFT OUTER JOIN {dv_schema}.{sat} AS {sat_alias} ON {sat_alias}._id = {hub}._id AND {sat_alias}._active AND {sat_alias}.type = '{sat_hybrid_type}'\r\n""".format(**params)
            else:
                sql_join += """LEFT OUTER JOIN {dv_schema}.{sat} AS {sat_alias} ON {sat_alias}._id = {hub}._id AND {sat_alias}._active\r\n""".format(**params)
        sql_join = sql_join[:-2]
        return sql_join

    def __get_sats(self):
        sats = {}
        sat_num = 0
        for map in self.field_mappings:
            if not map.source:
                continue
            source_table = map.source.get_table()
            # if type(source_table) is Hub:
            if '_hub' in source_table.__dbname__:
                continue
            sat_name = source_table.name
            key = sat_name
            if map.source_type:
                #voor hybride sats, type toevoegen aan sat naam
                key += map.source_type
            if not key in sats:
                sat_num += 1
                alias = 'sat' + str(sat_num)
                sats[key] = {'sat_name': sat_name, 'sat_alias': alias, 'hybrid_sat_type': map.source_type}
        return sats

    def __get_fields(self):
        self.__sats = {}
        self.__hubs = {}
        self.__entities = {}
        fields = ''
        for alias, elm in self.__elements.items():
            if isinstance(elm, Column):
                col = elm
                hub_name = col.table.__dbname__[:col.table.__dbname__.find('_sat')] + '_hub'
                col.table.hub_name = hub_name
                self.__sats[col.table.__dbname__] = col.table
                # hub = Table(hub_name)
                # self.__hubs[hub_name] = hub_name
                fields += '{}.{} as {},\n'.format(col.table.__dbname__, col.name, alias)
            elif HubEntity in elm.__mro__:
                tbl = elm.Hub
                self.__hubs[tbl.name] = tbl
                self.__entities[elm.__name__] = elm
            elif LinkEntity in elm.__mro__:
                self.__links[tbl.name] = elm.Link
        fields = fields[:-2]
        return fields

    def __get_from(self):
        from_sql = ''
        params = {}
        for hub_name, hub in self.__hubs.items():
            params['hub'] = hub_name
            params['schema'] = hub.__dbschema__
            if not from_sql:
                from_sql = """{schema}.{hub} AS {hub} \n""".format(**params)

        for link_name, link in self.__links.items():
            params['link'] = link_name
            params['schema'] = link.__dbschema__
            for ref in link.__dict__.values():
                if ref.ref_name == hub_name:
                    pass
            from_sql += """LEFT OUTER JOIN {schema}.{link} AS {link} ON {link}.{fk_hub} = {hub}._id\n""".format(**params)
        params['join'] = self.__get_join()
        from_sql = """{schema}.{hub} AS {hub} \n{join}""".format(**params)
        #
        # from_sql = ''
        # for elm in self.__elements:
        #     tbl = None
        #     if isinstance(elm, Column):
        #         pass
        #     elif HubEntity in elm.__mro__:
        #         tbl = elm.Hub
        #     elif Hub in elm.__mro__:
        #         tbl = elm
        #     elif Sat in elm.__mro__:
        #         tbl = elm
        #     if tbl:
        #         from_sql += '{}.{} as {}  '.format(tbl.__dbschema__, tbl.__dbname__, tbl.__dbname__)
        # from_sql = from_sql[:-2]
        return from_sql

    def __get_filter(self):
        return '1=1'


class QueryMaker2():
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
        print(tbl_alias)
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


class Foo(HubEntity):
    __dbschema__ = 'dvschema'

    class Bar(Sat):
        veld1 = Columns.TextColumn()
        veld2 = Columns.TextColumn()

    class Taz(Sat):
        veld3 = Columns.TextColumn()
        veld4 = Columns.TextColumn()


class Qux(Foo):
    class Kur(HybridSat):
        nummer =  Columns.IntColumn()


class Pog(Qux):
    class Bur(Sat):
        veld5 = Columns.TextColumn()


class CamelCaseTest(HubEntity):
    class Default(Sat):
        pass

    class CamelCaseSat(Sat):
        camelCaseCol = Columns.TextColumn()


class BlaLink(Link):
    foo = LinkReference(Foo)
    pog = LinkReference(Pog)

    class Default(Sat):
        veld6 = Columns.TextColumn()
        veld7 = Columns.TextColumn()
        veld8 = Columns.TextColumn()

#
# qry = QueryMaker('myview')
# qry.append(Foo)
# qry.append(Foo.Bar.veld1)
# qry.append(Foo.Bar.veld2)
# qry.append(Foo.Taz.veld3)
# sql = qry.to_sql(None)
# print(sql)
#
# print ('---------------')


