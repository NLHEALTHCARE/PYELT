import inspect
from collections import OrderedDict
from typing import Union, List, Dict

from pyelt.datalayers.database import Column, Table, Database, View, Schema, DbFunction
from pyelt.datalayers.dv import * # HubEntity, Link, HybridSat, LinkReference, Sat, DynamicLinkReference, DvValueset
from pyelt.datalayers.dwh import Dwh
from pyelt.datalayers.sor import SorTable, SorQuery
from pyelt.datalayers.valset import DvValueset
from pyelt.helpers.exceptions import PyeltException
from pyelt.mappings.base import BaseTableMapping, FieldMapping, ConstantValue
from pyelt.mappings.transformations import FieldTransformation

# backup gemaakt op 21062016: "sor_to_dv_mappings_old21062016.py"

class SorToEntityMapping(BaseTableMapping):
    def __init__(self, source: str, target: HubEntity, sor: Schema = None, filter: str = '', type: str = '') -> None:
        # target.cls_init_sats()

        # if not sor:
        #     sor_name = source.split('.')[0]
        #     from pyelt.pipeline import Pipeline
        #     p = Pipeline()
        #     db = p.dwh
        #     sor = db.get_sor_schema(sor_name)
        if isinstance(source, str) and 'SELECT' in source.upper() and ' ' in source:
            source = SorQuery(source, sor)
        elif isinstance(source, str):
            source = SorTable(source, sor)
        if not sor and source is SorTable:
            raise Exception('Je moet een sor laag opgeven bij aanmaken van de SorToEntityMapping')
        # target.init_cls()
        super().__init__(source, target, filter)
        self.entity = target
        self.type = type
        if not type and not target.__base__ == HubEntity:
            self.type = target.__subtype__
        self.sat_mappings = {} #type: Dict[str, SorSatMapping]
        self.bk_mapping = None #type: str
        self.key_mappings = []  # type: str

    def map_field(self, source: Union[str, 'ConstantValue', list, dict], target: 'Column' = None, transform_func: 'FieldTransformation'=None, ref: str = '', type: str = '') -> None:
        if not source:
            return
        if not target:
            return
        if target.name == 'bk':
            return self.map_bk(source)
        sat = target.get_table()

        sat_name = sat.cls_get_name()
        sat_mapping = SorSatMapping(self.source, sat, type)
        if not sat_mapping.name in self.sat_mappings:
            self.sat_mappings[sat_mapping.name] = sat_mapping
        else:
            sat_mapping = self.sat_mappings[sat_mapping.name]
        sat_mapping.map_field(source, target, transform_func, ref)
        sat_mapping.type = type
        # field_mapping = FieldMapping(source, target, transform_func, ref = ref)
        # self.field_mappings.append(field_mapping)

    def map_bk(self, source: Union[str,List[str]]) -> None:
        if isinstance(source, list):
            source = " || '.' ||".join(source)
        self.bk_mapping = source

    def map_keys_in_sat(self, source: Union[str, List[str]], target: Union['Column', List['Column']]):
        """In plaats van een bk_mapping kun je ook mappen op een veld combinatie in een reeds bestaande sat.

        Voorwaarde is dat er reeds een gevulde hub met een sat is, die vanuit een andere bron is gevuld.

        Denk hierbij ook aan een zogenaamde keys_sat: een sat met alleen sleutel info vanuit de verschillende bronnen"""
        if isinstance(source, list):
            for index in range(len(source)):
                self.key_mappings.append(KeyMapping(source[index], target[index]))



    def map_fixed_value(self, fixed_value: str, target_field: 'Column' = None) -> None:
        source_field = ConstantValue("'{}'".format(fixed_value))
        self.map_field(source_field, target_field)


class KeyMapping():
    def __init__(self, source, target):
        self.source = source
        self.target = target
        self.sat = target.get_table()

class EntityViewToEntityMapping(SorToEntityMapping):
    def __init__(self, source, target, filter: str = '') -> None:
        super().__init__(source, target, filter)
        self.view = source

class SorSatMapping(BaseTableMapping):
    def __init__(self, source: str, target: Sat, type: str = '') -> None:
        super().__init__(source, target)
        if type:
            self.name += '(' + type + ')'
        self.sor_table = source
        self.sat = target


    def map_field(self, source: Union[str, 'ConstantValue'], target: 'Column', transform_func: 'FieldTransformation'=None, ref: str = '') -> None:
        # automatisch cast naar type toevoegen
        if target.type.lower() != 'text' and isinstance(source, str) and not '(' in source and not ')' in source and not '::' in source:
            source += '::' + target.type
        super().map_field(source, target, transform_func, ref = ref)

    def get_source_fields(self, alias: str = '')-> str:
        if not alias: alias = 'hstg'
        is_hybrid_sat = self.target.__base__ == HybridSat
        # return super().get_source_fields(alias)
        fields = ''  # type: str
        target_json_dict = OrderedDict()
        for field_map in self.field_mappings:
            if field_map.target.type == 'jsonb':
                if field_map.target.name not in target_json_dict:
                    target_json_dict[field_map.target.name] = []
                target_json_dict[field_map.target.name].append(field_map)
            elif isinstance(field_map.source, ConstantValue):
                field = '{},'.format(field_map.source)
            elif isinstance(field_map.source, FieldTransformation):
                # field = '{},'.format(field_map.source.get_sql(alias))
                field = '{},'.format(field_map.source.get_sql())
            elif isinstance(field_map.source, DbFunction):
                # field = '{},'.format(field_map.source.get_sql(alias))
                field = '{},'.format(field_map.source.get_sql(alias))
            elif not alias:
                field = '{},'.format(field_map.source)
            else:
                field = '{}.{},'.format(alias, field_map.source)
            if '[]' in field_map.target.type:
                #array velden
                #eerst komma eraf halen
                field = field[:-1]
                field = "'{" + field + "}',"
            # voorkom eventuele dubbele veldnamen bij hybrid sats
            if is_hybrid_sat and field not in fields:
                fields += field
            else:
                fields += field


        for name, value in target_json_dict.items():
            sql_snippet = """json_build_object("""
            for field_map in value:
                sql_snippet += """'{0}', {0}, """.format(field_map.source.name)
            sql_snippet = sql_snippet[:-2] + ')::jsonb,'
            fields += sql_snippet
        fields = fields[:-1]
        return fields

    def get_sat_fields(self) -> str:
        # return super().get_target_fields()
        fields = []  # type: str
        #json velden willen we helemaal als laatste omdat ze in get_source_fields ook als laatste komen te staan
        json_fields = []# type: str
        for field_map in self.field_mappings:

            field = '{}'.format(field_map.target)
            # voorkom eventuele dubbele veldnamen bij hybrid sats
            if field_map.target.type == 'jsonb':
                if field not in json_fields:
                    json_fields.append(field)
            elif field not in fields:
                fields.append(field)
        # fields = fields[:-1]
        return_sql = ','.join(fields)
        if json_fields:
            return_sql += "," + ','.join(json_fields)
        return return_sql

    def get_fields_compare(self, source_alias: str='', target_alias:str = '') -> str:
        if not source_alias: source_alias = 'hstg'
        if not target_alias: target_alias = 'sat'
        return super().get_fields_compare(source_alias, target_alias)


class SorToLinkMapping(BaseTableMapping):
    auto_generated_sor_fk = 'auto'

    def __init__(self, source: Union[str, 'Table', 'View'], target: 'LinkEntity', sor: Schema, filter: str = '', type: str = '') -> None:
        if isinstance(source, str):
            source = SorTable(source, sor)
        super().__init__(source, target, filter)
        self.sor_table_name = str(source)
        if isinstance(type, str):
            self.type = type
        elif type == HubEntity:
            self.type = type.__subtype__
        self.target = target
        # self.target.cls_init()
        self.hubs = {} #type: Dict[str, Table]
        self.sat_mappings = {}  # type: Dict[str, Sat]

    def get_fk_name(self, entity_cls):
        for prop_name, entity in self.target.__dict__.items():
            if entity == entity_cls or entity == entity_cls.__base__:
                fk = """fk_{}""".format(entity.cls_get_hub_name())
                if prop_name.lower() != entity.__name__.lower():
                    fk = """fk_{}_{}""".format(prop_name.lower(), entity.cls_get_hub_name())
                return fk
        raise Exception('Entity niet gevonden in Link')

    def map_bk(self, source_bk, target_link_ref: Union[LinkReference, DynamicLinkReference], join: str = '', type: Union[str, 'HubEntity'] = ''):
        if not (isinstance(target_link_ref, LinkReference) or isinstance(target_link_ref, DynamicLinkReference)):
            raise PyeltException('Link moet verwijzen naar veld van type LinkReference of DynamicLinkReference')

        if isinstance(target_link_ref, LinkReference):
            hub_name = target_link_ref.hub.__dbname__
            hub = target_link_ref.hub

        elif isinstance(target_link_ref, DynamicLinkReference):
            # type verwijst hier naar de entity die dynamisch gelinkt wordt in de fk
            if not type or isinstance(type, str):
                raise PyeltException('Bij DynamicLinkReference moet type verwijzen naar een afgeleide van HubEntity')
            hub_name = type.cls_get_hub_name()
            hub = type.cls_get_hub()

            self.type = type.__name__.lower()
        hub_alias = target_link_ref.fk.replace('fk_', '')

        source_field = '_id' #type: str
        source_col = Column(source_field, 'integer', tbl=hub) #type: Column
        target_fk = target_link_ref.fk
        target_col = Column(target_fk, 'integer', tbl=hub)
        field_mapping = LinkFieldMapping(source_col, target_col)
        field_mapping.join = join
        field_mapping.source_alias = hub_alias
        if isinstance(source_bk, list):
            source_bk = "|| '.' || ".join(source_bk)
        field_mapping.bk = source_bk
        self.hubs[target_fk] = hub
        self.field_mappings.append(field_mapping)

    def map_sor_fk(self, source_fk: str = '', target_link_ref: Union[LinkReference, DynamicLinkReference] = None, join: str = '', type: Union[str, 'HubEntity'] = ''):
        if not (isinstance(target_link_ref, LinkReference) or isinstance(target_link_ref, DynamicLinkReference)):
            raise PyeltException('Link moet verwijzen naar veld van type LinkReference of DynamicLinkReference')
        if isinstance(target_link_ref, LinkReference):
            hub_name = target_link_ref.hub.__dbname__
            hub = target_link_ref.hub
            # if not type and target_link_ref.sub_entity_type != hub_name.replace('_hub', ''):
            #     type = target_link_ref.sub_entity_type
            # if not type and not target_link_ref.hub.__base__ == HubEntity:
            #     type = target_link_ref.hub.__dbname__.replace('_hub', '')

            if not source_fk or source_fk == SorToLinkMapping.auto_generated_sor_fk:
                # source_fk = target_link_ref.fk
                source_fk = 'fk_{}{}'.format(target_link_ref.sub_entity_type, hub_name)
        elif isinstance(target_link_ref, DynamicLinkReference):
            # type verwijst hier naar de entity die dynamisch gelinkt wordt in de fk
            if not type or isinstance(type, str):
                raise PyeltException('Bij DynamicLinkReference moet type verwijzen naar een afgeleide van HubEntity')
            hub = type.cls_get_hub()
            self.type = type.__name__.lower()
        source_col = Column(source_fk, 'integer', tbl=self.sor_table_name)

        target_fk = target_link_ref.fk
        target_col = Column(target_fk, 'integer', tbl=hub)
        field_mapping = LinkFieldMapping(source_col, target_col)
        field_mapping.join = join
        self.hubs[target_fk] = hub
        self.field_mappings.append(field_mapping)

    def map_entity(self, target_link_ref: Union[LinkReference, DynamicLinkReference] = None, bk: Union[List[str], str] = '', join: str = '', type: Union[str, 'HubEntity'] = ''):
        if isinstance(target_link_ref, DynamicLinkReference) and not bk:
            hub_name = type.cls_get_hub_name()
            sor_fk = 'fk_{}'.format(hub_name)
            self.map_sor_fk(sor_fk, target_link_ref, join, type)
        elif bk:
            self.map_bk(bk, target_link_ref, join, type)
        else:
            self.map_sor_fk('', target_link_ref, join, type)

    # def map_dynamic_entity(self, source_entity: HubEntity, target_link_ref: DynamicLinkReference,  type: str = '', bk: str = '', join: str = ''):
    #     if bk:
    #         self.map_bk(bk, target_link_ref, join, type)
    #     else:
    #         hub_name = target_link_ref.entity_cls.get_hub_name()
    #         sor_fk = 'fk_{}{}'.format(type, hub_name)
    #         type = source_entity
    #         self.map_sor_fk(sor_fk, target_link_ref, join, type)

    def map_field(self, source: Union[str, 'ConstantValue'], target: 'Column' = None, transform_func: 'FieldTransformation' = None, ref: str = '', type: str = '') -> None:
        if not target:
            return
        sat = target.get_table()
        sat_name = sat.cls_get_name()
        sat_mapping = SorSatMapping(self.source, sat, type)
        if not sat_mapping.name in self.sat_mappings:
            self.sat_mappings[sat_mapping.name] = sat_mapping
        else:
            sat_mapping = self.sat_mappings[sat_mapping.name]
        sat_mapping.map_field(source, target, transform_func, ref)
        sat_mapping.type = type

    def map_entity_old(self, entity_cls: HubEntity,  bk: str = '', join: str = '', type: str = ''):
        # is_hybrid = False
        hub_name = entity_cls.cls_get_hub_name()
        hub = entity_cls.cls_get_hub()

        if bk:
            source_field = '_id' #type: str
            source_col = Column(source_field, 'integer', tbl=hub) #type: Column
        # elif is_hybrid:
        #     source_field = 'fk_{}{}'.format(type, hub_name)
        #     source_col = Column(source_field, 'integer', tbl=self.sor_table)
        else:
            # source_field = 'fk_' + hub_name
            source_field = 'fk_{}{}'.format(type, hub_name)
            source_col = Column(source_field, 'integer', tbl=self.sor_table_name)

        # target_field = '_fk_' + hub_name #type: str
        target_field = 'fk_{}'.format(hub_name)
        target_field = self.get_fk_name(entity_cls)
        # if target_field in self.hubs:
        #     target_field = '_fk_parent_' + self.type + hub_name
        target_col = Column(target_field, 'integer', tbl=hub)
        field_mapping = LinkFieldMapping(source_col, target_col)
        field_mapping.bk = bk
        field_mapping.join = join
        self.hubs[target_field] = hub
        self.field_mappings.append(field_mapping)

    # def map_source(self, source, target: 'HubEntity'):
    #     source_col = Column(source, 'integer', tbl=self.sor_table)
    #     target_field = """_fk_source_{}""".format(target.get_hub_name())
    #     target_col = Column(target_field, 'integer', tbl=target.hub)
    #     field_mapping = LinkFieldMapping(source_col, target_col)
    #     self.hubs[target_field] = target.hub
    #     self.field_mappings.append(field_mapping)
    #
    # def map_target(self, source, target: 'HubEntity'):
    #     source_col = Column(source, 'integer', tbl=self.sor_table)
    #     target_field = """_fk_target_{}""".format(target.get_hub_name())
    #     target_col = Column(target_field, 'integer', tbl=target.hub)
    #     field_mapping = LinkFieldMapping(source_col, target_col)
    #     # field_mapping.bk = bk
    #     # field_mapping.join = join
    #     self.hubs[target_field] = target.hub
    #     self.field_mappings.append(field_mapping)
    #
    #     # source_col = Column(source, 'integer', tbl=self.sor_table)
    #     # target_field = target.__qualname__
    #     # mro = target.__mro__
    #     # currentframe = inspect.currentframe()
    #     # fs = inspect.getouterframes(currentframe)
    #     # target_field = self.get_fk_name(target)
    #     # # if target_field in self.hubs:
    #     # #     target_field = '_fk_parent_' + self.type + hub_name
    #     # target_col = Column(target_field, 'integer', tbl=target.hub)
    #     # field_mapping = LinkFieldMapping(source_col, target_col)
    #     # # field_mapping.bk = bk
    #     # # field_mapping.join = join
    #     # self.hubs[target_field] = target.hub
    #     # self.field_mappings.append(field_mapping)



class EntityViewToLinkMapping(SorToLinkMapping):
    def __init__(self, source, target, filter=''):
        super().__init__(source, target, filter)
        self.view = source

    def add_mapping(self, *args, **kwargs):
        if len(args) == 1:
            hub_name = args[0]
            source_fk = '_id'
            target_fk = 'fk_' + hub_name
        elif len(args) == 2:
            source_fk = args[0]
            hub_name = args[1]
            target_fk = 'fk_' + hub_name

        db = Dwh()
        hub = Table(hub_name, db.dv)
        source_field = Column('_id',  tbl=hub)
        target_field = Column(target_fk, 'integer')
        field_mapping = LinkFieldMapping(source_field, target_field)
        if 'join' in kwargs:
           field_mapping.join = kwargs['join']
        self.field_mappings.append(field_mapping)

    def get_source_fks(self):
        fks = ''
        for field_mapping in self.field_mappings:
            alias = field_mapping.source.table.type
            if alias:
                fks += '{}._id, '.format(alias)
            else:
                fks += '{}._id, '.format(field_mapping.source.table)
        fks = fks[:-2]
        return fks

    def get_from(self, dv_name = 'dv'):
        from_sql = ''
        for field_mapping in self.field_mappings:
            join_tbl = field_mapping.get_source_table()
            alias = join_tbl.type
            if alias:
                from_sql += '{} AS {}, '.format(join_tbl, alias)
            else:
                from_sql += '{}, '.format(join_tbl)
        from_sql = from_sql[:-2]
        return from_sql

    def get_join_where(self, dv_name = 'dv'):

        join_sql = ''
        for field_mapping in self.field_mappings:
            join = field_mapping.join
            bk = field_mapping.bk
            if bk:
                join_tbl = field_mapping.get_source_table()
                alias = join_tbl.type
                if alias:
                    join_sql += ' {0}.bk = {1} AND '.format(alias, bk)
                else:
                    join_sql += ' {0}.bk = {1} AND '.format(join_tbl, bk)
        return join_sql

    def get_fks_compare(self, source_alias='', target_alias=''):

        fks_compare = ''

        for field_mapping in self.field_mappings:

            # source_alias = self.get_alias_of_source_tbl(field_mapping, fks_compare)
            if isinstance(field_mapping.source, ConstantValue):
                fks_compare += '{0} = {1}.{2} AND '.format(field_mapping.source, target_alias, field_mapping.target)
            else:
                fks_compare += '{0}.{1} = link.{2} AND '.format(field_mapping.source.table, field_mapping.source, field_mapping.target)
        fks_compare = fks_compare[:-4]
        return fks_compare

class LinkFieldMapping(FieldMapping):
    def __init__(self, source: 'Column', target: 'Column', transform_func: FieldTransformation = None) -> None:
        super().__init__(source, target, transform_func)
        self.join = '' #type: str
        self.bk = '' #type: str

        self.is_view_mapping = False  #type: bool

    def get_source_table(self) -> 'Table':
        if self.join:
            return self.join.split('.')[0]
        else:
            return self.source.table.name

# class SorToRefMapping(BaseTableMapping):
#     def __init__(self, source: Union[str, Dict[str, str]], ref_type: str)-> None:
#         target = 'referenties'
#
#         super().__init__(source, target)
#
#         self.ref_type = ref_type
#         if not isinstance(source, dict):
#             self.sor_table = source
#             self.source_code_field = ''
#             self.source_descr_field = "''"
#
#     def map_code_field(self, source_code_field: str)-> None:
#         target = 'code'
#         if '.' in source_code_field:
#             source_code_field = source_code_field.replace(self.sor_table + '.' , '')
#         self.map_field(source_code_field, target)
#         self.source_code_field =source_code_field
#
#     def map_descr_field(self, source_descr_field: str)-> None:
#         target = 'descr'
#         if '.' in source_descr_field:
#             source_descr_field = source_descr_field.replace(self.sor_table + '.', '')
#         self.map_field(source_descr_field, target)
#         self.source_descr_field = source_descr_field
#

class SorToValueSetMapping(BaseTableMapping):
    def __init__(self, source: str, target: DvValueset, sor: Schema = None ) -> None:
        if isinstance(source, str) and 'SELECT' in source.upper() and ' ' in source:
            source = SorQuery(source, sor)
        elif isinstance(source, str):
            source = SorTable(source, sor)
        super().__init__(source, target)
        self.sor_table = source
        self.valueset = target

    def map_field(self, source: Union[str, 'ConstantValue'], target: 'Column', transform_func: 'FieldTransformation'=None, ref: str = '') -> None:
        # automatisch cast naar type toevoegen
        if target.type.lower() != 'text' and isinstance(source, str) and not '(' in source and not ')' in source and not '::' in source:
            source += '::' + target.type
        super().map_field(source, target, transform_func, ref = ref)

    def get_source_fields(self, alias: str = '')-> str:
        if not alias: alias = 'hstg'
        fields = ''  # type: List[str]
        target_json_dict = OrderedDict()
        for field_map in self.field_mappings:
            if field_map.target.type == 'jsonb':
                if field_map.target.name not in target_json_dict:
                    target_json_dict[field_map.target.name] = []
                target_json_dict[field_map.target.name].append(field_map)
            elif isinstance(field_map.source, ConstantValue):
                field = '{},'.format(field_map.source)
            elif isinstance(field_map.source, FieldTransformation):
                # field = '{},'.format(field_map.source.get_sql(alias))
                field = '{},'.format(field_map.source.get_sql())
            elif isinstance(field_map.source, DbFunction):
                # field = '{},'.format(field_map.source.get_sql(alias))
                field = '{},'.format(field_map.source.get_sql(alias))
            elif not alias:
                field = '{},'.format(field_map.source)
            else:
                field = '{}.{},'.format(alias, field_map.source)
            if '[]' in field_map.target.type:
                #array velden
                #eerst komma eraf halen
                field = field[:-1]
                field = "'{" + field + "}',"
            # voorkom eventuele dubbele veldnamen bij hybrid sats
            if field not in fields:
                fields += field

        for name, value in target_json_dict.items():
            sql_snippet = """json_build_object("""
            for field_map in value:
                sql_snippet += """'{0}', {0}, """.format(field_map.source.name)
            sql_snippet = sql_snippet[:-2] + ')::jsonb,'
            fields += sql_snippet
        fields = fields[:-1]
        return fields

    def get_target_fields(self) -> str:
        fields = []
        #json velden willen we helemaal als laatste omdat ze in get_source_fields ook als laatste komen te staan
        json_fields = []
        for field_map in self.field_mappings:
            field = '{}'.format(field_map.target)
            if field_map.target.type == 'jsonb':
                if field not in json_fields:
                    json_fields.append(field)
            elif field not in fields:
                fields.append(field)
        return_sql = ','.join(fields)
        if json_fields:
            return_sql += "," + ','.join(json_fields)
        return return_sql


class SorToValueSetMapping_old(BaseTableMapping):
    def __init__(self, source: Union[str, Dict[str, str]], target: str) -> None:
        target = Table('valueset_code', )
        super().__init__(source, target)

        self.valueset = target
        self.source_type_field = ''
        self.source_level_field = "''"

        if not isinstance(source, dict):
            self.sor_table = source
            self.source_code_field = ''
            self.source_descr_field = "''"

    def map_code_field(self, source_code_field: str) -> None:
        target = 'code'
        if '.' in source_code_field:
            source_code_field = source_code_field.replace(self.sor_table + '.', '')
        self.map_field(source_code_field, target)
        self.source_code_field = source_code_field

    def map_descr_field(self, source_desc_field: str) -> None:
        target = 'weergave_naam'
        if '.' in source_desc_field:
            source_desc_field = source_desc_field.replace(self.sor_table + '.', '')
        self.map_field(source_desc_field, target)
        self.source_descr_field = source_desc_field

    def map_type_field_old(self, source_type_field: str, source_oid_field: str = "''") -> None:
        target = 'valueset_naam'

        self.map_field(source_type_field, target)
        self.source_type_field = source_type_field
        if source_oid_field:
            target = 'valueset_type_oid'
            self.map_field(source_oid_field, target)

        self.source_oid_field = source_oid_field

    def map_type_field(self, valueset_field: str) -> None:
        target = 'valueset_naam'

        self.map_field(valueset_field, target)
        self.source_type_field = valueset_field


    def map_level_field(self, source_level_field: str) -> None:
        target = 'niveau'

        self.map_field(source_level_field, target)
        self.source_level_field = source_level_field

