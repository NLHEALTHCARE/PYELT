from pyelt.datalayers.database import Column
from pyelt.datalayers.dm import Dim, DmReference
from pyelt.datalayers.dv import HubEntity, Hub
from pyelt.mappings.base import BaseTableMapping, FieldMapping
from pyelt.mappings.transformations import FieldTransformation


class DvToDimMapping(BaseTableMapping):
    def __init__(self, source, target):
        # if not isinstance(source, str) and issubclass(source, HubEntity):
        #     source.cls_init_sats()
        # target.cls_init_cols()
        super().__init__(source, target)

    def map_field(self, source: str, target: str = '', transform_func: 'FieldTransformation'=None, type = '') -> None:
        if source == '':
            source = None
        field_mapping = DvToDmFieldMapping(source, target, source_type = type)
        self.field_mappings.append(field_mapping)

    def to_pygram_mapping(self):
        dim = self.target
        d = dim.cls_to_pygram_mapping()
        for field_mapping in self.field_mappings:
            if not field_mapping.source:
                d[field_mapping.target.name] = 'null'
            elif field_mapping.source.name and field_mapping.target.table == dim:
                d[field_mapping.target.name] = field_mapping.source.name
        return d

    def generate_sql(self, dwh):
        if not issubclass(self.source, HubEntity):
            return ""
        entity = self.source #type: HubEntity
        # entity.cls_init_sats()
        params = {}
        params['fields'] = self.__get_fields()
        dv_schema = entity.cls_get_schema(dwh).name
        params['schema'] = dv_schema
        params['hub'] = entity.cls_get_hub_name()
        params['join'] = self.__get_join(dv_schema)
        sql = """SELECT {fields} FROM {schema}.{hub} AS hub {join}""".format(**params)
        return sql

    def __get_fields(self):
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

    def __get_join(self, dv_schema):
        sql_join = ""
        sats = self.__get_sats()
        for key, sat_props in sats.items():
            params = {}
            params['dv_schema'] = dv_schema
            params['sat'] =sat_props['sat_name']
            params['sat_alias'] = sat_props['sat_alias']
            params['sat_hybrid_type'] = sat_props['hybrid_sat_type']
            if params['sat_hybrid_type']:
                sql_join += """LEFT OUTER JOIN {dv_schema}.{sat} AS {sat_alias} ON {sat_alias}._id = hub._id AND {sat_alias}._active AND {sat_alias}.type = '{sat_hybrid_type}'\r\n""".format(**params)
            else:
                sql_join += """LEFT OUTER JOIN {dv_schema}.{sat} AS {sat_alias} ON {sat_alias}._id = hub._id AND {sat_alias}._active\r\n""".format(**params)
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


class DvToDmFieldMapping(FieldMapping):
    def __init__(self, source, target, source_type = ''):

        super().__init__(source, target)
        self.table = target
        self.source_type = source_type

class DvToFactMapping(BaseTableMapping):
    def __init__(self, source, target,automap = False):
        super().__init__(source, target)


    def map_field(self, source: str, target: str = '', transform_func: 'FieldTransformation'=None, ref: str = '') -> None:
        field_mapping = DvToDmFieldMapping(source, target)
        self.field_mappings.append(field_mapping)

    def to_pygram_mapping(self):
        fact = self.target
        d = {}
        for col_name, col in fact.__ordereddict__.items():
            if isinstance(col, DmReference) or isinstance(col, Column):
                d[col_name] = 'null'  # col.default_value
        for field_mapping in self.field_mappings:
            if field_mapping.source.name:
                d[field_mapping.target.get_fk_field_name()] = field_mapping.source.name
        return d

    def map_lookup_field(self, source: str, dim_reference: DmReference):
        dim = dim_reference.ref_table
        dim._lookup_fields

    def map_function(self, get_bronsysteem_id, fk_grouperdatum):
        pass


