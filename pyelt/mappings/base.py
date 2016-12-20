import inspect
from typing import Union, Tuple, Any
from pyelt.datalayers.database import Column, Table, View, DbFunction
from pyelt.datalayers.dv import DvEntity
from pyelt.mappings.transformations import FieldTransformation
from pyelt.mappings.validations import Validation


class BaseMapping():
    def __init__(self, source: Any, target: Any) -> None:
        self.source = source
        self.target = target
        if inspect.isclass(target):
            self.name = '{} -> {}'.format(str(self.source)[:80], self.target.cls_get_name())
        else:
            self.name = '{} -> {}'.format(str(self.source)[:80], str(self.target))

    def __str__(self) -> str:
        return self.name

    def validate(self):
        pass

class BaseTableMapping(BaseMapping):
    def __init__(self, source: Any, target: Union[str, 'Table'], filter: str = '') -> None:
        super().__init__(source,target)
        #filter voor ophalen van data uit source
        self.filter = filter #type: str
        self.field_mappings = [] #type: List[FieldMapping]
        self.key_mappings = [] #type: List[str]
        self.validations = [] #type: List[Validation]
        #type: List[Union[ConstantValue, FieldTransformation, Column, str]]

    def map_field(self, source: str, target: str = '', transform_func: 'FieldTransformation'=None, ref: str = '') -> None:
        if source and target:
            field_mapping = FieldMapping(source, target, transform_func, ref = ref)
            self.field_mappings.append(field_mapping)

    # def parse_mapping_str(self, input_str: str) -> Tuple[str, str]:
    #     """input str in format
    #     'source          =>      target  [type]'"""
    #     source = input_str.split('=>')[0].strip()
    #     target = input_str.split('=>')[1].strip()
    #     return source, target

    def get_source_fields(self, alias: str='') -> str:
        fields = '' #type: str
        for field_map in self.field_mappings:
            if isinstance(field_map.source, ConstantValue):
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
            #voorkom eventuele dubbele veldnamen bij hybrid sats
            if field not in fields:
                fields += field
        fields = fields[:-1]
        return fields

    def get_target_fields(self, alias:str='') -> str:
        fields = []  # type: str
        for field_map in self.field_mappings:
            if not alias:
                field = '{}'.format(field_map.target)
            else:
                field = '{}.{}'.format(alias, field_map.target)
            #voorkom eventuele dubbele veldnamen bij hybrid sats
            if field not in fields:
                fields.append(field)
        # fields = fields[:-1]
        return ','.join(fields)

    def get_fields_compare(self, source_alias: str='', target_alias: str='') -> str:
        fields_compare = '' #type: str
        for field_map in self.field_mappings:
            if isinstance(field_map.source, ConstantValue):
                fields_compare += "{1} != COALESCE({0}.{2}, '') OR ".format(target_alias, field_map.source, field_map.target)
            elif isinstance(field_map.source, FieldTransformation):
                source = field_map.source.get_sql(source_alias)
                fields_compare += "{0} != COALESCE({1}.{2}, '') OR ".format(source, target_alias, field_map.target)
            elif '(' in field_map.source.name and target_alias:
                # source bevat functie: geen alias toevoegen
                fields_compare += "{1} != COALESCE({0}.{2}, '') OR ".format(target_alias, field_map.source, field_map.target)
            elif source_alias and target_alias:
                fields_compare += "COALESCE({0}.{2}, '') != COALESCE({1}.{3}, '') OR ".format(source_alias, target_alias, field_map.source, field_map.target)
            elif source_alias and not target_alias:
                fields_compare += "COALESCE({0}.{2} != COALESCE({2}, '') OR ".format(source_alias, field_map.source, field_map.target)
            elif target_alias and not source_alias:
                fields_compare += "COALESCE({1} != COALESCE({0}.{2}, '') OR ".format(target_alias, field_map.source, field_map.target)
            else:
                fields_compare += "COALESCE({0} != COALESCE({1}, '') OR ".format(field_map.source, field_map.target)
        fields_compare = fields_compare[:-3]
        return fields_compare

    # def get_keys(self, alias=''):
    #     if not alias:
    #         return ','.join(self.keys)
    #     else:
    #         keys = ''
    #         for key in self.keys:
    #             keys += '{}.{},'.format(alias, key)
    #             keys = keys[:-1]
    #         return keys
    #
    # def get_keys_compare(self, source_alias='', target_alias=''):
    #     keys_compare = ''
    #     for key in self.keys:
    #         keys_compare += '{0}.{2} = {1}.{2} AND '.format(source_alias, target_alias, key)
    #     keys_compare = keys_compare[:-4]
    #     return keys_compare

class FieldMapping(BaseMapping):
    def __init__(self, source: Union[str, 'Column', 'FieldTransformation'], target: Union[str, 'Column'], transform_func: 'FieldTransformation' = None, ref: str = '') -> None:
        source_col = None #type: Union[FieldTransformation, Column]
        target_col = None #type: Column
        if isinstance(source, str):
            if " " in source or ('(' in source and ')' in source):
                source_col = FieldTransformation(name=source, sql=source)
            else:
                source_col = Column(source)
        elif isinstance(source, DbFunction):
            source_col = source
        else:
            source_col = source
        if transform_func:
            transform_func.field_name = source_col.name
            source_col = transform_func
        if isinstance(target, str):
            target_col = Column(target)
        else:
            target_col = target
        super().__init__(source_col, target_col)
        self.transformation = transform_func
        self.source_type = '' #type: str
        self.target_type = '' #type: str
        self.ref = ref #type: str


class ConstantValue:
    def __init__(self, value, type: str = 'text', alias: str = '') -> None:
        self.value = value
        self.type = type

    def __str__(self) -> str:
        if isinstance(self.value, str) and self.type == 'text':
            if "'" in self.value:
                return "{}".format(self.value)
            else:
                return "'{}'".format(self.value)
        else:
            return "{}".format(self.value)


