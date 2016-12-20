from pyelt.datalayers.database import Column
from pyelt.datalayers.dm import Dim, DmReference
from pyelt.mappings.base import BaseTableMapping, FieldMapping


class DvToDimMapping(BaseTableMapping):
    def __init__(self, source, target,automap = False):
        super().__init__(source, target)
        if automap:
            pass
    #         loop door alle veldnamen van de source komt en dan self.map fiels aangeven

    def map_field(self, source: str, target: str = '', transform_func: 'FieldTransformation'=None, ref: str = '') -> None:
        field_mapping = DvToDmFieldMapping(source, target)
        self.field_mappings.append(field_mapping)

    def to_pygram_mapping(self):
        dim = self.target
        d = dim.cls_to_pygram_mapping()
        for field_mapping in self.field_mappings:
            if field_mapping.source.name and field_mapping.target.table == dim:
                d[field_mapping.target.name] = field_mapping.source.name
        return d

class DvToDmFieldMapping(FieldMapping):
    def __init__(self, source, target):
        super().__init__(source, target)
        self.table = target

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

