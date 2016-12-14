from pyelt.mappings.base import BaseTableMapping, FieldMapping


class DvToDmMapping(BaseTableMapping):
    def __init__(self, source, target,automap = False):
        super().__init__(source, target)
        if automap:
            pass
    #         loop door alle veldnamen van de source komt en dan self.map fiels aangeven

    def map_field(self, source: str, target: str = '', transform_func: 'FieldTransformation'=None, ref: str = '') -> None:
        field_mapping = DvToDmFieldMapping(source, target)
        self.field_mappings.append(field_mapping)

    def to_pygram_mapping(self, Dim):
        d = {}
        for field_mapping in self.field_mappings:
            if field_mapping.target.table == Dim:
                d[field_mapping.target.name] = field_mapping.source.name
        return d

class DvToDmFieldMapping(FieldMapping):
    def __init__(self, source, target):
        super().__init__(source, target)
        self.table = target
