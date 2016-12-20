from pygrametl.tables import Dimension,FactTable, BulkDimension, CachedDimension

from pyelt.datalayers.database import Column
from pyelt.datalayers.dv import OrderedMembersMetaClass, DVTable


class Dim(DVTable, metaclass=OrderedMembersMetaClass):
    _lookup_fields = []
    @classmethod
    def cls_get_name(cls):
        if cls.name:
            return cls.name
        else:
            full_name = cls.__qualname__
            dim_name = full_name.split('.')[0]

            dim_name = dim_name.replace('Dim', '')
            dim_name = dim_name.lower()
            dim_name = 'dim_' + dim_name
            cls.name = dim_name
            return dim_name

    @classmethod
    def cls_init_cols(cls):
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                if not col.name:
                    col.name = col_name
                col.table = cls

    @classmethod
    def cls_get_column_names(cls):
        list_col_names = []
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                list_col_names.append(col.name)
        return list_col_names

    @classmethod
    def cls_get_lookup_fields(cls):
        cls.cls_init_cols()
        lookup_fields = [col.name for col in cls._lookup_fields]
        return lookup_fields

    @classmethod
    def cls_to_pygram_dim(cls, schema_name, lookup_fields = []):
        cls.cls_init_cols()
        if not lookup_fields:
            lookup_fields = cls.cls_get_lookup_fields()
        if lookup_fields:
            dim = CachedDimension(
                name= schema_name + '.' + cls.cls_get_name(),
                key='id',
                attributes= cls.cls_get_column_names(),
                lookupatts = lookup_fields,
                cachefullrows=True)
        else:
            dim = Dimension(
                name=schema_name + '.' + cls.cls_get_name(),
                key='id',
                attributes=cls.cls_get_column_names())
        return dim

    @classmethod
    def cls_to_pygram_bulk_dim(cls, schema_name, lookup_fields = [], bulkloader = None):
        cls.cls_init_cols()
        dim = BulkDimension(
            name = schema_name + '.' + 'dim_patient',
            key = 'id',
            attributes = cls.cls_get_column_names(),
            lookupatts = lookup_fields,
            bulksize = 50000,
            nullsubst = '0',  # anders kan bulkdimension er niet mee omgaan
            bulkloader = bulkloader)
        return dim

    @classmethod
    def cls_to_pygram_mapping(cls):
        d = {}
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                d[col_name] = 'null' #col.default_value
        return d


class Fact(DVTable, metaclass=OrderedMembersMetaClass):
    @classmethod
    def cls_get_name(cls):
        if cls.name:
            return cls.name
        else:
            full_name = cls.__qualname__
            fact_name = full_name.split('.')[0]
            if fact_name.endswith('Fact'):
                fact_name = fact_name[:-4]
            fact_name = fact_name.lower()
            fact_name = 'fact_' + fact_name
            cls.name = fact_name
            return fact_name


    @classmethod
    def cls_get_measure_names(cls):
        list_col_names = []
        for col_name, col in cls.__ordereddict__.items():
            if not isinstance(col, DmReference) and isinstance(col,Column):
                list_col_names.append(col_name)
        return list_col_names

    @classmethod
    def cls_get_key_names(cls):
        list_col_names = []
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, DmReference):
                list_col_names.append(col_name)
        return list_col_names

    @classmethod
    def cls_to_pygram_fact(cls, schema_name):
        fct = FactTable(
            name=schema_name + '.' + cls.cls_get_name(),
            keyrefs=cls.cls_get_key_names(),
            measures=cls.cls_get_measure_names())
        return fct


class DmReference():
    def __init__(self, dim_cls):
        self.dim_cls = dim_cls

    def get_fk_field_name(self):
        fk_name = self.dim_cls.cls_get_name()
        fk_name = fk_name.replace('dim_', 'fk_')
        return fk_name
