from pygrametl.tables import Dimension,FactTable

from pyelt.datalayers.database import Column
from pyelt.datalayers.dv import OrderedMembersMetaClass, DVTable


class Dim(DVTable, metaclass=OrderedMembersMetaClass):
    @classmethod
    def get_name(cls):
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
    def init_cols(cls):
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                if not col.name:
                    col.name = col_name
                col.table = cls


    @classmethod
    def get_column_names(cls):
        list_col_names = []
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                list_col_names.append(col.name)
        return list_col_names


    @classmethod
    def to_pygram_dim(cls,schema_name):
        cls.init_cols()
        dim = Dimension(
            name= schema_name + '.' + cls.get_name(),
            key='id',
            attributes= cls.get_column_names())
        print('test jan ',cls.get_column_names(),schema_name,cls.get_name())
        return dim

    def to_pygram_bulk_dim(self):
        pass
#     to do


class Fact(DVTable, metaclass=OrderedMembersMetaClass):
    @classmethod
    def get_name(cls):
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
    def get_key_names(cls):
        list_col_names = []
        for col_name, col in cls.__ordereddict__.items():
            if not isinstance(col, DmReference) and isinstance(col,Column):
                list_col_names.append(col_name)
        return list_col_names

    @classmethod
    def get_measure_names(cls):
        list_col_names = []
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, DmReference):
                list_col_names.append(col_name)
        return list_col_names

    @classmethod
    def to_pygram_fact(cls, schema_name):
        fct = FactTable(
            name=schema_name + '.' + cls.get_name(),
            keyrefs=cls.get_key_names(),
            measures=cls.get_measure_names())
        print(schema_name,cls.get_name(),cls.get_key_names(),cls.get_measure_names())
        return fct


class DmReference():
    def __init__(self, dim_cls):
        self.dim_cls = dim_cls

    def get_fk_field_name(self):
        fk_name = self.dim_cls.cls_get_name()
        fk_name = fk_name.replace('dim_', 'fk_')
        return fk_name
