from pygrametl.tables import Dimension

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
    def to_pygram_dim(cls):
        dim = Dimension(
            name='dm.' + cls.get_name(),
            key='id',
            attributes=['voorletters', 'achternaam'])
        return dim

class Fact(DVTable, metaclass=OrderedMembersMetaClass):
    @classmethod
    def get_name(cls):
        if cls.name:
            return cls.name
        else:
            full_name = cls.__qualname__
            fact_name = full_name.split('.')[0]

            fact_name = fact_name.replace('Fact', '')
            fact_name = fact_name.lower()
            fact_name = 'fact_' + fact_name
            cls.name = fact_name
            return fact_name


class DmReference():
    def __init__(self, dim_cls):
        self.dim_cls = dim_cls

    def get_fk_field_name(self):
        fk_name = self.dim_cls.get_name()
        fk_name = fk_name.replace('dim_', 'fk_')
        return fk_name
