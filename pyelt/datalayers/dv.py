from pyelt.datalayers.dv_metaclasses import *

class AbstractOrderderTable(Table, metaclass=OrderedTableMetaClass):
    """De colummen die zijn opgegeven blijven gesorteerd"""

    @classmethod
    def cls_create_dbname(cls):
        """override deze methode in subclasses"""
        return cls.__dbname__

    @classmethod
    def cls_get_name(cls) -> str:
        """Geeft database name terug
        :return: return __dbname__
        :rtype: str"""
        return cls.__dbname__

    @classmethod
    def cls_get_schema(cls, db) -> 'Schema':
        """Geeft schema object terug. met daarin een gekoppelde db. Je kunt dit object reflecten"""
        schema_name = cls.__dbschema__
        schema = db.get_schema(schema_name)
        return schema

    @classmethod
    def cls_get_columns(cls) -> List[Column]:
        """:return: return __cols__
        :rtype: List[Column]"""
        return cls.__cols__

    @classmethod
    def cls_get_key(cls) -> List[Column]:
        """:return: return __cols__
        :rtype: List[Column]"""
        key = [col.name for col in cls.__cols__ if col.is_key]
        return key

class DvTable(AbstractOrderderTable):
    """Basis abstracte tabel voor alle datavault tabellen. Deze bevat de vast velden die in alle datavault tabellen nodig zijn."""
    _id = Columns.SerialColumn()
    _runid = Columns.FloatColumn()
    _source_system = Columns.TextColumn()
    _valid = Columns.BoolColumn()
    _validation_msg = Columns.TextColumn()
    _insert_date = Columns.DateTimeColumn()


class Hub(DvTable):
    """Datavault hub. Bevat business key (bk)"""
    type = Columns.TextColumn()
    bk = Columns.TextColumn(unique=True)



class Sat(DvTable):
    """Datavault sat. Technische sleutel is de _id samen met de _runid"""
    _id = Columns.IntColumn(pk=True)
    _runid = Columns.FloatColumn(pk=True)
    _finish_date = Columns.TextColumn()
    _active = Columns.BoolColumn(default_value=True, indexed=True)
    _revision = Columns.IntColumn()
    # wordt aangemaakt in metaclass: hub = FkReference(Hub, _id)


    @classmethod
    def cls_get_short_name(cls) ->str:
        """:returns class name, lower case, zonder tussenvoegsel _sat"""
        return cls.__name__.lower().replace('_sat', '')


class HybridSat(Sat):
    """Heeft extra type column als onderdeel van de sleutel"""
    class Types():
        pass
    type = Columns.TextColumn(pk=True)

    @classmethod
    def cls_get_types(cls) -> List[str]:
        types = []
        for key, val in cls.Types.__dict__.items():
            if not key.startswith('__'):
                types.append(val)
        return types


class Link(DvTable):
    type = Columns.TextColumn()
    @classmethod
    def cls_get_link_refs(cls):
        link_refs = {}
        for prop_name, link_ref in cls.__dict__.items():
            if isinstance(link_ref, LinkReference):
                link_refs[prop_name] = link_ref
        return link_refs


class HybridLink(Link):
    class Types:
        pass

    @classmethod
    def cls_get_types(cls):
        types = []
        for key, val in cls.Types.__dict__.items():
            if not key.startswith('__'):
                types.append(val)
        return types


class LinkReference(FkReference):
    def __init__(self, ref_cls: 'Hub', name: str = ''):
        hub = ref_cls
        if HubEntity in ref_cls.__mro__:
            self.sub_entity_type = ref_cls.__subtype__
            hub = ref_cls.Hub
        super().__init__(hub)
        self.name = name

    @property
    def hub(self):
        return self.ref_table

    # def get_fk(self):
    #
    #     fk = """fk_{}""".format(self.hub.__dbname__)
    #
    #     return fk

class DynamicLinkReference():
    pass



class HubEntity(metaclass=HubEntityMetaClass):
    """Een HubEntity is een verzameling van 1 hub met 0,1 of meerdere sats"""
    __dbschema__ = 'dv'
    __dbname__ = ''
    __subtype__ = ''

    class Hub(Hub):
        pass

    @classmethod
    def cls_get_sats(cls) -> Dict[str, Sat]:
        """:returns __sats__
        """
        return cls.__sats__

    @classmethod
    def cls_get_hub_type(cls) -> str:
        """Naam van de (sub)class geldt als type"""
        type = cls.__name__.lower().replace('_entity', '').replace('entity', '')
        return type

    @classmethod
    def cls_get_view_name(cls) -> str:
        view_name = cls.__name__.lower().replace('_entity', '').replace('entity', '') + '_view'
        return view_name

    @classmethod
    def cls_get_schema(cls, dwh) -> 'Schema':
        schema = dwh.get_schema(cls.__dbschema__)
        return schema

    @classmethod
    def cls_get_name(cls) -> str:
        return cls.__name__.lower() + '_entity'

    @classmethod
    def cls_get_hub_name(cls) -> str:
        return cls.Hub.__dbname__

    def __str__(self):
        return self.__class__.__name__.lower + '_entity'


class LinkEntity(metaclass=LinkEntityMetaClass):
    """Een link entity is een link met 0, 1 of meerdere sats"""
    __dbschema__ = 'dv'
    __dbname__= ''
    class Link(Link):
        pass

    @classmethod
    def cls_get_schema(cls, dwh) -> 'Schema':
        schema = dwh.get_schema(cls.__dbschema__)
        return schema

    @classmethod
    def cls_get_name(cls) ->str:
        return cls.__name__.lower().replace('_link', '') + '_link'

    @classmethod
    def cls_get_sats(cls) -> Dict[str, Sat]:
        return cls.__sats__

class EnsembleView():
    pass
#####################################
