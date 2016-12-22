import inspect
from collections import OrderedDict
from typing import List, Dict

from pyelt.datalayers.database import Schema, Column, Columns, Table
from pyelt.orm.dv_objects import HubData, SatData, EntityData
from pyelt.helpers.global_helper_functions import camelcase_to_underscores

class DV(Schema):
    """Schema voor ORM. Status = try-out """
    def __init__(self):
        pass
        # self.patienten = [] #type: List[Patient]

    def load(self, entity_cls, filter = ''):
        return_list = []
        view_name = entity_cls.__name__.lower().replace('_entity', '') + '_view'
        sql = """SELECT * FROM {} WHERE 1 = 1 AND {}""".format(view_name, filter)
        rows = self.execute(sql)
        for row in rows:
            obj = entity_cls()
            for field_name,field_value in row.items():
                obj.__dict__[field_name] = field_value
            return_list.append(obj)
        return return_list

    def register_table(self, cls):
        self.tables[''] = cls()


class DVTable(Table):
    """Datavault abstract table. Bevat de standaard velden zoals _id, _runid enz. Standaard velden beginnen met _ in de database en in het framework.
    Hubs en sats erver van deze base over.  """
    __cls_is_init = False
    _schema = 'dv'
    # _id = Columns.IntColumn()
    # _runid = Columns.FloatColumn()
    # _source_system = Columns.TextColumn()
    name = ''

    def __init__(self):
        super().__init__(self.__class__.name, None)
        self._id = Column('_id', type='int', tbl=self)
        self._runid = Column('_runid', type='numeric', tbl=self)
        self._source_system = Column('_source_system', type='text', tbl=self)

    @classmethod
    def cls_init_cols(cls):
        """Initieer de kolommen. De kolomnaam en de tabel wordt gezet. De kolomnaam wordt uit de class-props gehaald en de tabel uit de class zelf.
        Kolommen worden in de domein als volgt gedefinieerd:
        class Sat1(Sat):
            veld1 = Columns.TextColumn()

        In deze cls_init_cols wordt de naam 'veld1' als kolomnaam gehanteerd en de tabel sat1_sat als tabel
       """
        if cls.__cls_is_init:
            return
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                if not col.name:
                    col.name = col_name
                col.table = cls

        cls.__cls_is_init = True
        base_entity_cls = cls.cls_get_entity()
        base_entity_cls.cls_init()

    @classmethod
    def cls_get_entity(cls) -> 'DvEntity':
        """Haalt de entity op die hoort bij deze sat.

        class Foo(DvEntity):
            class Bar(Sat):
                veld1 = Columns.TextColumn()

        Bar.cls_cls_get_entity() -> returns Foo


        class Baz(Foo):
            class Qux(Sat):
                veld1 = Columns.TextColumn()

        Qux.cls_cls_get_entity() -> returns Foo

        """
        full_name = cls.__qualname__
        entity_name = full_name.split('.')[0]
        import sys
        entity_cls = getattr(sys.modules[cls.__module__], entity_name)  # globals()[entity_name]
        # haal de super class op die als base de DvEntity heeft, deze heeft de naam vd super in zich
        base_entity_cls = entity_cls.cls_get_class_with_dventity_base()
        return entity_cls

    @classmethod
    def cls_get_columns(cls) -> List[Column]:
        """:returns columns als list"""
        cls.cls_init_cols()
        cols = []
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                cols.append(col)
        return cols


class Hub(DVTable, HubData):
    """Datavault Hub. Heeft maar een veld naast de standaard velden van de DvTable, namelijk de bk.

    bk staat voor business key. Een business key is:

    * betekenis volle sleutel
    * bedrijfsbreed gedragen, onafhankelijk van een systeem
    * onveranderlijk ,want het is een sleutel
    * mag bestaan uit meer velden die als sting aan elkaar worden geplakt
    """

    def __init__(self, name: str = '') -> None:
        DVTable.__init__(self)
        HubData.__init__(self)
        self.name = name
        self.bk = Columns.TextColumn('bk')
        self.bk.table = self

class OrderedMembersMetaClass(type):
    """Metaclass voor de sats. Deze zorgt ervoor dat de class properties in de class-dict gesorteerd blijven zoals ze in de code staan.
    Hierdoor kun je volgorde van de velden in de db bepalen.
    Naast de default __dict__ van de class komt er bij deze implementatie van deze meta class een extra __ordereddict__ property

    gebruik:
    class Sat(DVTable, metaclass=OrderedMembersMetaClass):
        ...
    class Adres(Sat):
        ...
    for col_name, col in Adres.__ordereddict__.items():
        ...
    """

    @classmethod
    def __prepare__(mcs, name, bases):
        return OrderedDict()

    def __new__(cls, name, bases, classdict):
        result = type.__new__(cls, name, bases, classdict)
        result.__ordereddict__ = classdict
        return result


class Sat(DVTable, SatData, metaclass=OrderedMembersMetaClass):
    """
    Datavault Sat. Een sat bevat de velden van de datavault.
    De sleutel van de sat is de _id = hub._id samen met de _runid
    """

    def __new__(cls, *args, **kwargs):
        cls.cls_init_cols()
        return super().__new__(cls)



    @classmethod
    def cls_get_name(cls):
        """Maakt de tabelnaam die hoort bij de sat.

        voorbeelden:
        class Foo(DvEntity):
            class BarSat(Sat):
                veld1 = Columns.TextColumn()

        geeft de naam foo_sat_bar.

        --------------
        class Foo(DvEntity):
            class Bar(Sat):
                veld1 = Columns.TextColumn()

        geeft ook de naam foo_sat_bar.

        --------------
        class Foo(DvEntity):
            class Default(Sat):
                veld1 = Columns.TextColumn()

        geeft de naam foo_sat.

        --------------
        class Base(DvEntity):
            ...
        class Foo(Base):
            class Bar(Sat):
                veld1 = Columns.TextColumn()

        geeft de naam base_sat_bar.
        """
        if cls.name:
            return cls.name
        else:
            base_entity_cls = cls.cls_get_entity()
            entity_name = base_entity_cls.__name__.lower()
            full_name = cls.__qualname__
            sat_name = full_name.replace('.', '_sat_')
            sat_name = entity_name + '_sat_' + cls.__name__.lower()
            sat_name = sat_name.replace('_default', '')
            cls.name = sat_name
            return sat_name

    @classmethod
    def cls_get_short_name(cls):
        """:returns class name, lower case, zonder tussenvoegsel _sat"""
        return cls.__name__.lower().replace('_sat', '')


    def __init__(self):
        """Voor ORM gedeelte. Status = try-out"""
        DVTable.__init__(self)
        SatData.__init__(self)




class HybridSat(Sat):
    """Datavault Hybrid Sat. Een Hybrid Sat is een sat met een extra type-veld in de sleutel.
    De sleutel bestaat hiermee uit 3 velden (_id, _runid en type) ivp 2 zolas bij een gewone sat (_id en _runid)
    """
    class Types(metaclass=OrderedMembersMetaClass):
        """Overschrijf deze class en zet hierin de types als string-constanten. Dit is enkel nodig voor het aanmaken van standaard entity-views

        Bijvoorbeeld

        class Foo(DvEntity):
            class Bar(HybridSat):
                class Types(HybridSats.Types):
                    type1 = 'type1'
                    type2 = 'type2'


        """
        pass

    @classmethod
    def cls_get_types(cls) -> List[str]:
        """
        :return: list met str-types
        """
        types = []
        for key, val in cls.Types.__ordereddict__.items():
            if not key.startswith('__'):
                types.append(val)
        return types


class DvEntity(EntityData):
    """Datavault entity.

    Dit de hub met bijbehorende sats. Let wel, deze class wordt geen database tabel, dat worden alleen de hub en de sats
    """
    __cls_is_init = False
    _schema_name = 'dv' #type: str

    hub = Hub() #type: Hub
    bk = hub.bk #type: Columns.TextColumn
    sats = {} #type: Dict[str, Sat]

    @classmethod
    def cls_init(cls):
        """initieert de classe verder door gebruikte property namen om te zetten.
        initieert alle sats
        initieert hub met bk en schema naam
        """

        if not cls.__cls_is_init:
            cls.cls_init_sats()
            cls.hub = cls.cls_get_hub()
            cls.hub._schema_name = cls._schema_name
            cls.bk = cls.hub.bk
            cls.__cls_is_init = True

    @classmethod
    def cls_init_sats(cls):
        """prepareert alle sats in de entity
        per sat:

        *  init cols
        * zet schema name
        """
        sats = cls.cls_get_sats()
        for sat in sats.values():
            sat.cls_init_cols()
            sat._schema_name = cls._schema_name

    @classmethod
    def cls_get_name(cls) -> str:
        """
        :return: class name, lower case + _entity

        example::
            class FooBar(DvEntity):
                pass

            FooBar.cls_get_name()
            -> foo_bar_entity

            class FooBarEntity(DvEntity):
                pass

            FooBarEntity.cls_get_name()
            -> foo_bar_entity
        """
        # entity_name = cls.__name__.lower() +         '_entity'
        entity_name = camelcase_to_underscores(cls.__name__)
        entity_name = entity_name.replace('_entity', '') + '_entity'

        return entity_name

    @classmethod
    def cls_get_hub(cls) -> 'Hub':
        cls.hub.name = cls.cls_get_hub_name()
        hub_name = cls.cls_get_hub_name()
        return Hub(hub_name)

    @classmethod
    def cls_get_schema(cls, dwh) -> 'Schema':
        schema_name = cls._schema_name
        schema = dwh.get_schema(schema_name)
        return schema



    @classmethod
    def cls_get_class_with_dventity_base(cls) -> 'DvEntity':
        if DvEntity in cls.__bases__:
            return cls
        else:
            for base in cls.__bases__:
                if hasattr(base, "cls_get_class_with_dventity_base"):
                    return base.cls_get_class_with_dventity_base()

    @classmethod
    def cls_get_hub_name(cls) -> str:
        dventity = cls.cls_get_class_with_dventity_base()
        hub_name = dventity.__name__.lower().replace('_entity', '').replace('entity', '') + '_hub'
        return hub_name

    @classmethod
    def cls_get_hub_type(cls) -> str:
        """Naam van de (sub)class geldt als type"""
        type = cls.__name__.lower().replace('_entity', '').replace('entity', '')
        return type

    @classmethod
    def cls_get_view_name(cls) -> str:
        # dventity = cls.get_class_with_dventity_base()
        view_name = cls.__name__.lower().replace('_entity', '').replace('entity', '') + '_view'
        return view_name

    @classmethod
    def cls_get_sats(cls) -> Dict[str, Sat]:
        all_sats = {}
        base_class_sats = cls.cls_get_base_class_sats()
        this_class_sats = cls.cls_get_this_class_sats()
        sub_class_sats = cls.cls_get_sub_classes_sats()
        all_sats.update(base_class_sats)
        all_sats.update(this_class_sats)
        all_sats.update(sub_class_sats)
        return all_sats

    @classmethod
    def cls_get_this_class_sats(cls) -> Dict[str, Sat]:
        sats = {}
        class_with_dv_entity_base = cls.cls_get_class_with_dventity_base()
        for key, sat_cls in cls.__dict__.items():
            if inspect.isclass(sat_cls) and Sat in sat_cls.__mro__:
                sat_cls.name = class_with_dv_entity_base.__name__.lower() + '_sat_' + sat_cls.__name__.lower()
                sat_cls.name = sat_cls.name.replace('_default', '')
                if sat_cls.__base__ == HybridSat:
                    sat_cls.type = key
                sats[key] = sat_cls
        return sats

    @classmethod
    def cls_get_sub_classes_sats(cls) -> Dict[str, Sat]:
        #subclasses
        sats = {}
        class_with_dv_entity_base = cls.cls_get_class_with_dventity_base()
        for sub_cls in cls.__subclasses__():
            if isinstance(sub_cls, type):
                for key, sat_cls in sub_cls.__dict__.items():
                    if inspect.isclass(sat_cls) and Sat in sat_cls.__mro__:
                        sat_cls.name = class_with_dv_entity_base.__name__.lower() + '_sat_' + sat_cls.__name__.lower()
                        sat_cls.name = sat_cls.name.replace('_default', '')
                        if sat_cls.__base__ == HybridSat:
                            sat_cls.type = key
                        sats[key] = sat_cls
        return sats

    @classmethod
    def cls_get_base_class_sats(cls):
        sats = {}
        class_with_dv_entity_base = cls.cls_get_class_with_dventity_base()
        for base in cls.__mro__:
            for key, sat_cls in base.__dict__.items():
                if inspect.isclass(sat_cls) and (sat_cls.__base__ == Sat or sat_cls.__base__ == HybridSat) and key[:1].isupper():
                    sat_cls.name = class_with_dv_entity_base.__name__.lower() + '_sat_' + sat_cls.__name__.lower()
                    sat_cls.name = sat_cls.name.replace('_default', '')
                    if sat_cls.__base__ == HybridSat:
                        sat_cls.type = key
                    sats[key] = sat_cls
        return sats

    def sats(self):
        sats = {}
        for sat_name, sat in self.__dict__.items():
            if isinstance(sat, Sat):
                sats[sat_name] = sat
        return sats

    def __init__(self):
        EntityData.__init__(self)
        self.dv = DV()

    def __str__(self):
        return self.__class__.cls_get_name()


class LinkReference():
    def __init__(self, entity_cls: 'DvEntity', name: str = ''):
        self.entity_cls = entity_cls
        self.entity_cls_with_dventity_base = entity_cls.cls_get_class_with_dventity_base()
        self.name = name

    def get_fk(self):
        fk = """fk_{}_hub""".format(self.name.lower())
        return fk


class DynamicLinkReference():
    """Kan verwijzen naar verschillende hubs, afhandelijk van type"""

    def __init__(self, name: str = ''):
        self.name = name

    def get_fk(self):
        fk = """fk_{}_hub""".format(self.name.lower())
        return fk

class Link():
    _schema_name = 'dv'
    @classmethod
    def cls_init(cls):
        for prop_name, prop in cls.__dict__.items():
            if isinstance(prop, LinkReference):
                if not prop.name:
                    entity_name = prop.entity_cls_with_dventity_base.__name__.lower()
                    if entity_name.lower() != prop_name.lower():
                        prop.name = prop_name.lower() + '_' + entity_name
                    else:
                        prop.name = prop_name
            if isinstance(prop, DynamicLinkReference):
                prop.name = prop_name
        cls.cls_init_sats()

    @classmethod
    def cls_init_sats(cls):
        sats = cls.cls_get_sats()
        for sat in sats.values():
            sat.cls_init_cols()

    @classmethod
    def cls_get_name(cls):

        link_name = camelcase_to_underscores(cls.__name__)

        if not link_name.endswith('_link'):
            link_name += '_link'
        return link_name

    @classmethod
    def cls_get_schema(cls, dwh) -> 'Schema':
        schema_name = cls._schema_name
        if schema_name == 'dv':
            schema = dwh.dv
        elif schema_name == 'valset':
            schema = dwh.valset
        return schema

    @classmethod
    def cls_get_entities_old(cls):
        entities = {}
        for key, entity_cls in cls.__dict__.items():
            if isinstance(entity_cls, type) and DvEntity in entity_cls.__bases__:
                # entity_cls.hub.name = entity_cls.__name__.lower() + '_hub'
                entities[key.lower()] = entity_cls
        return entities

    @classmethod
    def cls_get_link_refs(cls) -> Dict[str, LinkReference]:
        link_refs = {}
        for prop_name, link_ref in cls.__dict__.items():
            if isinstance(link_ref, LinkReference):
                link_refs[prop_name] = link_ref
        return link_refs

    @classmethod
    def cls_get_sats(cls):
        sats = {}
        for key, sat_cls in cls.__dict__.items():
            if inspect.isclass(sat_cls) and (sat_cls.__base__ == Sat or sat_cls.__base__ == HybridSat) and key[:1].isupper():
                # if isinstance(sat_cls, type) and sat_cls.__base__ == Sat:
                sat_cls.name = cls.__name__.lower() + '_sat_' + sat_cls.__name__.lower()
                sat_cls.name = cls.cls_get_name() + '_sat_' + sat_cls.__name__.lower()
                sat_cls.name = sat_cls.name.replace('_default', '')
                if sat_cls.__base__ == HybridSat:
                    sat_cls.type = key
                sats[key] = sat_cls
        return sats

    def __init__(self, *args):
        self.hubs = []
        for arg in args:
            self.hubs.append(arg)

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

class DvValueset(Table, metaclass=OrderedMembersMetaClass):
    _schema_name = 'valset'
    __init = False
    valueset_naam = Columns.TextColumn('valueset_naam')
    code = Columns.TextColumn('code')
    omschrijving = Columns.TextColumn('omschrijving')

    @classmethod
    def cls_get_name(cls) -> str:
        from pyelt.helpers.global_helper_functions import camelcase_to_underscores
        name = camelcase_to_underscores(cls.__name__)
        return name

    @classmethod
    def cls_get_schema(cls, dwh) -> 'Schema':
        schema_name = cls._schema_name
        schema = dwh.get_schema(schema_name)
        return schema

    @classmethod
    def cls_init_cols(cls):
            # cls.__ordereddict__.update(cls.__base__.__ordereddict__)

        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                if not col.name:
                    col.name = col_name
                col.table = cls

    @classmethod
    def cls_get_columns(cls) -> Dict[str, Column]:
        cols = {}
        cls.cls_init_cols()
        for col_name, col in cls.__base__.__ordereddict__.items():
            if isinstance(col, Column):
                cols[col_name] = col
        for col_name, col in cls.__ordereddict__.items():
            if isinstance(col, Column):
                cols[col_name] = col
        return cols

class EnsembleView():

    @classmethod
    def cls_init(cls):
        name = cls.__name__.lower()
        entity_dict = {}
        for key, entity_or_link_cls in cls.__dict__.items():
            entity_dict[key] = entity_or_link_cls

    def __init__(self, name='', entity_and_link_list=[]):
        self.name = name
        self.entity_and_link_list = entity_and_link_list
        self.entity_dict = {}
        for entity in entity_and_link_list:
            self.entity_dict[entity.get_name()] = entity  # maakt de dictionary aan.

    def add_entity_or_link(self, entity_or_link, alias=''):
        """

        :rtype: object
        """
        if not alias:
            alias = entity_or_link.cls_get_name()
        # testen of alias al bestaat
        assert alias not in self.entity_dict.keys(), '{} bestaat al; kies een nieuw alias'.format(alias)
        self.entity_dict[alias] = entity_or_link



