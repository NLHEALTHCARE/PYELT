import inspect
from collections import OrderedDict

from pyelt.datalayers.database import *
# from pyelt.datalayers.dv import Sat, Hub
from pyelt.helpers.global_helper_functions import camelcase_to_underscores


class OrderedTableMetaClass(type):
    """Metaclass voor de tabellen, zoals hubs en sats.
    Deze Metaclass zorgt ervoor dat de class properties in de class-dict gesorteerd blijven zoals ze in de code staan.
    Hierdoor kun je volgorde van de velden in de db bepalen.
    Naast de default __dict__ van de class komt er bij deze implementatie van deze meta class een extra __ordereddict__ property

    gebruik::

        class Sat(DVTable, metaclass=OrderedMembersMetaClass):
            ...
        class Adres(Sat):
            ...
        for col_name, col in Adres.__ordereddict__.items():
            ...

    Hiernaast heeft de metaclass de volgende meta-velden:

    * __dbname__: dit wordt geinitieerd met de naam van de tabel zoals die in de database zal verschijnen.
                Let wel: sats binnen een entity krijgen de naam van de entity zie hiervoor HubEntityMetaClass
    * __dbschema__: hierin de naam van het het database schema
    * __cols__: list van Columns. De columns worden bij nieuwe class aanroep vooraf geinitieerd met veldnamen. Dat zijn de namen van de properties
    """

    @classmethod
    def __prepare__(mcs, name, bases):
        return OrderedDict()

    def __new__(mcs, name, bases, classdict):
        # we maken een nieuwe lege class-type aan. Deze is nodig voor de volledige mro.
        # (de parameter bases geef namelijk alleen de rechtstreekse bases van een class
        new_dummy_dvtable_cls = type.__new__(mcs, name, bases, {})

        # loop reversed door alle bases en breidt telkens dict uit met die van de bases, beginnend met de meest diepe base in hierarchie
        dict = OrderedDict()
        for base in reversed(new_dummy_dvtable_cls.__mro__):
            if '__ordereddict__' in base.__dict__:
                dict.update(base.__ordereddict__)
        dict.update(classdict)

        # nu we de dict hebben gaan we eigenlijke type aanmaken
        new_dvtable_cls = type.__new__(mcs, name, bases, dict)
        new_dvtable_cls.__ordereddict__ = dict
        new_dvtable_cls.__dbname__ = camelcase_to_underscores(name) #wordt bij sats later weer overschreven in EntityMetaclass
        if getattr(new_dvtable_cls, "cls_create_dbname", None):
            new_dvtable_cls.__dbname__ = new_dvtable_cls.cls_create_dbname()
        new_dvtable_cls.__cols__ = []
        # door alle kolommen loopen en hier een kopie van maken en die toevoegen aan de class.
        for prop_name, prop in new_dvtable_cls.__ordereddict__.items():
            if prop_name == 'aanvragend_specialisme':
                debug = True
            if isinstance(prop, Columns.RefColumn):
                new_col = Columns.RefColumn( prop.valueset_name, prop_name)
                new_col.type = 'text'
                new_col.table = new_dvtable_cls
                new_dvtable_cls.__cols__.append(new_col)
                setattr(new_dvtable_cls, prop_name, new_col)
            elif isinstance(prop, Column):
                new_col = Column(prop_name, type=prop.type, tbl=new_dvtable_cls)
                new_col.is_key = prop.is_key
                new_col.nullable = prop.nullable
                new_col.is_indexed = prop.is_indexed
                new_col.is_unique = prop.is_unique
                new_col.default_value = prop.default_value
                new_dvtable_cls.__cols__.append(new_col)
                setattr(prop, 'name', prop_name)
                setattr(prop, 'table', new_dvtable_cls)
                setattr(new_dvtable_cls, prop_name, prop)
                setattr(new_dvtable_cls, prop_name, new_col)
            elif isinstance(prop, FkReference):
                if prop_name.lower() == prop.ref_table.__name__.lower():
                    fk = 'fk_' + prop.ref_table.__dbname__
                elif prop_name.lower() + '_hub' == prop.ref_table.__dbname__:
                    fk = 'fk_' + prop.ref_table.__dbname__
                elif prop.fk:
                    fk = prop.fk
                else:
                    fk = 'fk_' + prop_name.lower() + '_' + prop.ref_table.__dbname__
                prop.set_fk_name(fk)
                new_col = Column(fk, type='int', tbl=new_dvtable_cls)
                new_dvtable_cls.__cols__.append(new_col)
                setattr(new_dvtable_cls, fk, new_col)
        new_dvtable_cls.__ordereddict__ = dict
        return new_dvtable_cls

class HubEntityMetaClass(type):
    """Metaclass voor Hubentity. Deze metaclass zorgt ervoor dat er tenminste een Hub is en dat dbnames van de sats alvast worden gezet"""
    def __new__(mcs, name, bases, classdict):
        new_hub_entity_cls = type.__new__(mcs, name, bases, classdict)
        # als HubEntity rechtstreeks in de bases staat dan krijgt de entity een hub
        add_hub_to_cls = False
        for base in bases:
            if 'HubEntity' in str(base):
                add_hub_to_cls = True
        if add_hub_to_cls:
            from pyelt.datalayers.dv import Hub
            hub_cls = type('Hub', (Hub,), {})
            if not new_hub_entity_cls.__dbname__:
                hub_cls.__dbname__ = camelcase_to_underscores(name) + '_hub'
            else:
                hub_cls.__dbname__ = new_hub_entity_cls.__dbname__ + '_hub'
            hub_cls.__dbschema__ = new_hub_entity_cls.__dbschema__
            hub_cls.name = hub_cls.__dbname__
            new_hub_entity_cls.Hub = hub_cls
            setattr(new_hub_entity_cls, '__subtype__', '')
            # new_hub_entity_cls.__subtype__ = ''
        else:
            subcls_type =  new_hub_entity_cls.__name__.lower().replace('entity', '').replace('hub', '')
            setattr(new_hub_entity_cls, '__subtype__', subcls_type)
            # new_hub_entity_cls.Hub.__subtype__ = subcls_type

        # zoeken naar de entity in de mro met de rechtstreekse overerving van de HubEntity
        entity_with_hub = None
        for base in new_hub_entity_cls.__mro__:
            if 'HubEntity' in str(base.__bases__):
                entity_with_hub = base

        # satnames zetten
        new_hub_entity_cls.__sats__ = OrderedDict()
        from pyelt.datalayers.dv import Sat
        for key, sat_cls in new_hub_entity_cls.__dict__.items():
            if inspect.isclass(sat_cls) and Sat in sat_cls.__mro__ :
                sat_cls.__dbschema__ = entity_with_hub.__dbschema__
                sat_name = camelcase_to_underscores(key).replace('_sat', '').replace('sat', '')
                sat_cls.__dbname__ = camelcase_to_underscores(entity_with_hub.__name__) + '_sat_' + sat_name
                sat_cls.__dbname__ = sat_cls.__dbname__.replace('_default', '')
                sat_cls.name = sat_cls.__dbname__
                ref_hub = FkReference(new_hub_entity_cls.Hub, new_hub_entity_cls.Hub._id)
                setattr(sat_cls, 'hub', ref_hub)
                sat_cls.__ordereddict__['hub'] = ref_hub

        # sat collectie maken
        new_hub_entity_cls.__sats__ = OrderedDict()
        for base in reversed(new_hub_entity_cls.__mro__):
            for key, sat_cls in base.__dict__.items():
                if inspect.isclass(sat_cls) and Sat in sat_cls.__mro__:
                    new_hub_entity_cls.__sats__[sat_cls.name] = sat_cls
        return new_hub_entity_cls

class LinkEntityMetaClass(type):
    def __new__(mcs, name, bases, classdict):
        new_link_entity_cls = type.__new__(mcs, name, bases, classdict)
        link_cls = new_link_entity_cls.Link
        link_cls.__dbname__ = camelcase_to_underscores(name).replace('_link', '').replace('_entity', '') + '_link'
        if '__dbschema__' in classdict:
            link_cls.__dbschema__ = classdict['__dbschema__']

        # zoeken naar de entity in de mro met de rechtstreekse overerving van de LinkEntity
        entity_with_link = None
        for base in new_link_entity_cls.__mro__:
            if 'LinkEntity' in str(base.__bases__):
                entity_with_link = base
        #schema zetten op link class
        if entity_with_link:
            link_cls.__dbschema__ = entity_with_link.__dbschema__



        # link class in link_refs zetten
        from pyelt.datalayers.dv import LinkReference
        for key, link_ref in link_cls.__dict__.items():
            if type(link_ref) is LinkReference:
                link_ref.link = link_cls

        # satnames zetten
        new_link_entity_cls.__sats__ = OrderedDict()
        from pyelt.datalayers.dv import Sat
        for key, sat_cls in new_link_entity_cls.__dict__.items():
            if inspect.isclass(sat_cls) and Sat in sat_cls.__mro__:
                sat_cls.__dbschema__ = entity_with_link.__dbschema__
                sat_name = camelcase_to_underscores(key).replace('_sat', '').replace('sat', '')
                sat_cls.__dbname__ = camelcase_to_underscores(entity_with_link.__name__) + '_sat_' + sat_name
                sat_cls.__dbname__ = sat_cls.__dbname__.replace('_default', '')
                sat_cls.name = sat_cls.__dbname__

        # sat collectie maken
        new_link_entity_cls.__sats__ = OrderedDict()
        for base in reversed(new_link_entity_cls.__mro__):
            for key, sat_cls in base.__dict__.items():
                if inspect.isclass(sat_cls) and Sat in sat_cls.__mro__:
                    new_link_entity_cls.__sats__[sat_cls.name] = sat_cls
        return new_link_entity_cls
#############################


