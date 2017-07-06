import inspect
from typing import List

from pyelt.datalayers.database import Columns, Table, DbFunction, Column
from pyelt.datalayers.dv import * #HubEntity, Sat, HybridSat, Link, LinkReference, DynamicLinkReference, OrderedMembersMetaClass, HybridLink, DvValueset
from pyelt.datalayers.sor import SorTable, SorQuery
from pyelt.datalayers.valset import DvValueset
from pyelt.mappings.base import BaseTableMapping, ConstantValue
from pyelt.mappings.sor_to_dv_mappings import SorToValueSetMapping, SorToEntityMapping, SorToLinkMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.databases import SourceQuery


class DomainValidator:
    wrong_field_name_chars = [' ', '*', '(', ')', '!', '@', '|', '{', '}', "'", '"', ':', ';', '#', '$', '%', '^', '&']
    """Helper om domein classes te valideren (entities, hubs, sats en links). Wordt aangeroepen voordat run start in pipeline.pipe"""
    def validate(self, module):
        validation_msg = ''
        for name, cls in inspect.getmembers(module,  inspect.isclass):
            if HubEntity in cls.__mro__ and cls is not HubEntity:
                # cls.cls_init()
                validation_msg += self.validate_entity(cls)
            elif LinkEntity in cls.__mro__ and cls is not LinkEntity:
                validation_msg += self.validate_linkentity(cls)
            # elif Link in cls.__mro__ and cls is not Link and cls is not HybridLink:
            #     validation_msg += self.validate_linkentity(cls)
            elif DvValueset in cls.__mro__ and cls is not DvValueset:
                validation_msg += self.validate_valueset(cls)
        return validation_msg

    def validate_entity(self, entity_cls):
        validation_msg = ''
        sat_classes = entity_cls.cls_get_sats()
        for sat_cls in sat_classes.values():
            columns = sat_cls.cls_get_columns()
            if not columns:
                validation_msg += 'Domainclass <red>{}.{}</> is niet geldig. Sat <red>{}</> zonder velden.\r\n'.format(entity_cls.__module__, entity_cls.__name__, sat_cls.cls_get_name())
            for col in columns:
                if col.name[0].isnumeric():
                    validation_msg += 'Domainclass <red>{}.{}.{}</> is niet geldig. Columnname <red>{}</> is ongeldig.\r\n'.format(entity_cls.__module__, entity_cls.__name__, sat_cls.cls_get_name(),
                                                                                                                                   col.name)

                for char in DomainValidator.wrong_field_name_chars:
                    if char in col.name:
                        validation_msg += 'Domainclass <red>{}.{}.{}</> is niet geldig. Columnname <red>{}</> is ongeldig.\r\n'.format(entity_cls.__module__, entity_cls.__name__, sat_cls.cls_get_name(),
                                                                                                                                       col.name)

                if isinstance(col, Columns.RefColumn):
                    if not col.valueset_name:
                        validation_msg += 'Domainclass <red>{}.{}.{}</> is niet geldig. RefColumn <red>{}</> zonder type.\r\n'.format(entity_cls.__module__, entity_cls.__name__, sat_cls.cls_get_name(),
                                                                                                                                      col.name)
            if sat_cls.__base__ == HybridSat:
                if not 'Types' in sat_cls.__dict__:
                    validation_msg += 'Hybrid Sat <red>{}.{}.{}</> is niet geldig. Definieer in de HybridSat een inner-class Types(HybridSat.Types) met een aantal string constanten. Dit is nodig om de databse view aan te maken.\r\n'.format(
                        entity_cls.__module__, entity_cls.__name__, sat_cls.cls_get_name())
                elif sat_cls.__dict__['Types'] and not sat_cls.__dict__['Types'].__base__ is HybridSat.Types:
                    validation_msg += 'Hybrid Sat <red>{}.{}.{}</> is niet geldig. Inner-class Types moet erven van HybridSat.Types zoals "class Types(HybridSat.Types):" .\r\n'.format(
                        entity_cls.__module__, entity_cls.__name__, sat_cls.cls_get_name())

        # for sat_cls in entity_cls.__sats__.values():
        #     if sat_cls.__name__.lower() == 'default' and entity_cls.__base__ is not HubEntity:
        #         validation_msg += 'Domainclass <red>{}.{}</> is niet geldig. Sat <red>Default</> mag alleen hangen onder de class die rechtstreeks erft van HubEntity (degene die de hub wordt).\r\n'.format(
        #             entity_cls.__module__, entity_cls.__name__, sat_cls.cls_get_name())


        return validation_msg

    def validate_linkentity(self, link_entity_cls: Link) -> str:
        validation_msg = ''
        if 'Link' not in link_entity_cls.__dict__:
            validation_msg += """LinkEntity <red>{}.{}</> is niet geldig. Een linkentity moet tenminste een subclass Link bevatten
             Een link entity moet er zo uit zien:
        <blue>
        class MedewerkerOrganisatieLinkEntity(LinkEntity):
            class Link(Link):
                medewerker = LinkReference(Medewerker)
                werkgever = LinkReference(Organisatie)"""
            return validation_msg
        else:
            link_cls = link_entity_cls.Link
            link_refs = link_cls.cls_get_link_refs()
            if len(link_refs) < 2:
                validation_msg += """Link <red>{}.{}</> is niet geldig. Een link moet ten minste twee LinkReferences bevatten (klasse variabelen van type LinkReference(HubEntity)).
                Een link moet er zo uit zien:
        <blue>
        class MedewerkerOrganisatieLinkEntity(LinkEntity):
            class Link(Link):
                medewerker = LinkReference(Medewerker)
                werkgever = LinkReference(Organisatie)
            </>""".format(
                    link_cls.__module__, link_cls.__name__)
            for link_ref in link_refs.values():
                if '_hub' not in link_ref.hub.__dbname__:
                    validation_msg += 'Link <red>{}.{}</> is niet geldig. LinkReference <red>{}</> moet verwijzen naar een Hub.\r\n'.format(
                        link_cls.__module__, link_cls.__name__, link_ref.name)
        return validation_msg

    def validate_valueset(self, valueset_cls: DvValueset) -> str:
        validation_msg = ''
        contains_code_field = False
        contains_valueset_name_field = False
        contains_ref_column = False
        for col in valueset_cls.cls_get_columns():
            if isinstance(col, Column):
                if col.name == 'code':
                    contains_code_field = True
                elif col.name == 'valueset_naam':
                    contains_valueset_name_field = True
                if isinstance(col, Columns.RefColumn):
                    contains_ref_column = True
        if not contains_code_field:
            validation_msg += """ValueSet <red>{}.{}</> is niet geldig. Een ValueSet moet ten minste een code veld bevatten.""".format(
                valueset_cls.__module__, valueset_cls.__name__)
        if not contains_code_field:
            validation_msg += """ValueSet <red>{}.{}</> is niet geldig. Een ValueSet moet ten minste een valueset_naam veld bevatten.""".format(
                valueset_cls.__module__, valueset_cls.__name__)
        # if contains_ref_column:
        #     validation_msg += """ValueSet <red>{}.{}</> is niet geldig. Een ValueSet mag geen refcolumn bevatten.""".format(
        #         valueset_cls.__module__, valueset_cls.__name__)
        return validation_msg


class MappingsValidator:
    """Helper om domein classes te valideren (entities, hubs, sats en links). Wordt aangeroepen voordat run start in pipeline.pipe"""

    def validate_before_sor_ddl(self, mappings_list: List[BaseTableMapping]) -> str:
        validation_msg = ''
        for mappings in mappings_list:
            if isinstance(mappings, SourceToSorMapping):
                validation_msg += self.validate_source_to_sor_mappings_before_ddl(mappings)

        # mapping_target_tables = [map.target for map in mappings_list]
        # if len(mapping_target_tables) != len(set(mapping_target_tables)):
        #     validation_msg += 'SorMappings zijn niet geldig. Er wordt meer keer gemapt op dezelfde doel tabel.\r\n'.format()

        return validation_msg

    def validate_after_ddl(self, mappings_list: List[BaseTableMapping]) -> str:
        validation_msg = ''
        for mappings in mappings_list:
            if isinstance(mappings, SourceToSorMapping):
                validation_msg += self.validate_source_to_sor_mappings_after_ddl(mappings)
            elif isinstance(mappings, SorToValueSetMapping):
                validation_msg += self.validate_sor_to_ref_mappings(mappings)
            elif isinstance(mappings, SorToEntityMapping):
                validation_msg += self.validate_sor_to_entity_mappings(mappings)
            elif isinstance(mappings, SorToLinkMapping):
                validation_msg += self.validate_sor_to_link_mappings(mappings)

        return validation_msg

    def validate_source_to_sor_mappings_before_ddl(self, mappings):
        validation_msg = ''
        source = mappings.source
        if not source.key_names:
            validation_msg += 'Mapping <red>{}</> is niet geldig. Geen geldige key opgegeven. Voorbeeld: source_file.set_primary_key([])\r\n'.format(mappings.name)
        for key_name in source.key_names:
            if not key_name:
                validation_msg += 'Mapping <red>{}</> is niet geldig. Geen geldige key opgegeven. Voorbeeld: source_file.set_primary_key([])\r\n'.format(mappings.name)

        col_names = ''
        for field_map in mappings.field_mappings:
            col_names += """{}""".format(field_map.target)
        if not col_names:
            validation_msg += 'Mapping <red>{}</> is niet geldig. Geen geldige kolommen opgegeven. TIP: gebruik auto_map=True.\r\n'.format(mappings.name)

        for col in source.columns:
            if col.name.startswith('?'):
                validation_msg += 'Mapping <red>{}</> is niet geldig. Kolom is niet geldig. Controleer of alle velden een geldige alias hebben in de SourceQuery("""{}""")\r\n'.format(mappings.name,
                                                                                                                                                                                       source.sql)

        source_col_names = [col.name for col in mappings.source.columns]
        for ignore_col_name in mappings.ignore_fields:
            if ignore_col_name not in source_col_names:
                validation_msg += 'Mapping <red>{}</> is niet geldig. Ignore_field  <red>{}</> komt niet voor in bron.\r\n'.format(mappings.name, ignore_col_name)

        for key_name in mappings.source.key_names:
            if key_name not in source_col_names:
                validation_msg += 'Mapping <red>{}</> is niet geldig. Key <red>{}</> komt niet voor in bron.\r\n'.format(key_name, key_name)


        return validation_msg

    def validate_source_to_sor_mappings_after_ddl(self, mappings):
        validation_msg = ''
        if not mappings.auto_map:
            validation_msg += 'Mapping <red>{}</> is niet geldig. Auto_map moet aan staan bij SourceToSorMapping.\r\n'.format(mappings.name)

        source_col_names = [col.name for col in mappings.source.columns]
        if not mappings.field_mappings:
            validation_msg += 'Mapping <red>{}</> is niet geldig. Geen velden gemapt. Gebruik auto_map = True of map_field() om velden te mappen.\r\n'.format(mappings.name)
        for fld_mapping in mappings.field_mappings:
            if isinstance(fld_mapping.source, str) and fld_mapping.source.name not in source_col_names:
                validation_msg += 'Mapping <red>{}</> is niet geldig. Veld <red>{}</> komt niet voor in bron. TIP: gebruik haakjes () voor berekeningen of om constanten te definieren als bronvelden. Je kunt ook ConstantValue() gebruiken (zie bron header voor geldige veldnamen).\r\n'.format(
                    mappings.name,
                    fld_mapping.source.name)

        fld_mappings_names = [fld_mapping.name for fld_mapping in mappings.field_mappings]
        if len(fld_mappings_names) != len(set(fld_mappings_names)):
            validation_msg += 'Mapping <red>{}</> is niet geldig. Mappings zijn niet uniek.\r\n'.format(mappings.name)

        keys = mappings.keys
        if not keys:
            validation_msg += 'Mapping <red>{}</> is niet geldig. Geen primary key gemapt.\r\n'.format(mappings.name)
        else:
            invalid_key_names = ''
            for key in keys:
                if key not in source_col_names:
                    invalid_key_names += key + ','
            if invalid_key_names:
                validation_msg += 'Mapping <red>{}</> is niet geldig. Key: [<red>{}</>] komt niet voor in bron. (zie bron header voor geldige veldnamen).\r\n'.format(mappings.name, invalid_key_names)
        if len(keys) != len(set(keys)):
            validation_msg += 'Mapping <red>{}</> is niet geldig. Key-velden niet uniek.\r\n'.format(mappings.name)

        fixed_field_names = ['_id', '_runid', '_active', '_source_system', '_insert_date', '_finish_date', '_revision', '_valid', '_validation_msg', '_hash']
        for fixed_field_name in fixed_field_names:
            if fixed_field_name in source_col_names:
                validation_msg += 'Mapping <red>{}</>, veld <red>{}</> is niet geldig. Mag de volgende veldnamen niet gebruiken: {}.\r\n'.format(mappings.name, fixed_field_name, fixed_field_names)

        # if isinstance(mappings.source, SourceQuery) and mappings.filter:
        #     validation_msg += 'Mapping <red>{}</> is niet geldig. Mag geen filter opgeven bij SouryQuery(). Gebruik een where clause.\r\n'.format(mappings.name)
        if isinstance(mappings.source, SourceQuery) and mappings.ignore_fields:
            validation_msg += 'Mapping <red>{}</> is niet geldig. Mag geen ignore_fields opgeven bij SouryQuery. Sluit de velden uit in de sql.\r\n'.format(mappings.name)

        return validation_msg

    def validate_sor_to_entity_mappings(self, mappings):
        validation_msg = ''
        if isinstance(mappings.source, SorTable) and not (mappings.bk_mapping or mappings.key_mappings):
            validation_msg += 'Mapping <red>{}</> is niet geldig. Bk mapping of external_key_mapping ontbreekt. Gebruik map_bk(str of list(str) of mapfield(str of list(str), bk)\r\n'.format(
                mappings.name)



        sor_table = mappings.source
        if not sor_table.is_reflected:
            sor_table.reflect()
        source_col_names = [col.name for col in sor_table.columns]
        if isinstance(mappings.source, SorQuery):
            # controleer of tenminste de vaste velden aanwezig zijn
            if '_runid' not in source_col_names or '_active' not in source_col_names or '_valid' not in source_col_names:
                validation_msg += 'Mapping <red>{}</> is niet geldig. Noodzakelijke vaste velden (_runid, _active en _valid) ontbreken in query. Ook fk naar hub moet reeds aanwezig zijn in hstage tabel. Je moet [tabel]_hstage.* gebruiken voor de sor-tabel die de fk naar de hub bevat. \r\n'.format(
                    mappings.name)
            # if not '.*' in mappings.source.sql:
            #     validation_msg += 'Mapping <red>{}</> is niet geldig. Geldige fk naar hub ontbreekt in sql. Je moet [tabel]_hstage.* gebruiken voor de sor-tabel die de fk naar de hub bevat. Deze moet reeds eerder zijn gemapt op de hub als SorTable().\r\n'.format(
            #         mappings.name)
        for sat_mappings in mappings.sat_mappings.values():
            fld_mappings_names = [fld_mapping.name for fld_mapping in sat_mappings.field_mappings]
            # testen op uniek
            if len(fld_mappings_names) != len(set(fld_mappings_names)):
                validation_msg += 'Mapping <red>{}</> is niet geldig. Mappings zijn niet uniek.\r\n'.format(sat_mappings.name)
            fld_mappings_target_names = [fld_mapping.target.name for fld_mapping in sat_mappings.field_mappings if fld_mapping.target.type != 'jsonb']
            if len(fld_mappings_target_names) != len(set(fld_mappings_target_names)):
                validation_msg += 'Mapping <red>{}</> is niet geldig. Doelvelden zijn niet uniek.\r\n'.format(sat_mappings.name)
            # testen of doel veld bestaat
            for fld_mapping in sat_mappings.field_mappings:
                if isinstance(fld_mapping.source, ConstantValue):
                    continue

                if not isinstance(fld_mapping.source, Column):
                    # overslaan
                    continue

                source_col_name = fld_mapping.source.name
                if '(' in source_col_name:
                    # sql functies overslaan
                    continue


                source_col_name = source_col_name.split('::')[0]
                if source_col_name not in source_col_names:
                    validation_msg += 'Mapping <red>{}</> is niet geldig. Veld <red>{}</> komt niet voor in bron. (zie bron header voor geldige veldnamen).\r\n'.format(mappings.name,
                                                                                                                                                                        source_col_name)


            if sat_mappings.target.__base__ == HybridSat:
                if not sat_mappings.type:
                    validation_msg += 'Mapping <red>{}</> is niet geldig. Bij mappings naar Hybdrid sats moet elke veldmapping van een type worden voorzien bijv. (type = mobiel)\r\n'.format(
                        mappings.name)
        return validation_msg

    def validate_sor_to_ref_mappings(self, mappings):
        validation_msg = ''

        # if not isinstance(mappings.source, dict) and not mappings.source_descr_field:
        #     validation_msg += 'Mapping <red>{}</> is niet geldig. Omschrijvingsveld ontbreekt.\r\n'.format(
        #                 mappings.name)

        return validation_msg

    def validate_sor_to_link_mappings(self, mappings):
        validation_msg = ''
        if len(mappings.field_mappings) < 2:
            validation_msg += 'Linkmapping <red>{}</> is niet geldig. Moet minstens naar 2 velden verwijzen.\r\n'.format(mappings.name)

        num_dynamic_refs = 0
        for fld_mapping in mappings.field_mappings:
            if isinstance(fld_mapping.target, DynamicLinkReference):
                num_dynamic_refs += 1
        if num_dynamic_refs > 1:
            validation_msg += 'Linkmapping <red>{}</> is niet geldig. Link mag maar 1 DynamicLinkReference veld bevatten.\r\n'.format(mappings.name)

        return validation_msg

        # todo: betere foutmeldingen
        # todo: controle of sat compleet is gemapt
