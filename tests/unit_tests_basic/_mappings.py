from tests._domainmodel import Patient, Traject, Patient_Traject_Link, SubTraject, Handeling, Zorginstelling, Zorgverzekeraar, Locatie, Hulpverlener, PatientHandelingLink
from main import get_root_path
from pyelt.helpers.mappingcreator import MappingWriter
from pyelt.mappings.base import ConstantValue
from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping, SorToLinkMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.files import CsvFile


def init_source_to_sor_mappings():
    mappings = []
    source_file = CsvFile(get_root_path() + '/tests/data/patienten1.csv', delimiter=';')
    source_file.reflect()
    source_file.set_primary_key(['patientnummer'])
    sor_mapping = SourceToSorMapping(source_file, 'patient_hstage', auto_map=True)
    mappings.append(sor_mapping)

    source_file = CsvFile(get_root_path() + '/tests/data/zorgtrajecten1.csv', delimiter=';')
    source_file.reflect()
    source_file.set_primary_key(['patientnummer', 'trajectnummer'])
    sor_mapping = SourceToSorMapping(source_file, 'traject_hstage', auto_map=True)
    mappings.append(sor_mapping)

    source_file = CsvFile(get_root_path() + '/tests/data/zorghandelingen1.csv', delimiter=';')
    source_file.reflect()
    source_file.set_primary_key(['patientnummer', 'datum', 'dbc_zorgactiviteit_code', 'hulpverlener_agb'])
    sor_mapping = SourceToSorMapping(source_file, 'handeling_hstage', auto_map=True)
    mappings.append(sor_mapping)
    # MappingWriter.create_python_code_mappings(source_file, remove_prefix='')
    return mappings


def init_sor_to_dv_mappings(sor):
    mappings = []
    mapping = SorToEntityMapping('patient_hstage', Patient, sor)
    mapping.map_field("patientnummer || '_test'", Patient.bk)
    mapping.map_field('achternaam', Patient.Personalia.achternaam)
    mapping.map_field('tussenvoegsels', Patient.Personalia.tussenvoegsels)
    mapping.map_field('voornaam', Patient.Personalia.voornaam)
    mapping.map_field('straat', Patient.Adres.straat)
    mapping.map_field('huisnummer::integer', Patient.Adres.huisnummer)
    mapping.map_field('huisnummertoevoeging', Patient.Adres.huisnummer_toevoeging)
    mapping.map_field('postcode')
    mapping.map_field('LEFT(hstg.postcode, 4)', Patient.Adres.postcode)
    mapping.map_field('plaats', Patient.Adres.plaats)
    mapping.map_field('geslacht')
    mapping.map_field('geboortedatum::date', Patient.Default.geboortedatum)
    mapping.map_field('inschrijvingsnummer', Patient.Inschrijving.inschrijfnummer)
    mapping.map_field('bsn', Patient.Inschrijving.bsn)
    mapping.map_field('inschrijvingsdatum::date', Patient.Inschrijving.inschrijfdatum)
    # hybrid sat mapping
    mapping.map_field('telefoon', Patient.ContactGegevens.telnummer, type=Patient.ContactGegevens.Types.telefoon)
    mapping.map_field('mobiel', Patient.ContactGegevens.telnummer, type=Patient.ContactGegevens.Types.mobiel)
    mappings.append(mapping)

    mapping = SorToEntityMapping('traject_hstage', SubTraject, sor)
    mapping.map_field("patientnummer || '_test' || trajectnummer", SubTraject.bk)
    mapping.map_field('trajectnummer::integer', SubTraject.Default.nummer)
    mapping.map_field('start_datum::timestamp', SubTraject.Default.start)
    mapping.map_field('einde_datum::timestamp', SubTraject.Default.eind)
    mapping.map_field('status', SubTraject.Default.status)
    mappings.append(mapping)

    link_mapping = SorToLinkMapping('traject_hstage', Patient_Traject_Link, sor, type='subtraject')
    link_mapping.map_bk("patientnummer || '_test'", Patient_Traject_Link.Patient)
    link_mapping.map_sor_fk('', Patient_Traject_Link.Traject, type='subtraject')
    mappings.append(link_mapping)

    ######################
    mapping = SorToEntityMapping('handeling_hstage', Handeling, sor)
    mapping.map_bk(['patientnummer', 'datum', 'dbc_zorgactiviteit_code'])
    mapping.map_field("datum::date", Handeling.Default.datum)
    mapping.map_field("dbc_zorgactiviteit_code", Handeling.Default.dbc_activiteit_code)
    mappings.append(mapping)

    mapping = SorToEntityMapping('handeling_hstage', Zorginstelling, sor)
    mapping.map_bk("zorginstelling_agb")
    mapping.map_field("zorginstelling_naam", Zorginstelling.Default.naam)
    # mapping.map_field("zorginstelling_naam", Handeling.Default.dbc_activiteit_code)
    mappings.append(mapping)

    mapping = SorToEntityMapping('handeling_hstage', Zorgverzekeraar, sor)
    mapping.map_bk("zorgverzekeraar_izovi")
    mapping.map_field("zorgverzekeraar_naam", Zorgverzekeraar.Default.naam)
    mappings.append(mapping)

    mapping = SorToEntityMapping('handeling_hstage', Locatie, sor)
    mapping.map_bk("locatie")
    mappings.append(mapping)

    mapping = SorToEntityMapping('handeling_hstage', Hulpverlener, sor)
    mapping.map_bk("hulpverlener_agb")
    mapping.map_field("hulpverlener_naam", Hulpverlener.Default.naam)
    mappings.append(mapping)

    link_mapping = SorToLinkMapping('handeling_hstage', PatientHandelingLink, sor)
    link_mapping.map_bk("patientnummer || '_test'", PatientHandelingLink.Patient)
    link_mapping.map_entity(PatientHandelingLink.Handeling)
    link_mapping.map_entity(PatientHandelingLink.Dynamic, bk="hulpverlener_agb", type=PatientHandelingLink.Types.hulpverlener)
    # link_mapping.map_bk("hulpverlener_agb", PatientHandelingLink.Dynamic, type=PatientHandelingLink.Types.hulpverlener)
    # link_mapping.map_sor_fk('_fk_hulpverlener_hub', PatientHandelingLink.Dynamic, type=PatientHandelingLink.Types.hulpverlener)
    mappings.append(link_mapping)

    link_mapping = SorToLinkMapping('handeling_hstage', PatientHandelingLink, sor)
    link_mapping.map_bk("patientnummer || '_test'", PatientHandelingLink.Patient)
    link_mapping.map_entity(PatientHandelingLink.Handeling)
    link_mapping.map_sor_fk('fk_locatie_hub', PatientHandelingLink.Dynamic, type=PatientHandelingLink.Types.locatie)
    mappings.append(link_mapping)

    return mappings