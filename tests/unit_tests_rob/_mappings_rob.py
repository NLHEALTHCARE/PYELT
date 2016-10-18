# from tests._domainmodel import SubPatient, Traject, Patient_Traject_Link
from tests.unit_tests_rob._domain_rob import Zorgverlener, Zorginstelling, Zorgverlener_Zorginstelling_Link, Adres, \
    Zorgverlener_Adres_Link, Zorginstelling_Adres_Link
from tests.unit_tests_rob.global_test_suite import execute_sql
from main import get_root_path
from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping, SorToLinkMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.sources.files import CsvFile


def init_source_to_sor_mappings():
    mappings = []



    source_file = CsvFile(get_root_path() + '/PYELT/tests/data/zorgverleners_rob.csv', delimiter=';')
    source_file.reflect()
    source_file.set_primary_key(['zorgverlenernummer'])
    sor_mapping = SourceToSorMapping(source_file, 'zorgverlener_hstage', auto_map=True)
    mappings.append(sor_mapping)

    source_file = CsvFile(get_root_path() + '/PYELT/tests/data/zorginstelling_rob.csv', delimiter=';')
    source_file.reflect()
    source_file.set_primary_key(['zorginstellings_nummer'])
    sor_mapping = SourceToSorMapping(source_file, 'zorginstelling_hstage', auto_map=True)
    mappings.append(sor_mapping)

    return mappings





def init_sor_to_dv_mappings(pipe):
    mappings = []

    sor = pipe.sor

    mapping = SorToEntityMapping('zorgverlener_hstage', Zorgverlener, sor)
    mapping.map_field("zorgverlenernummer", Zorgverlener.bk)
    mapping.map_field('achternaam', Zorgverlener.Personalia.achternaam)
    mapping.map_field('tussenvoegsels', Zorgverlener.Personalia.tussenvoegsels)
    mapping.map_field('voorletters', Zorgverlener.Personalia.voorletters)
    mapping.map_field('voornaam', Zorgverlener.Personalia.voornaam)
    # mapping.map_field('straat', Zorgverlener.Adres.straat)
    # mapping.map_field('huisnummer::integer', Zorgverlener.Adres.huisnummer)
    # mapping.map_field('huisnummer_toevoeging', Zorgverlener.Adres.huisnummer_toevoeging)
    # mapping.map_field('postcode', Zorgverlener.Adres.postcode)
    # mapping.map_field('plaats', Zorgverlener.Adres.plaats)
    # mapping.map_field('land', Zorgverlener.Adres.land)
    mapping.map_field('zorgverlenernummer', Zorgverlener.Default.zorgverlenernummer)
    mapping.map_field('aanvangsdatum::date', Zorgverlener.Default.aanvangsdatum)
    mapping.map_field('einddatum::date', Zorgverlener.Default.einddatum)

    mapping.map_field('telefoon', Zorgverlener.ContactGegevens.telnummer, type=Zorgverlener.ContactGegevens.Types.telefoon)
    mapping.map_field('telefoon_landcode', Zorgverlener.ContactGegevens.landcode, type=Zorgverlener.ContactGegevens.Types.telefoon)
    mapping.map_field('telefoon_datum::date', Zorgverlener.ContactGegevens.datum, type=Zorgverlener.ContactGegevens.Types.telefoon)
    mapping.map_field('mobiel', Zorgverlener.ContactGegevens.telnummer, type=Zorgverlener.ContactGegevens.Types.mobiel)
    mapping.map_field('mobiel_landcode', Zorgverlener.ContactGegevens.landcode, type=Zorgverlener.ContactGegevens.Types.mobiel)
    mapping.map_field('mobiel_datum::date', Zorgverlener.ContactGegevens.datum, type=Zorgverlener.ContactGegevens.Types.mobiel)
    mapping.map_field('mobiel2', Zorgverlener.ContactGegevens.telnummer, type=Zorgverlener.ContactGegevens.Types.mobiel2)
    mapping.map_field('mobiel2_landcode', Zorgverlener.ContactGegevens.landcode, type=Zorgverlener.ContactGegevens.Types.mobiel2)
    mapping.map_field('mobiel2_datum::date', Zorgverlener.ContactGegevens.datum, type=Zorgverlener.ContactGegevens.Types.mobiel2)
    mappings.append(mapping)


    mapping = SorToEntityMapping('zorgverlener_hstage', Adres, sor)
    mapping.map_field("postcode||huisnummer||COALESCE(huisnummer_toevoeging,'')", Adres.bk)
    mapping.map_field('straat', Adres.Default.straat)
    mapping.map_field('huisnummer::integer', Adres.Default.huisnummer)
    mapping.map_field('huisnummer_toevoeging',Adres.Default.huisnummer_toevoeging)
    mapping.map_field('postcode', Adres.Default.postcode)
    mapping.map_field('plaats', Adres.Default.plaats)
    mapping.map_field('land', Adres.Default.land)
    mappings.append(mapping)

    mapping = SorToEntityMapping('zorginstelling_hstage', Zorginstelling, sor)
    mapping.map_field("zorginstellings_nummer", Zorginstelling.bk)
    mapping.map_field('zorginstellings_naam', Zorginstelling.Default.zorginstellings_naam)
    # mapping.map_field('straat', Zorginstelling.Adres.straat)
    # mapping.map_field('huisnummer::integer', Zorginstelling.Adres.huisnummer)
    # mapping.map_field('huisnummer_toevoeging', Zorginstelling.Adres.huisnummer_toevoeging)
    # mapping.map_field('postcode', Zorginstelling.Adres.postcode)
    # mapping.map_field('plaats', Zorginstelling.Adres.plaats)
    # mapping.map_field('land', Zorginstelling.Adres.land)
    mappings.append(mapping)

    mapping = SorToEntityMapping('zorginstelling_hstage', Adres, sor)
    mapping.map_field("postcode||huisnummer||COALESCE(huisnummer_toevoeging,'')", Adres.bk)
    mapping.map_field('straat', Adres.Default.straat)
    mapping.map_field('huisnummer::integer', Adres.Default.huisnummer)
    mapping.map_field('huisnummer_toevoeging',Adres.Default.huisnummer_toevoeging)
    mapping.map_field('postcode', Adres.Default.postcode)
    mapping.map_field('plaats', Adres.Default.plaats)
    mapping.map_field('land', Adres.Default.land)
    mappings.append(mapping)


    link_mapping = SorToLinkMapping('zorgverlener_hstage', Zorgverlener_Zorginstelling_Link, sor)
    link_mapping.map_entity(Zorgverlener_Zorginstelling_Link.zorgverlener)
    link_mapping.map_entity(Zorgverlener_Zorginstelling_Link.huisarts)
    link_mapping.map_entity(Zorgverlener_Zorginstelling_Link.fysio)


    link_mapping.map_bk("zorginstellings_nummer", Zorgverlener_Zorginstelling_Link.zorginstelling)
    mappings.append(link_mapping)

    link_mapping = SorToLinkMapping('zorgverlener_hstage', Zorgverlener_Adres_Link, sor)
    link_mapping.map_entity(Zorgverlener_Adres_Link.zorgverlener)
    link_mapping.map_entity((Zorgverlener_Adres_Link.adres))
    mappings.append(link_mapping)

    link_mapping = SorToLinkMapping('zorginstelling_hstage', Zorginstelling_Adres_Link, sor)
    link_mapping.map_entity(Zorginstelling_Adres_Link.zorginstelling)
    link_mapping.map_entity((Zorginstelling_Adres_Link.adres))
    mappings.append(link_mapping)


    return mappings

    def get_row_count(unittest, table_name, count):
        test_sql = "SELECT * FROM " + table_name
        result = execute_sql(test_sql)
        unittest.assertEqual(len(result), count, table_name)