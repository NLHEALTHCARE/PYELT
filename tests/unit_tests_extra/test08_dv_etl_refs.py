import unittest

from tests.unit_tests_extra import _domeinmodel
from tests.unit_tests_extra._configs import test_system_config
from tests.unit_tests_extra._globals import *
from tests.unit_tests_extra._domeinmodel import *
from pyelt.datalayers.database import Table
from pyelt.helpers.mappingcreator import MappingWriter
from pyelt.mappings.base import ConstantValue
from pyelt.mappings.sor_to_dv_mappings import SorToValueSetMapping, SorToEntityMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.mappings.transformations import FieldTransformation
from pyelt.sources.databases import SourceQuery, SourceTable



# Don revision  start bij 0,
# Done auto cast van types anders dan text

source_db_sql = [
    """DELETE FROM handelingen;""",

    """INSERT INTO handelingen(
            id, naam, soort, datum, kosten, fk_org, fk_spec)
    VALUES (1, 'ok1', 's01', '2016-8-18 10:25:00', 123.99, 1, 1),
    (2, 'ok2', 's02', '2016-8-18 10:26:00', 500, 1, 1)""",

    """UPDATE handelingen SET naam = 'ok1a' where id = 1""",
]


def init_source_to_sor_mappings(pipe):
    mappings = []
    validations = []
    source_db = pipe.source_db

    source_qry = SourceQuery(source_db, """
SELECT
  *
FROM
  public.handelingen
""", 'view_2')
    source_qry.set_primary_key(['id'])
    sor_mapping = SourceToSorMapping(source_qry, 'handeling_hstage', auto_map=True)
    mappings.append(sor_mapping)

    source_tbl = SourceTable('handelingen', source_db.default_schema, source_db)
    source_tbl.set_primary_key(['id'])
    sor_mapping = SourceToSorMapping(source_tbl, 'handeling2_hstage', auto_map=True, ignore_fields=['fk_org'])
    mappings.append(sor_mapping)
    return mappings


soorten_dict = {
    's01': 'soort1',
    's02': 'soort2',
    's03': 'soort3',
}


def lookup():
    pass


def init_ref_mappings():
    mappings = []

    # ref_mapping = SorToValueSetMapping({'soort1': 'Soort omschrijving 1', 'soort2': 'Soort omschrijving 2', 'soort 3': 'Soort omschrijving 3'}, 'handeling_soorten')
    # mappings.append(ref_mapping)
    return mappings


def init_sor_to_dv_mappings(pipe):
    mappings = []

    mapping = SorToEntityMapping('handeling_hstage', Handeling, pipe.sor)
    mapping.map_bk('id')
    mapping.map_field("naam", Handeling.Default.naam)
    mapping.map_field("datum::date", Handeling.Default.datum)
    t = FieldTransformation()
    t.new_step("select target from a where source = '{fld}'")
    mapping.map_field("soort", Handeling.Default.soort, transform_func=t)
    # mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
    mappings.append(mapping)
    return mappings


#
#     mapping = SorToEntityMapping('handeling_hstage', Organisatie, pipe.sor)
#     mapping.map_bk('fk_org')
#     mapping.map_field("org", Organisatie.Default.naam)
#     mappings.append(mapping)
#
#     # source_sql = "SELECT een.id, een.id as id2, twee.naam as twee_naam FROM sor_extra.handeling_hstage een JOIN sor_extra.handeling2_hstage twee ON een.id = twee.id"
#     # mapping = SorToEntityMapping(source_sql, Handeling, pipe.sor)
#     # mapping.map_field("kosten::numeric", Handeling.Financieel.kostprijs)
#     # mappings.append(mapping)
#     return mappings


class TestCase_RunDv(unittest.TestCase):
    is_init = False
    """Testen met databse als bron"""

    def setUp(self):
        if not TestCase_RunDv.is_init:
            print('init_db')
            init_db()
            TestCase_RunDv.is_init = True
        self.pipeline = get_global_test_pipeline()
        self.pipe = self.pipeline.get_or_create_pipe('test_system', config=test_system_config)
        self.pipe.register_domain(_domeinmodel)
        self.pipe.mappings = []
        self.pipe.mappings.extend(init_source_to_sor_mappings(self.pipe))
        # self.pipe.mappings.extend(init_ref_mappings())
        self.pipe.mappings.extend(init_sor_to_dv_mappings(self.pipe))

    # def test00(self):
    #     self.pipeline.run()
    #     tbl = Table('handeling_hstage', self.pipe.sor)
    #     s = MappingWriter.create_python_code_mappings(tbl, Handeling)
    #     print(s)

    #
    def test_run1(self):
        print('======================================================')
        print('===        R U N  1                                ===')
        print('======================================================')
        # # maak tabellen in bron leeg
        exec_sql(source_db_sql[0], test_system_config['source_connection'])
        # # vul bron tabellen met initiele waarden
        exec_sql(source_db_sql[1], test_system_config['source_connection'])

        self.pipeline.run()


if __name__ == '__main__':
    unittest.main()
