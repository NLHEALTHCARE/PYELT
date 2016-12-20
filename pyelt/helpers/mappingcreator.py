from sqlalchemy.engine import reflection

from pyelt.datalayers.database import Table
from pyelt.datalayers.dm import Dim
from pyelt.datalayers.dv import DvEntity, HybridSat


class MappingWriter():
    @staticmethod
    def create_python_code_mappings(sor_table: 'Table', entity: DvEntity) -> None:
        """helper functie om mappings aan te maken in string format"""
        if len(sor_table.columns) == 0: sor_table.reflect()
        s = """def init_sor_{}_to_{}_mappings(sor):\r\n""".format(sor_table.name, entity.__name__.lower())
        s += """    mappings = []\r\n"""
        s += """\r\n    # ###########################\r\n"""
        s += """    # Beschikbare velden {}:\r\n""".format(sor_table.name.upper())
        s += '    # '
        idx = 0
        for col in sor_table.columns:
            source = col.name.lower()
            if source.startswith('_'): continue
            s += """{}, """.format(source)
            idx += 1
            if idx == 10:
                idx = 0
                s += '\r\n'
                s += '    # '
        s += """\r\n\r\n"""
        s += """    mapping = SorToEntityMapping('{0}', {1}, sor)\r\n""".format(sor_table, entity.__name__)
        s += """    mapping.map_bk([''])\r\n""".format(sor_table, entity.__name__)
        for sat_name, sat in entity.cls_get_sats().items():
            sat.cls_init_cols()
            s += """\r\n""".format(sor_table, entity.__name__)
            s += """    #SAT {}.{}\r\n""".format(entity.__name__, sat_name)
            for col in sat.cls_get_columns():
                source = ''
                if col.type != 'text':
                    # cast
                    source = "::{1}""".format(source, col.type)

                if sat.__base__ == HybridSat:
                    first_type = ''
                    if 'Types' in sat.__dict__:
                        for k, name in sat.Types.__dict__.items():
                            if not k.startswith('__'):
                                first_type = k
                                break
                    s += """    mapping.map_field("{0}", {1}.{2}.{3}, type={1}.{2}.Types.{4})\r\n""".format(source, entity.__name__, sat_name, col.name, first_type)
                else:
                    s += """    mapping.map_field("{0}", {1}.{2}.{3})\r\n""".format(source, entity.__name__, sat_name, col.name)

        s += """    mappings.append(mapping)\r\n"""
        s += """    return mappings\r\n"""
        s += """\r\n"""
        print(s)
        return s

    @staticmethod
    def create_python_code_dm_mappings(source_schema, source_name: str, dm_schema, dim: Dim) -> None:
        col_names = ''
        if source_name:
            inspector = reflection.Inspector.from_engine(dm_schema.db.engine)
            cols = inspector.get_columns(source_name, source_schema)
            i = 0
            for col in cols:
                col_names += col['name'] + ', '
                if i> 0 and i % 5 == 0:
                    col_names += '\r\n    # '
                i += 1

        """helper functie om mappings aan te maken in string format"""
        s = """def init_{}_{}_to_{}_mappings():\r\n""".format(source_schema, source_name, dim.cls_get_name())
        s += """\r\n    # ###########################\r\n"""
        s += """    # Beschikbare velden: {}\r\n""".format(col_names)
        s += '    \r\n'
        s += '    source_sql = "SELECT * FROM {}.{}"\r\n'.format(source_schema, source_name)
        s += '    mapping = DvToDimMapping(source_sql, {})\r\n'.format(dim.__name__)
        dim.cls_init_cols()
        for col_name in dim.cls_get_column_names():
            s += """    mapping.map_field('', {}.{})\r\n""".format(dim.__name__, col_name)
        s += """    return mapping\r\n"""
        s += """\r\n"""
        print(s)
        return s

    @staticmethod
    def create_python_code_mappings_old(table: 'Table', remove_prefix: str = '') -> None:
        """helper functie om mappings aan te maken in string format"""
        if len(table.columns) == 0: table.reflect()
        s = """mapping = SorToEntityMapping('{0}_hstage', {0}_entity)\r\n""".format(table.name)
        for col in table.columns:
            source = col.name.lower()
            target = col.name.lower().replace(remove_prefix, '')
            type = MappingWriter.oracle_type_to_postgres_type(col.type)
            if type != 'text':
                #cast
                source = "{0}::{1}""".format(source, type)
            s += """mapping.map_field("{}", sat_prop)\r\n""".format(source)
        print(s)

    @staticmethod
    def oracle_type_to_postgres_type(type: str) -> str:
        type = type.lower().strip()
        if 'string' in type:
            return 'text'
        elif 'varchar' in type:
            return 'text'
        elif 'number' in type:
            return 'numeric'
            #todo integers
        elif 'datetime' in type:
            return 'timestamp'
        else:
            return type
