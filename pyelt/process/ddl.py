from pyelt.datalayers.dm import *
from pyelt.datalayers.dv import *
from pyelt.datalayers.sor import *
from pyelt.datalayers.valset import *
from pyelt.helpers.pyelt_logging import Logger, LoggerTypes
from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping, SorToLinkMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.process.base import BaseProcess


class Ddl(BaseProcess):
    def __init__(self, owner: 'Pipe', layer: 'Schema') -> None:
        super().__init__(owner)
        self.layer = layer
        self.sql_logger = None  # Type: Logger
        self.is_initialised = False  # Type: bool
        # self.old_version_number = 0.0  # Type: float
        # self.new_version_number = 1.0  # Type: float

    def init(self) -> None:
        self.init_logger()
        self.increase_version()
        self.is_initialised = True

    def increase_version(self):
        layer_name = self.layer.name
        if layer_name == 'sys':
            #wijzigingen in sys zelf niet meenemen, want daarvoor heb je immers sys nodig
            return
        dwh = self.pipeline.dwh
        old_version_number = dwh.get_layer_version(layer_name)  # Type: float
        new_version_number = dwh.increase_layer_version(layer_name)
        self.sql_logger.log_simple("""
        --DDL aanpassing aan {}: {} --> {}
        --===========================
        """.format(layer_name, old_version_number, new_version_number))

    def init_logger(self) -> None:
        self.sql_logger = Logger.create_logger(LoggerTypes.DDL, self.pipeline.runid, self.pipeline.config, to_console=False)

    def __log_sql(self, log_message, sql, rowcount = 0):
        msg = """{}

{}

rows: {}
--------------------------
""".format(log_message, sql, rowcount)
        self.sql_logger.log_simple(msg)

    def execute(self, sql: str, log_message: str = '') -> None:
        if not self.is_initialised:
            self.init()
        while '  ' in sql:
            sql = sql.replace('\t', ' ').replace('  ', ' ')
        sql = sql.replace('\n ', '\n').replace('\n', '\n    ')
        self.sql_logger.log_simple(sql + '\r\n')
        try:
            rowcount = self.dwh.execute(sql, log_message)
            if log_message and self.logger:
                self.logger.log(log_message, rowcount=rowcount, indent_level=4)
                self.__log_sql(log_message, sql, rowcount)
            self.layer.is_reflected = False
        except Exception as err:
            if self.logger:
                self.logger.log_error(log_message, sql, err.args[0])
                # even wachten, want anders raakt error-msg verward met sql
            time.sleep(0.1)
            # raise Exception(err)

    def confirm_execute(self, sql: str, log_message: str) -> None:
        if not self.is_initialised:
            self.init()
        self.sql_logger.log_simple(sql + '\r\n')
        try:
            self.dwh.confirm_execute(sql, log_message)
            self.logger.log(log_message, indent_level=4)
            self.__log_sql(log_message, sql)
            self.layer.is_reflected = False
        except Exception as err:
            self.logger.log_error(log_message, sql, err.args[0])
            # even wachten, want anders raakt error-msg verward met sql
            time.sleep(0.1)
            raise Exception(err)

    def create_or_alter_table(self, table_cls: AbstractOrderderTable):
        schema = table_cls.cls_get_schema(self.dwh)
        if not schema.is_reflected:
            schema.reflect()
        table_name = table_cls.cls_get_name()

        params = {}
        params.update(self._get_fixed_params())
        params['table_name'] = table_name
        params['schema'] = schema.name
        params['columns_def'] = self.__get_columns_def(table_cls)
        params['constraints'] = ''
        constraints = self.__get_constraints(table_cls)
        for constraint_sql in constraints.values():
            params['constraints'] += constraint_sql + ',\r\n'
        params['constraints'] = params['constraints'][:-3]
        params['indexes'] = ''
        indexes = self.__get_indexes(table_cls)
        for index_sql in indexes.values():
            params['indexes'] += index_sql + ';\r\n'
        params['indexes'] = params['indexes'][:-3]
        if not table_name in schema:
            if params['constraints']:
                sql = """CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                  {columns_def},
                  {constraints}
                ) WITH (OIDS=FALSE, autovacuum_enabled=true);

                {indexes}""".format(**params)
            else:
                sql = """CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                  {columns_def}
                 ) WITH (OIDS=FALSE, autovacuum_enabled=true);

                 {indexes}""".format(**params)
            self.execute(sql, 'create <blue>{}</>'.format(table_name))
        else:
            #ALTER
            db_tbl = Table(table_name, schema=schema)
            db_tbl.reflect()
            add_fields = ''
            for col in table_cls.__cols__:
                if not col.name in db_tbl:
                    add_fields += 'ADD COLUMN {} {}, '.format(col.name, col.type)
            for constraint_name, constraint_sql in constraints.items():
                if not constraint_name in db_tbl:
                    add_fields += 'ADD {}, '.format(constraint_sql)

            add_fields = add_fields[:-2]
            if add_fields:
                params['add_fields'] = add_fields
                sql = """ALTER TABLE {schema}.{table_name} {add_fields}; """.format(**params)
                self.execute(sql, 'alter <cyan>{}</> '.format(params['table_name']))
            add_indexes = ''
            for index_name, index_sql in indexes.items():
                if not index_name in db_tbl:
                    add_indexes += '{};\r\n '.format(index_sql)
            if add_indexes:
                self.execute(add_indexes, 'alter <cyan>{}</> add indexes '.format(params['table_name']))

    def __get_columns_def(self, table_cls):
        sql = ''
        for col in table_cls.cls_get_columns():
            sql += """{} {}""".format(col.name, col.type)
            if not col.nullable:
                sql += ' NOT NULL '
            if col.default_value:
                sql += " DEFAULT {}".format(col.default_value)
            sql += ',\r\n'

        sql = sql[:-3]
        return sql

    def __get_constraints(self, table_cls: AbstractOrderderTable) -> str:
        constraints = {}
        pk_fields = ''
        unique_fields = ''

        for col in table_cls.cls_get_columns():
            if col.is_key:
                pk_fields += "{},".format(col.name)
            if col.is_unique:
                unique_fields += "{},".format(col.name)
        pk_fields = pk_fields[:-1]
        unique_fields = unique_fields[:-1]
        params = {}
        params['schema'] = table_cls.__dbschema__
        params['table_name'] = table_cls.__dbname__
        params['pk_fields'] = pk_fields
        params['unique_fields'] = unique_fields
        if pk_fields:
            constraint_name = "{table_name}_pk".format(**params)
            params['constraint_name'] = constraint_name
            constraints[table_cls.__dbname__ + '_pk'] = "CONSTRAINT {constraint_name} PRIMARY KEY ({pk_fields})".format(**params)
        if unique_fields:
            constraint_name = "{table_name}_unique".format(**params)
            params['constraint_name'] = constraint_name
            constraints[table_cls.__dbname__ + '_unique'] = "CONSTRAINT {constraint_name} UNIQUE ({unique_fields})".format(**params)
        constraints.update(self.__get_fk_constraints(table_cls))
        return constraints

    def __get_fk_constraints(self, table_cls: AbstractOrderderTable) -> str:
        constraints = {}
        for name, ref in table_cls.__ordereddict__.items():
            if isinstance(ref, FkReference):
                params = {}
                params['schema'] = table_cls.__dbschema__
                params['table_name'] = table_cls.__dbname__
                params['ref_table_name'] = ref.ref_table.__dbname__
                params['ref_pk'] = ','.join(ref.ref_table.cls_get_key())
                params['fk'] = ref.fk
                params['fk'] = ref.fk
                constraint_name = "fk_{table_name}_{fk}".format(**params)
                params['constraint_name'] = constraint_name
                constraints[constraint_name] = "CONSTRAINT {constraint_name} FOREIGN KEY ({fk}) REFERENCES {schema}.{ref_table_name} ({ref_pk})".format(**params)
        return constraints

    def __get_indexes(self,table_cls: AbstractOrderderTable):
        indexes = {}
        for col in table_cls.cls_get_columns():
            if col.is_indexed:
                params = {}
                params['schema'] = table_cls.__dbschema__
                params['table_name'] = table_cls.__dbname__
                params['field'] = col.name
                index_name = "ix_{table_name}_{field}".format(**params)
                params['index_name'] = index_name
                indexes[index_name] = "CREATE INDEX {index_name} ON {schema}.{table_name}({field})".format(**params)
        return indexes

    def create_or_alter_functions(self, db_functions: Dict[str, DbFunction]) -> None:
        if not self.layer.is_reflected:
            self.layer.reflect()

        for db_func in db_functions.values():
            complete_name = db_func.get_complete_name()
            if complete_name not in self.layer.functions:
                sql = db_func.to_create_sql()
                self.execute(sql, 'CREATE FUNCTION ' + complete_name)
            # todo: wijzigingen van functies & overloads (functies met dubbele parameters
            elif db_func.return_type != self.layer.functions[complete_name].return_type:
                sql = 'DROP FUNCTION {};'.format(complete_name)
                self.execute(sql, 'DROP FUNCTION ' + complete_name)
                sql = db_func.to_create_sql()
                self.execute(sql, 'CREATE FUNCTION ' + complete_name)
            elif db_func.get_stripped_body() != self.layer.functions[complete_name].get_stripped_body():
                sql = db_func.to_create_sql()
                self.execute(sql, 'ALTER FUNCTION ' + complete_name)

    def create_or_alter_table_exceptions(self, schema):
        if not schema.is_reflected:
            schema.reflect()
        if '_exceptions' not in schema:
            params = {}
            params.update(self._get_fixed_params())
            params['schema'] = schema.name
            sql = """CREATE TABLE IF NOT EXISTS {schema}._exceptions (
                      _id serial NOT NULL,
                      _runid numeric(8,2) NOT NULL,
                      _source_system character varying,
                      _insert_date timestamp without time zone,
                      _valid boolean DEFAULT TRUE,
                      _validation_msg character varying,
                      schema text,
                      table_name text,
                      message text,
                      key_fields text,
                      fields text,
                      CONSTRAINT _exceptions_pkey PRIMARY KEY (_id)
                )
                WITH (
                  OIDS=FALSE,
                  autovacuum_enabled=true
                );""".format(**params)
            self.execute(sql, 'create exception table in ' + schema.name)

class DdlSor(Ddl):
    def __init__(self, pipe: 'Pipe') -> None:
        super().__init__(pipe, pipe.sor)

    def _get_fixed_params(self) -> Dict[str, Any]:
        params = {}
        params['runid'] = self.runid
        params['source_system'] = self.pipe.source_system
        params['sor'] = self.pipe.sor.name
        return params

    #####################################
    # SOR
    #####################################
    def create_or_alter_sor(self, mappings: SourceToSorMapping) -> None:
        sor = self.pipe.sor
        if not sor.is_reflected:
            sor.reflect()

        params = mappings.__dict__
        params.update(self._get_fixed_params())
        params['fixed_columns_def'] = self.__get_fixed_sor_columns_def()
        params['columns_def'] = self.__mappings_to_sor_columns_def(mappings)
        params['key_columns_def'] = self.__mappings_to_sor_key_columns_def(mappings)

        temp_table_name = mappings.temp_table
        if temp_table_name not in sor:
            sql = """CREATE UNLOGGED TABLE IF NOT EXISTS {sor}.{temp_table} (_hash text, _status text, {columns_def});""".format(**params)
            self.execute(sql, 'create <blue>{}_hash</>'.format(temp_table_name))
            sor.is_reflected = False
        else:
            temp_table = sor.tables[temp_table_name]
            if not temp_table.is_reflected:
                temp_table.reflect()
            for field_map in mappings.field_mappings:
                if field_map.target not in temp_table:
                    params['column_def'] = '{} text'.format(field_map.target)
                    sql = """ALTER TABLE {sor}.{temp_table} ADD COLUMN {column_def};""".format(**params)
                    self.execute(sql, 'alter <blue>{}_hash</>'.format(temp_table_name))
                    temp_table.is_reflected = False

        temp_table_hash = temp_table_name + '_hash'
        if temp_table_hash not in sor:
            sql = """CREATE UNLOGGED TABLE IF NOT EXISTS {sor}.{temp_table}_hash ({key_columns_def}, _hash text, _changed boolean);""".format(
                **params)
            self.execute(sql, 'create <blue>{}</>'.format(temp_table_hash))
            sor.is_reflected = False

        sor_table_name = mappings.sor_table
        if sor_table_name not in sor:
            sql = """CREATE TABLE IF NOT EXISTS {sor}.{sor_table} (
                  {fixed_columns_def},
                  {columns_def},
                  CONSTRAINT {sor_table}_pkey PRIMARY KEY (_id)
            )
            WITH (
              OIDS=FALSE,
              autovacuum_enabled=true
            );""".format(**params)
            self.execute(sql, 'create <blue>{}_hash</>'.format(sor_table_name))
            sor.is_reflected = False
        else:
            sor_table = sor.tables[sor_table_name]
            if not sor_table.is_reflected:
                sor_table.reflect()
            for field_map in mappings.field_mappings:
                if field_map.target not in sor_table:
                    params['column_def'] = '{} text'.format(field_map.target)
                    sql = """ALTER TABLE {sor}.{sor_table} ADD COLUMN {column_def};""".format(**params)
                    self.execute(sql, 'alter <blue>{}_hash</>'.format(sor_table_name))
                    sor_table.is_reflected = False

    def __get_fixed_sor_columns_def(self):
        sql = """
        _id serial NOT NULL,
        _runid numeric(8,2) NOT NULL,
        _active boolean DEFAULT true,
        _source_system character varying,
        _insert_date timestamp without time zone,
        _finish_date timestamp without time zone,
        _deleted_runid numeric(8,2),
        _revision integer DEFAULT 0,
        _valid boolean NOT NULL DEFAULT TRUE,
        _validation_msg character varying,
        _hash character varying
        """
        return sql

    def __mappings_to_sor_columns_def(self, mappings):
        sql = ''
        for field_map in mappings.field_mappings:
            sql += """{} text,\r\n""".format(field_map.target)
        sql = sql[:-3]
        return sql

    def __mappings_to_sor_key_columns_def(self, mappings):
        sql = ''
        for col in mappings.source.columns:
            if col.is_key:
                sql += """{} text,\r\n""".format(col.name)
                # bug: als je onderstaande key als type definieert, moet je de betrffende kolom ook in _temp als dat zelfde type defineren
                # voor QnD oplossing ff voor text gekozen
                # sql += """{} {},\r\n""".format(col.name, col.type)
        sql = sql[:-3]
        return sql

    def try_add_fk_sor_hub(self, mapping: SorToEntityMapping) -> None:
        if isinstance(mapping.source, SorTable):
            sor_table = mapping.source.name
        elif isinstance(mapping.source, SorQuery):
            sor_table = mapping.source.get_main_table()
        else:
            return

        sor = self.pipe.sor
        if not sor.is_reflected:
            sor.reflect()

        params = {}
        params.update(self._get_fixed_params())
        params['hub'] = mapping.target.cls_get_hub_name()
        params['sor_table'] = sor_table
        params['type'] = mapping.type

        fk_name = 'fk_{type}{hub}'.format(**params).lower()
        ix_name = 'ix_{sor_table}_fk_{type}{hub}'.format(**params).lower()

        if not fk_name in sor.tables[mapping.source.name]:
            sql = """ALTER TABLE {sor}.{sor_table} ADD COLUMN fk_{type}{hub} integer;""".format(**params)
            self.execute(sql, 'create <blue>' + fk_name)
            sor.is_reflected = False
        if not ix_name in sor.tables[mapping.source.name]:
            sql = """CREATE INDEX ix_{sor_table}_fk_{type}{hub} ON {sor}.{sor_table} USING btree (fk_{type}{hub});""".format(
                **params)
            self.execute(sql, 'create index on <blue>' + fk_name)
            sor.is_reflected = False

    def try_add_fk_sor_link(self, mapping: SorToLinkMapping) -> None:
        link_entity = mapping.target
        if len(link_entity.cls_get_sats()) == 0:
            return

        sor = self.pipe.sor
        if not sor.is_reflected:
            sor.reflect()

        params = {}
        params.update(self._get_fixed_params())
        params['link'] = mapping.target.cls_get_name()
        params['sor_table'] = mapping.source.name
        params['type'] = mapping.type

        fk_name = 'fk_{type}{link}'.format(**params)
        ix_name = 'ix_{sor_table}_fk_{type}{link}'.format(**params)

        if not fk_name in sor.tables[mapping.source.name]:
            sql = """ALTER TABLE {sor}.{sor_table} ADD COLUMN fk_{type}{link} integer;""".format(**params)
            self.execute(sql, 'create ' + fk_name)
            sor.is_reflected = False
        if not ix_name in sor.tables[mapping.source.name]:
            sql = """CREATE INDEX ix_{sor_table}_fk_{type}{link} ON {sor}.{sor_table} USING btree (fk_{type}{link});""".format(
                **params)
            self.execute(sql, 'create index on ' + fk_name)
            sor.is_reflected = False

    def create_or_alter_sor_functions(self, db_function) -> None:
        sor = self.pipe.sor
        if not sor.is_reflected:
            sor.reflect()
            f = sor.functions[0]

        params = db_function.__dict__
        sql = db_function.sql_body.format(**params)
        self.execute(sql, 'create function')
        sor.is_reflected = False



class DdlDv(Ddl):
    def __init__(self, owner: Union['Pipe', 'Pipeline'], dv_schema = None) -> None:
        super().__init__(owner, dv_schema)
        import pyelt.pipeline
        if isinstance(owner,pyelt.pipeline.Pipe):
            self.ddl_sor = DdlSor(owner)

    def create_or_alter_entity(self, entity: HubEntity) -> None:
        if entity == HubEntity:
            #een rechtstreekse HubEntity hoeft niet, alleen afgeleide (overgerfde)
            return
        super().create_or_alter_table(entity.Hub)
        sats = entity.cls_get_sats()
        for sat in sats.values():
            super().create_or_alter_table(sat)
        return

    def create_or_alter_link(self, link_entity: Link) -> None:
        if link_entity == LinkEntity:
            return
        super().create_or_alter_table(link_entity.Link)
        sats = link_entity.cls_get_sats()
        for sat in sats.values():
            super().create_or_alter_table(sat)

    def create_or_alter_view(self, entity_cls: 'HubEntity'):
        schema = entity_cls.cls_get_schema(self.dwh)
        if not schema.is_reflected:
            schema.reflect()
        params = {}
        params.update(self._get_fixed_params())
        params['dv_schema'] = schema.name

        view_name = entity_cls.cls_get_view_name()
        params['view_name'] = view_name
        params['hub'] = entity_cls.cls_get_hub_name()

        if view_name in schema:
            sql = """DROP VIEW {dv_schema}.{view_name};""".format(**params)
            self.execute(sql, 'drop view')

        sql_sat_fields = ''
        sql_join = ''
        sql_join_refs = ''
        index = 1

        all_sats = entity_cls.__sats__

        for sat_cls in all_sats.values():
            params['sat'] = sat_cls.cls_get_name()

            if sat_cls.__base__ == HybridSat:
                for type in sat_cls.cls_get_types():

                    params['sat_alias'] = 'sat' + str(index)
                    params['type'] = type
                    for col in sat_cls.cls_get_columns():
                        if not col.name.startswith('_') and col.name != 'type':
                            alias = sat_cls.cls_get_short_name() + '_' + type + '_' + col.name
                            alias = alias.replace(' ', '_')
                            if col.name == 'type':
                                idx = sat_cls.name.index('_sat') + 5
                                alias = sat_cls.name[idx:] + '_type'
                            sql_sat_fields += "sat{}.{} AS {}, ".format(index, col.name, alias)

                    sql_join += """LEFT OUTER JOIN {dv_schema}.{sat} AS {sat_alias} ON {sat_alias}._id = hub._id AND {sat_alias}._active AND ({sat_alias}.type = '{type}' OR {sat_alias}.type IS NULL)\r\n        """.format(
                        **params)
                    index += 1

            else:
                params['sat_alias'] = 'sat' + str(index)
                for col in sat_cls.cls_get_columns():
                    if not col.name.startswith('_'):
                        alias = sat_cls.cls_get_short_name() + '_' + col.name
                        if col.name == 'type':
                            idx = sat_cls.name.index('_sat') + 5
                            alias = sat_cls.name[idx:] + '_type'
                        sql_sat_fields += "sat{}.{} AS {}, ".format(index, col.name, alias)
                        if isinstance(col, Columns.RefColumn):
                            params['valueset_schema'] = 'valset' #todo valsetschema in generieke var
                            params['valueset_table'] = 'valueset' #todo table name
                            params['alias'] = col.name + '_' + col.valueset_name
                            params['valueset_name'] = col.valueset_name
                            params['code_field'] = col.name
                            sql_join_refs += """LEFT OUTER JOIN {valueset_schema}.{valueset_table} AS {alias} ON {sat_alias}.{code_field}::text = {alias}.code AND {alias}.valueset_naam = '{valueset_name}' AND {alias}._active\r\n        """.format(
                                **params)
                            sql_sat_fields += "{alias}.omschrijving AS {alias}_omschr, ".format(**params)



                            # sql_sat_fields += self.mappings_sat_fields(sat_mapping, params['sat_alias']) + ','
                sql_join += """LEFT OUTER JOIN {dv_schema}.{sat} AS {sat_alias} ON {sat_alias}._id = hub._id AND {sat_alias}._active\r\n        """.format(**params)
                index += 1

                # for field_map in sat_mapping.field_mappings:
                #     if field_map.ref:
                #         params['ref_type'] = field_map.ref
                #         params['ref_field'] = field_map.target.name
                #         sql_join += """LEFT OUTER JOIN {dv}.referenties AS {ref_type} ON {sat_alias}.{ref_field}::text = {ref_type}.code AND {ref_type}.type = '{ref_type}'\r\n        """.format(
                #             **params)

        sql_sat_fields = sql_sat_fields[:-2]
        params['sat_fields'] = sql_sat_fields
        params['join'] = sql_join + sql_join_refs
        params['filter'] = '1=1'
        if entity_cls.__subtype__:
            params['filter'] = "hub.type = '{}'".format(entity_cls.__subtype__)


        if params['sat_fields']:
            sql = """--DROP VIEW {dv_schema}.{view_name};
CREATE OR REPLACE VIEW {dv_schema}.{view_name} AS
    SELECT hub._id, hub.bk, hub.type, hub._runid, hub._source_system, True as _valid,
        {sat_fields}
    FROM {dv_schema}.{hub} hub
        {join}
    WHERE {filter}""".format(**params)

            self.execute(sql, 'create <darkcyan>{}</>'.format(view_name))
        else:
            sql = """CREATE OR REPLACE VIEW {dv_schema}.{view_name} AS
                SELECT hub._id, hub.bk, hub.type,
                    hub._runid as _runid, hub._source_system as hub_source_system, True as _valid
                FROM {dv_schema}.{hub} hub
                    {join}
                WHERE {filter}""".format(**params)
            self.execute(sql, 'create <darkcyan>{}</>'.format(view_name))
        # schema.is_reflected = False

    def create_or_alter_ensemble_view(self, ensemble_cls):

        dv = self.dwh.dv
        dv.reflect()

        # ensemble = TestEnsemble()  #todo: aanpassen want nu nog hardgecodeerd.

        ensemble = ensemble_cls()
        inspector = reflection.Inspector.from_engine(dv.db.engine)

        # aanmaken van sub_strings voor sql:
        sql_ensemblename = 'dv.ensembleview'
        sql_columns = ''
        sql_selectedtables = ''
        sql_conditions = ''
        params = {'schema_name': 'dv'}

        for alias, cls in ensemble.entity_dict.items():
            cls.cls_init()
            cls.entity_name = cls.cls_get_name().strip('_entity')
            cls.view_name = cls.cls_get_name().strip('_entity') + '_view'

            if cls.__base__ == Link:
                sql_selectedtables += ', {schema_name}.'.format(**params) + cls.cls_get_name()
            else:
                if alias == str(cls.cls_get_name()):  # geen alias opgegeven; dus alias is hetzelfde als de entity_name
                    sql_selectedtables += ', {schema_name}.{} as {}'.format(cls.view_name, cls.entity_name,**params)
                    sql_conditions += ' AND {0}._id = fk_{0}_hub'.format(cls.entity_name)
                    sql_ensemblename = sql_ensemblename.replace('.', '.{}_'.format(cls.entity_name)) # hier wordt de opgehaalde entiteit geplaatst direct achter de punt en voor datgene dat eerst na de punt volgde.

                    cols = inspector.cls_get_columns(cls.view_name, dv.name)
                    for col in cols:
                        col_name = col['name']
                        sql_columns += ', {0}.{1} as {0}_{1}'.format(cls.entity_name,col_name)

                else:  # alias voor entity_name is opgegeven
                    sql_selectedtables += ', {schema_name}.{} as {}'.format(cls.view_name,alias,**params)
                    sql_conditions += ' AND {0}._id = fk_{0}_{1}_hub'.format(alias,cls.entity_name)
                    sql_ensemblename = sql_ensemblename.replace('v.', 'v.{}_'.format(alias))

                    cols = inspector.cls_get_columns(cls.view_name, dv.name)
                    for col in cols:
                        col_name = col['name']
                        sql_columns += ', {0}.{1} as {0}_{1}'.format(alias,col_name)

        sql_columns = sql_columns[2:]  # verwijder de "' " van het begin van de string
        sql_selectedtables = sql_selectedtables[2:]
        sql_conditions = sql_conditions[4:]

        if ensemble.name:  # als ensemble.name niet een lege string is dan wordt de aangemaakte string sql_ensemblename gebaseerd op de gebruikte entiteiten vervangen door de vooraf opgegeven alternatieve naam (bv 'test_view')
             sql_ensemblename = '{schema_name}.'.format(**params) + ensemble.name

        params.update ({'ensemble': sql_ensemblename, 'columns': sql_columns, 'selected tables': sql_selectedtables, 'conditions': sql_conditions})
        sql = """CREATE OR REPLACE VIEW {ensemble} AS SELECT {columns} FROM {selected tables} WHERE {conditions}; ALTER TABLE {ensemble} OWNER TO postgres;""".format(**params)

        self.execute(sql, '<darkcyan>{}</>'.format(sql_ensemblename))


    def create_or_alter_table_valueset(self, dv_schema):
        #maak reftable

        if not dv_schema.is_reflected:
            dv_schema.reflect()
        if 'valueset' not in dv_schema:
            params = {}
            params.update(self._get_fixed_params())
            params['dv_schema'] = dv_schema.name
            params['fixed_columns_def'] = self.__get_fixed_sat_columns_def()
            sql = """CREATE TABLE IF NOT EXISTS {dv_schema}.valueset (
                      {fixed_columns_def},
                      naam text,
                      oid text,
                      datum_van text,
                      datum_tot text,
                      status text,
                      versie text,
                      projecten text,
                      uri text,
                      CONSTRAINT valueset_pkey PRIMARY KEY (_id),
                      CONSTRAINT valueset_oid_unique UNIQUE (oid)
                      --CONSTRAINT ref_valueset_naam_unique UNIQUE (naam)
                )
                WITH (
                  OIDS=FALSE,
                  autovacuum_enabled=true
                );


                CREATE TABLE IF NOT EXISTS {dv_schema}.valueset_code (
                      {fixed_columns_def},
                      valueset_naam text,
                      code text,
                      weergave_naam text,
                      omschrijving text,
                      omschrijving2 text,
                      --valueset_oid text,
                      niveau text,
                      fk_valueset int,
                      fk_parent int,
                      --niveau_type text,
                      --code_hl7 text,

                      CONSTRAINT valueset_code_pkey PRIMARY KEY (_id),
                      CONSTRAINT valueset_code_code_unique UNIQUE (_runid, valueset_naam, code, niveau)
                )
                WITH (
                  OIDS=FALSE,
                  autovacuum_enabled=true
                );
                CREATE INDEX valueset_code_valueset_naam_index ON {dv_schema}.valueset_code USING btree (valueset_naam );""".format(**params)
            self.execute(sql, 'create valuesets tables')

class DdlValset(Ddl):
    def create_or_alter_valueset(self, valueset_cls: DvValueset):
        if valueset_cls is DvValueset or valueset_cls is DvPeriodicalValueset:
            return
        super().create_or_alter_table(valueset_cls)


class DdlDatamart(Ddl):
    def __init__(self, pipeline, schema) -> None:
        super().__init__(pipeline, schema)

    def create_or_alter_dim(self, dim_cls):
        super().create_or_alter_table(dim_cls)

    def create_or_alter_fact(self, fact_cls):
        super().create_or_alter_table(fact_cls)
        return
        dm = self.layer
        if not dm.is_reflected:
            dm.reflect()
        facttable_name = fact_cls.cls_get_name()
        params = {}
        params['facttable'] = facttable_name
        params['dm'] = self.layer.name
        params['fact_columns_def'] = self.__get_fact_column_names_with_types(fact_cls)
        params['indexes'] = self.__get_indexes(fact_cls,params)
        params['constraints'] = self.__get_constraints(fact_cls,params)

        if not facttable_name in dm:
            sql = """CREATE TABLE IF NOT EXISTS {dm}.{facttable} (
                                  id serial,
                                  {fact_columns_def},
                                  {constraints}
                            )
                            WITH (
                              OIDS=FALSE,
                              autovacuum_enabled=true
                            );{indexes}""".format(**params)
            self.execute(sql, 'create <blue>{}</>'.format(facttable_name))

        else:
            fact_table = Table(facttable_name,dm)
            fact_table.reflect()
            add_fields = ''
            refected_column_names = [col.name for col in fact_table.columns]

            for col_name, col in fact_cls.__ordereddict__.items():
                if isinstance(col, Column):
                    if not col_name in refected_column_names:
                        add_fields += 'ADD COLUMN {} {}, '.format(col.name, col.type)
                if isinstance(col, DmReference):
                    if not col_name in refected_column_names:
                        add_fields += 'ADD COLUMN {} {}, '.format(col_name,'integer')
            add_fields = add_fields.rstrip(', ')
            params['add_fields'] = add_fields

            if add_fields:
                sql = """ALTER TABLE {dm}.{facttable} {add_fields};""".format(**params)
                self.execute(sql, 'alter <blue>{}</>'.format(facttable_name))


    def __get_fact_column_names_with_types(self, fact_cls):
        fields = ''
        for col in fact_cls.__cols__:
            fields += '{} {}, '.format(col.name, col.type)
        fields = fields[:-2]
        return fields


    def __get_indexes(self, fact_cls,params):
        fields = ''
        for name, field in fact_cls.__ordereddict__.items():
            if isinstance(field, DmReference):
                params['fk_fieldname'] = name
                fields += 'CREATE INDEX ix_{fk_fieldname}_{facttable} ON {dm}.{facttable}({fk_fieldname});\n'.format(**params)
        return fields

    def __get_constraints(self,fact_cls,params):
        fields = ''
        for name, field in fact_cls.__ordereddict__.items():
            if isinstance(field, DmReference):
                params['fk_fieldname'] = name
                params['dim_tabel'] = field.dim_cls.cls_get_name()
                fields += 'FOREIGN KEY ({fk_fieldname}) REFERENCES {dm}.{dim_tabel}(ID),\n'.format(**params)
        fields = fields.rstrip(',\n')
        return fields

