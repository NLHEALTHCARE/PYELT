import time
from typing import Dict
from sqlalchemy.engine import reflection

# from sample_domains._ensemble_views import get_ensemble

from pyelt.datalayers.database import Column, Columns, Schema, Table, DbFunction
from pyelt.datalayers.dm import DmReference
from pyelt.datalayers.dv import DvEntity, HybridSat, LinkReference, Link, DynamicLinkReference
from pyelt.datalayers.dwh import Dwh, DwhLayerTypes
from pyelt.datalayers.sor import Sor, SorTable

from pyelt.datalayers.dv import Ensemble_view
from pyelt.helpers.pyelt_logging import Logger, LoggerTypes
from pyelt.mappings.base import ConstantValue
from pyelt.mappings.sor_to_dv_mappings import EntityViewToEntityMapping, SorToEntityMapping, SorToLinkMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.process.base import BaseProcess


class Ddl(BaseProcess):
    def __init__(self, pipe: 'Pipe', layer: 'Schema') -> None:
        super().__init__(pipe)
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
        dwh = self.pipeline.dwh
        old_version_number = dwh.get_layer_version(layer_name)  # Type: float
        new_version_number = dwh.increase_layer_version(layer_name)
        self.sql_logger.log_simple("""
        --DDL aanpassing aan {}: {} --> {}
        --===========================
        """.format(layer_name, old_version_number, new_version_number))
        # if 'sor' in layer_name:
        #     self.pipe.sor.version = new_version_number
        # elif 'dv' in layer_name:
        #     self.pipe.pipeline.dwh.dv.version = new_version_number

    def get_version_old(self) -> None:
        layer_name = self.layer.name #self.pipe.get_layer_name_by_type(self.layer_type)
        sql = """SELECT version FROM sys.currentversion WHERE schemaname = '{}'""".format(layer_name)
        rows = self.dwh.execute_read(sql, 'get version')
        if len(rows) > 0:
            return float(rows[0][0])
        else:
            return None

    def increase_version_old(self) -> None:
        layer_name = self.layer.name
        self.old_version_number = self.get_version()
        if self.old_version_number:
            self.new_version_number = self.old_version_number + 0.01
            sql = """UPDATE sys.currentversion SET version = {} WHERE schemaname = '{}'""".format(self.new_version_number,
                                                                                                  layer_name)
            self.dwh.execute(sql, 'insert version')
        else:
            sql = """INSERT INTO sys.currentversion (schemaname, version, date) VALUES ('{}', 1, now());""".format(layer_name)
            self.dwh.execute(sql, 'update version')


    def init_logger(self) -> None:
        self.sql_logger = Logger.create_logger(LoggerTypes.DDL, self.pipeline.runid, self.pipeline.config, to_console=False)

    def __log_sql(self, log_message, sql, rowcount):
        msg = """{}

{}

rows: {}
--------------------------
""".format(log_message, sql, rowcount)
        self.sql_logger.log_simple(msg)

    def execute(self, sql: str, log_message: str) -> None:
        if not self.is_initialised:
            self.init()
        self.sql_logger.log_simple(sql + '\r\n')
        try:
            rowcount = self.dwh.execute(sql, log_message)
            self.logger.log(log_message, rowcount=rowcount, indent_level=4)
            self.__log_sql(log_message, sql, rowcount)
            self.layer.is_reflected = False
        except Exception as err:
            self.logger.log_error(log_message, sql, err.args[0])
            # print(err.args)
            # print(sql)
            # even wachten, want anders raakt error-msg verward met sql
            time.sleep(0.1)
            raise Exception(err)

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
                # print(db_func.get_stripped_body())
                # print('--------------')
                # print(self.layer.functions[complete_name].get_stripped_body())
                sql = db_func.to_create_sql()
                self.execute(sql, 'ALTER FUNCTION ' + complete_name)


class DdlSys(Ddl):
    pass
    #zie dwh.py voor aanmaken van sys tabellen

class DdlSor(Ddl):
    def __init__(self, pipe: 'Pipe') -> None:
        super().__init__(pipe, pipe.sor)

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

        # for step in self.steps:
        #     step(mappings)

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
        if not isinstance(mapping.source, SorTable):
            return
        sor = self.pipe.sor
        if not sor.is_reflected:
            sor.reflect()

        params = {}
        params.update(self._get_fixed_params())
        params['hub'] = mapping.target.get_hub_name()
        params['sor_table'] = mapping.source.name
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
        link = mapping.target
        if len(link.get_sats()) == 0:
            return

        sor = self.pipe.sor
        if not sor.is_reflected:
            sor.reflect()

        params = {}
        params.update(self._get_fixed_params())
        params['link'] = mapping.target.get_name()
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
    def __init__(self, pipe: 'Pipe') -> None:
        super().__init__(pipe, pipe.pipeline.dwh.dv)
        self.ddl_sor = DdlSor(pipe)



    #####################################
    # DV
    #####################################
    def create_or_alter_entity(self, entity: DvEntity) -> None:
        dv = self.dwh.dv
        entity_is_changed = False
        if not dv.is_reflected:
            dv.reflect()
        hub_name = entity.get_hub_name()
        params = {}
        params['hub'] = hub_name
        params.update(self._get_fixed_params())
        if not hub_name in dv:
            params['fixed_hub_columns_def'] = self.__get_fixed_hub_columns_def()

            sql = """CREATE TABLE IF NOT EXISTS {dv}.{hub} (
                          {fixed_hub_columns_def},
                          type text,
                          bk text NOT NULL,
                          CONSTRAINT {hub}_pkey PRIMARY KEY (_id),
                          CONSTRAINT {hub}_bk_unique UNIQUE (bk)
                    )
                    WITH (
                      OIDS=FALSE,
                      autovacuum_enabled=true
                    );""".format(**params)
            self.execute(sql, 'create <blue>{}</>'.format(hub_name))
            entity_is_changed = True
        sats = entity.get_sats()
        for sat in sats.values():
            self.__create_or_alter_sat(sat, entity)


        if entity_is_changed:
            self.create_or_alter_view(entity)

    def __create_or_alter_sat(self, sat, hub_or_link):
        dv = self.dwh.dv
        params = {}
        params.update(self._get_fixed_params())
        if hub_or_link.__base__ == DvEntity:
            hub_name = hub_or_link.get_hub_name()
            params['hub_or_link'] = hub_name
        elif hub_or_link.__base__ == Link:
            link_name = hub_or_link.get_name()
            params['hub_or_link'] = link_name
        sat_name = sat.get_name()
        params['sat'] = sat_name
        if not sat_name in dv:
            params['fixed_sat_columns_def'] = self.__get_fixed_sat_columns_def()
            params['sat_columns_def'] = self.__get_sat_column_names_with_types(sat)
            if not params['sat_columns_def']:
                return
            params['sat_pk_fields'] = '_id, _runid'
            if sat.__base__ == HybridSat:
                params['sat_pk_fields'] = '_id, _runid, type'
                params['fixed_sat_columns_def'] = self.__get_fixed_sat_columns_def(True)

            sql = """CREATE TABLE IF NOT EXISTS {dv}.{sat} (
                      {fixed_sat_columns_def},
                      {sat_columns_def},
                      CONSTRAINT {sat}_pkey PRIMARY KEY ({sat_pk_fields}),
                      CONSTRAINT fk_{sat}_{hub_or_link} FOREIGN KEY (_id) REFERENCES {dv}.{hub_or_link} (_id) MATCH SIMPLE
                )
                WITH (
                  OIDS=FALSE,
                  autovacuum_enabled=true
                ); """.format(**params)
            self.execute(sql, 'create <cyan>{}</>'.format(params['sat']))
            entity_is_changed = True
        else:
            sat_tbl = Table(sat_name, dv)
            sat_tbl.reflect()
            add_fields = ''
            for col_name, col in sat.__dict__.items():
                if isinstance(col, Column):
                    if not col.name:
                        col.name = col_name
                    if not col.name in sat_tbl:
                        add_fields += 'ADD COLUMN {} {}, '.format(col.name, col.type)
            add_fields = add_fields[:-2]
            if add_fields:
                params['add_fields'] = add_fields
                sql = """ALTER TABLE {dv}.{sat} {add_fields}; """.format(**params)
                self.dwh.confirm_execute(sql, 'alter <cyan>{}</> '.format(params['sat']))
                entity_is_changed = True
                # result = input('\r\nWil je de volgende wijzigingen aanbrengen in de database? (j=ja;n=negeer)\r\n{}\r\n'.format(sql))
                # if result.strip().lower()[:1] == 'j' or result.strip().lower()[:1] == 'y':
                #     self.execute(sql, 'alter sat table ' + params['sat'])
                #     entity_is_changed = True

    def __get_sat_column_names_with_types_old(self, sat_cls):
        fields = ''
        import inspect
        for attr in inspect.getmembers(sat_cls):
            col_name = attr[0]
            col = attr[1]
            if col == Column:
                if not col.name:
                    col.name = col_name
                fields += '{} {}, '.format(col.name, col.type)
        fields = fields[:-2]
        return fields

    def __get_sat_column_names_with_types(self, sat_cls):
        fields = ''
        for col_name, col in sat_cls.__ordereddict__.items():
            if isinstance(col, Column):
                if not col.name:
                    col.name = col_name
                fields += '{} {}, '.format(col.name, col.type)
        fields = fields[:-2]
        return fields

    def create_or_alter_link(self, cls_link: Link) -> None:
        dv = self.dwh.dv
        if not dv.is_reflected:
            dv.reflect()

        link_name = cls_link.get_name()
        params = {}
        params['link'] = link_name
        params.update(self._get_fixed_params())
        if not link_name in dv:
            params['fixed_link_columns_def'] = self.__get_fixed_link_columns_def()
            params['link_columns_def'] = self.__get_link_column_names(cls_link)
            params['foreign_key_constraints'] = self.__get_link_fk_constraints(cls_link)
            params['link_indexes'] = self.__get_link_indexes(cls_link)

            sql = """CREATE TABLE IF NOT EXISTS {dv}.{link} (
                          {fixed_link_columns_def},
                          type text,
                          {link_columns_def},
                          CONSTRAINT {link}_pkey PRIMARY KEY (_id),
                          {foreign_key_constraints}
                    )
                    WITH (
                      OIDS=FALSE,
                      autovacuum_enabled=true
                    );

                    {link_indexes}""".format(**params)
            self.execute(sql, 'create <blue>{}</>'.format(link_name))
        else:
            # Kijken naar wijzigingen
            link_tbl = Table(link_name, dv)
            if not link_tbl.is_reflected:
                link_tbl.reflect()
            add_fields = ''
            sql_indexes = ''
            for prop_name, link_ref in cls_link.__dict__.items():
                if isinstance(link_ref, LinkReference):
                    fk = link_ref.get_fk()
                    fk_params = {'dv': self.dwh.dv.name, 'hub': link_ref.entity_cls.get_hub_name(), 'fk': link_ref.get_fk(), 'link': cls_link.get_name()}
                    if not fk in link_tbl:
                        add_fields += """ADD COLUMN {fk} integer, ADD CONSTRAINT {fk}_constraint FOREIGN KEY ({fk}) REFERENCES {dv}.{hub} (_id) MATCH SIMPLE,\r\n""".format(**fk_params)
                    index_name = "ix_{link}{fk}".format(**fk_params)
                    if not index_name in link_tbl:
                        sql_indexes += """CREATE INDEX ix_{link}{fk} ON {dv}.{link} USING btree ({fk});\r\n""".format(**fk_params)
            add_fields = add_fields[:-3]
            sql_indexes = sql_indexes[:-3]
            if add_fields:
                params['add_fields'] = add_fields
                sql = """ALTER TABLE {dv}.{link} {add_fields};\r\n""".format(**params)
                sql += sql_indexes + ';'
                # result = input('\r\nWil je de volgende wijzigingen aanbrengen in de database (j=ja;n=negeer)?\r\n{}\r\n'.format(sql))
                # if result.strip().lower()[:1] == 'j' or result.strip().lower()[:1] == 'y':
                self.confirm_execute(sql, 'alter <blue>{}</>'.format(link_name))
        sats = cls_link.get_sats()
        for sat in sats.values():
            self.__create_or_alter_sat(sat, cls_link)




    def __get_link_column_names(self, link_cls):
        sql = ''
        for prop_name, link_ref in link_cls.__dict__.items():
            if isinstance(link_ref, LinkReference) or isinstance(link_ref, DynamicLinkReference):
                sql += """{} integer,\r\n""".format(link_ref.get_fk())
        sql = sql[:-3]
        return sql

    def __get_link_fk_constraints(self, link_cls):
        sql = ''
        for prop_name, link_ref in link_cls.__dict__.items():
            if isinstance(link_ref, LinkReference):
                # fk = """_fk_{}""".format(link_ref.name.lower())
                params = {'dv': self.dwh.dv.name, 'hub': link_ref.entity_cls.get_hub_name(), 'fk': link_ref.get_fk()} #.replace('_fk_parent_', '').replace('_fk_', '')}
                sql += """CONSTRAINT {fk}_constraint FOREIGN KEY ({fk}) REFERENCES {dv}.{hub} (_id) MATCH SIMPLE,\r\n""".format(
                    **params)
        sql = sql[:-3]
        return sql

    def __get_link_indexes(self, link_cls):
        sql = ''
        for prop_name, link_ref in link_cls.__dict__.items():
            if isinstance(link_ref, LinkReference) or isinstance(link_ref, DynamicLinkReference):
                params = {'dv': self.dwh.dv.name, 'fk': link_ref.get_fk(), 'link': link_cls.get_name()}
                sql += """CREATE INDEX ix_{link}{fk} ON {dv}.{link} USING btree ({fk});\r\n""".format(**params)
        sql = sql[:-3]
        return sql

    def __get_fixed_hub_columns_def(self):
        sql = """
        _id serial NOT NULL,
        _runid numeric(8,2) NOT NULL,
        _source_system character varying,
        _insert_date timestamp without time zone,
        _valid boolean DEFAULT TRUE,
        _validation_msg character varying
        """
        return sql

    def __get_fixed_sat_columns_def(self, is_hybrid= False):
        sql = """
        _id serial NOT NULL,
        _runid numeric(8,2) NOT NULL,
        _active boolean DEFAULT TRUE,
        _source_system character varying,
        _insert_date timestamp without time zone,
        _finish_date timestamp without time zone,
        _revision integer,
        _valid boolean NOT NULL DEFAULT TRUE,
        _validation_msg character varying,
        _hash character varying
        """
        if is_hybrid:
            sql = """
        _id serial NOT NULL,
        _runid numeric(8,2) NOT NULL,
        _active boolean DEFAULT TRUE,
        _source_system character varying,
        _insert_date timestamp without time zone,
        _finish_date timestamp without time zone,
        _revision integer,
        _valid boolean NOT NULL DEFAULT TRUE,
        _validation_msg character varying,
        _hash character varying,
        type text NOT NULL
        """
        return sql

    def __get_fixed_link_columns_def(self):
        sql = """
        _id serial NOT NULL,
        _runid numeric(8,2) NOT NULL,
        _source_system character varying,
        _insert_date timestamp without time zone,
        _valid boolean DEFAULT TRUE,
        _validation_msg character varying
        """
        return sql

    # def __mappings_to_sat_columns_def(self, mappings):
    #     sql = ''
    #     temp_fields_dict = {}
    #     for field_map in mappings.field_mappings:
    #         if field_map.target.name not in temp_fields_dict:
    #             sql += """{} {},\r\n""".format(field_map.target.name, field_map.target.type)
    #             temp_fields_dict[field_map.target.name] = field_map.target
    #     sql = sql[:-3]
    #     return sql
    #
    # def __mappings_sat_fields(self, mappings, alias):
    #     sql = ''
    #     for field_map in mappings.field_mappings:
    #         # sql += """{0}.{1} AS {2}_{1}, """.format(alias, field_map.target.name, mappings.target.replace('_sat', ''))
    #         sql += """{0}.{1} AS {1}, """.format(alias, field_map.target.name)
    #         if field_map.ref:
    #             # sql += """{0}.descr AS {2}_{1}_descr, """.format(field_map.ref, field_map.target.name, mappings.target.replace('_sat', ''))
    #             sql += """{0}.descr AS {1}_descr, """.format(field_map.ref, field_map.target.name)
    #     sql = sql[:-2]
    #     return sql
    #
    # def __mappings_to_link_columns_def(self, mappings):
    #     sql = ''
    #
    #     for field_mapping in mappings.field_mappings:
    #         sql += """{} {},\r\n""".format(field_mapping.target.name, field_mapping.target.type)
    #     sql = sql[:-3]
    #     return sql
    #
    # def __mappings_to_link_fk_constraints(self, mappings):
    #     sql = ''
    #     for field_mapping in mappings.field_mappings:
    #         if isinstance(field_mapping.source, ConstantValue): continue
    #         params = {'dv': self.dwh.dv.name, 'hub': field_mapping.target.table.name, 'fk': field_mapping.target} #.replace('_fk_parent_', '').replace('_fk_', '')}
    #         sql += """CONSTRAINT fk_{fk} FOREIGN KEY ({fk}) REFERENCES {dv}.{hub} (_id) MATCH SIMPLE,\r\n""".format(
    #             **params)
    #     sql = sql[:-3]
    #     return sql
    #
    # def __mappings_to_link_indexes(self, mappings):
    #     sql = ''
    #     # for hub in mappings.hubs:
    #     #     params = {'dv': self.dwh.dv.name, 'link': mappings.link, 'hub': hub}
    #     #
    #     #     sql += """-- CREATE INDEX ix_{link}_fk_{hub} ON {dv}.{link} USING btree (_fk_{hub});\r\n""".format(**params)
    #     sql = sql[:-3]
    #     return sql

    def create_or_alter_view(self, entity_cls: 'DvEntity'):
        dv = self.dwh.dv
        if not dv.is_reflected:
            dv.reflect()
        params = {}
        params.update(self._get_fixed_params())

        view_name = entity_cls.get_hub_name().replace('_hub', '_view')
        params['view_name'] = view_name
        params['hub'] = entity_cls.get_hub_name()

        if view_name in dv:
            sql = """DROP VIEW {dv}.{view_name};""".format(**params)
            self.execute(sql, 'drop view')

        sql_sat_fields = ''
        sql_join = ''
        sql_join_refs = ''
        index = 1
        # for mapping_name, sat_mapping in entity_mappings.sat_mappings.items():
        for sat_cls in entity_cls.get_sats().values():
            params['sat'] = sat_cls.get_name()

            if sat_cls.__base__ == HybridSat:
                for type in sat_cls.get_types():
                    params['sat_alias'] = 'sat' + str(index)
                    params['type'] = type
                    for col in sat_cls.get_columns():
                        if not col.name.startswith('_'):
                            alias = sat_cls.get_short_name() + '_' + type + '_' + col.name
                            alias = alias.replace(' ', '_')
                            if col.name == 'type':
                                idx = sat_cls.name.index('_sat') + 5
                                alias = sat_cls.name[idx:] + '_type'
                        sql_sat_fields += "sat{}.{} AS {}, ".format(index, col.name, alias)

                    sql_join += """LEFT OUTER JOIN {dv}.{sat} AS {sat_alias} ON {sat_alias}._id = hub._id AND {sat_alias}._active AND ({sat_alias}.type = '{type}' OR {sat_alias}.type IS NULL)\r\n        """.format(
                        **params)
                    index += 1

            else:
                params['sat_alias'] = 'sat' + str(index)
                for col in sat_cls.get_columns():
                    if not col.name.startswith('_'):
                        alias = sat_cls.get_short_name() + '_' + col.name
                        if col.name == 'type':
                            idx = sat_cls.name.index('_sat') + 5
                            alias = sat_cls.name[idx:] + '_type'
                        sql_sat_fields += "sat{}.{} AS {}, ".format(index, col.name, alias)
                        if isinstance(col, Columns.RefColumn):
                            params['ref_alias'] = col.name + '_' + col.ref_type
                            # if col.name in col.ref_type:
                            #     ref_type = ref_type.replace(col.name, '')
                            # elif col.ref_type in col.name:
                            #     ref_type = ref_type.replace(col.ref_type, '')
                            params['ref_type'] = col.ref_type
                            params['ref_field'] = col.name
                            sql_join_refs += """LEFT OUTER JOIN {dv}._ref_values AS {ref_alias} ON {sat_alias}.{ref_field}::text = {ref_alias}.code AND {ref_alias}.valueset_naam = '{ref_type}' AND {ref_alias}._active\r\n        """.format(
                                **params)
                            sql_sat_fields += "{ref_alias}.weergave_naam AS {ref_alias}_omschr, ".format(**params)



                            # sql_sat_fields += self.mappings_sat_fields(sat_mapping, params['sat_alias']) + ','
                sql_join += """LEFT OUTER JOIN {dv}.{sat} AS {sat_alias} ON {sat_alias}._id = hub._id AND {sat_alias}._active\r\n        """.format(**params)
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

        if params['sat_fields']:
            sql = """--DROP VIEW {dv}.{view_name};
CREATE OR REPLACE VIEW {dv}.{view_name} AS
    SELECT hub._id, hub.bk, hub.type, hub._runid, hub._source_system, True as _valid,
        {sat_fields}
    FROM {dv}.{hub} hub
        {join}""".format(**params)
            self.execute(sql, 'create <darkcyan>{}</>'.format(view_name))
        else:
            sql = """CREATE OR REPLACE VIEW {dv}.{view_name} AS
                SELECT hub._id, hub.bk, hub.type,
                    hub._runid as _runid, hub._source_system as hub_source_system, True as _valid
                FROM {dv}.{hub} hub
                    {join}""".format(**params)
            self.execute(sql, 'create <darkcyan>{}</>'.format(view_name))
        dv.is_reflected = False

    def create_or_alter_ensemble_view(self, ensemble):

        dv = self.dwh.dv
        dv.reflect()

        ensemble = TestEnsemble()  #todo: aanpassen want nu nog hardgecodeerd.
        inspector = reflection.Inspector.from_engine(dv.db.engine)

        # aanmaken van sub_strings voor sql:
        sql_ensemblename = 'dv.ensembleview'
        sql_columns = ''
        sql_selectedtables = ''
        sql_conditions = ''
        params = {'schema_name': 'dv'}

        for alias, cls in ensemble.entity_dict.items():
            cls.init_cls()
            cls.entity_name = cls.get_name().strip('_entity')
            cls.view_name = cls.get_name().strip('_entity') + '_view'

            if cls.__base__ == Link:
                sql_selectedtables += ', {schema_name}.'.format(**params) + cls.get_name()
            else:
                if alias == str(cls.get_name()):  # geen alias opgegeven; dus alias is hetzelfde als de entity_name
                    sql_selectedtables += ', {schema_name}.{} as {}'.format(cls.view_name, cls.entity_name,**params)
                    sql_conditions += ' AND {0}._id = fk_{0}_hub'.format(cls.entity_name)
                    sql_ensemblename = sql_ensemblename.replace('.', '.{}_'.format(cls.entity_name)) # hier wordt de opgehaalde entiteit geplaatst direct achter de punt en voor datgene dat eerst na de punt volgde.

                    cols = inspector.get_columns(cls.view_name, dv.name)
                    for col in cols:
                        col_name = col['name']
                        sql_columns += ', {0}.{1} as {0}_{1}'.format(cls.entity_name,col_name)

                else:  # alias voor entity_name is opgegeven
                    sql_selectedtables += ', {schema_name}.{} as {}'.format(cls.view_name,alias,**params)
                    sql_conditions += ' AND {0}._id = fk_{0}_{1}_hub'.format(alias,cls.entity_name)
                    sql_ensemblename = sql_ensemblename.replace('v.', 'v.{}_'.format(alias))

                    cols = inspector.get_columns(cls.view_name, dv.name)
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


    def create_or_alter_ref(self):
        #maak reftable
        dv = self.dwh.dv
        if not dv.is_reflected:
            dv.reflect()
        if '_ref_values' not in dv:
            params = {}
            params.update(self._get_fixed_params())
            params['fixed_columns_def'] = self.__get_fixed_sat_columns_def()
            sql = """CREATE TABLE IF NOT EXISTS {dv}._ref_valuesets (
                      {fixed_columns_def},
                      naam text,
                      oid text,
                      datum_van text,
                      datum_tot text,
                      status text,
                      versie text,
                      projecten text,
                      CONSTRAINT ref_valueset_pkey PRIMARY KEY (_id),
                      CONSTRAINT ref_valueset_oid_unique UNIQUE (oid)
                      --CONSTRAINT ref_valueset_naam_unique UNIQUE (naam)
                )
                WITH (
                  OIDS=FALSE,
                  autovacuum_enabled=true
                );


                CREATE TABLE IF NOT EXISTS {dv}._ref_values (
                      {fixed_columns_def},
                      --fk_valueset_type int,
                      fk_parent int,
                      temp_id text,
                      temp_fk text,
                      valueset_oid text,
                      valueset_naam text,
                      niveau text,
                      niveau_type text,
                      code text,
                      code_hl7 text,
                      weergave_naam text,
                      omschrijving text,
                      omschrijving2 text,
                      CONSTRAINT ref_values_pkey PRIMARY KEY (_id),
                      CONSTRAINT ref_values_code_unique UNIQUE (_runid, valueset_naam, code, niveau)
                )
                WITH (
                  OIDS=FALSE,
                  autovacuum_enabled=true
                );""".format(**params)
            self.execute(sql, 'create valuesets tables')

    def create_or_alter_table_exceptions(self):
        dv = self.dwh.dv
        if not dv.is_reflected:
            dv.reflect()
        if '_exceptions' not in dv:
            params = {}
            params.update(self._get_fixed_params())
            params['fixed_columns_def'] = self.__get_fixed_hub_columns_def()
            sql = """CREATE TABLE IF NOT EXISTS {dv}._exceptions (
                      {fixed_columns_def},
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
            self.execute(sql, 'create exception table')


class DdlDatamart(Ddl):
    def __init__(self, pipeline, schema) -> None:
        super().__init__(pipeline, schema)

    def create_or_alter_dim(self, cls):
        dm = self.layer
        if not dm.is_reflected:
            dm.reflect()
        dim_name = cls.get_name()
        params = {}
        params['dim'] = dim_name
        params['dm'] = self.layer.name
        params['dim_columns_def'] = self.__get_dim_column_names_with_types(cls)
        if not dim_name in dm:
            sql = """CREATE TABLE IF NOT EXISTS {dm}.{dim} (
                          id serial,
                          {dim_columns_def},
                          CONSTRAINT {dim}_pkey PRIMARY KEY (id)
                    )
                    WITH (
                      OIDS=FALSE,
                      autovacuum_enabled=true
                    );""".format(**params)
            self.execute(sql, 'create <blue>{}</>'.format(dim_name))
        else:
            dim_table = Table(dim_name, dm)
            dim_table.reflect()
            add_fields = ''
            refected_column_names = [col.name for col in dim_table.columns]

            for col_name, col in cls.__ordereddict__.items():
                if isinstance(col, Column):
                    if not col_name in refected_column_names:
                        add_fields += 'ADD COLUMN {} {}, '.format(col.name, col.type)
                        print(add_fields,'testestes')
            add_fields = add_fields.rstrip(', ')
            params['add_fields'] = add_fields

            if add_fields:
                sql = """ALTER TABLE {dm}.{dim} {add_fields};""".format(**params)
                self.execute(sql, 'alter <blue>{}</>'.format(dim_name))

    def __get_dim_column_names_with_types(self, dim_cls):
        fields = ''
        for col_name, col in dim_cls.__ordereddict__.items():
            if isinstance(col, Column):
                if not col.name:
                    col.name = col_name
                fields += '{} {}, '.format(col.name, col.type)
        fields = fields[:-2]
        return fields

    def create_or_alter_fact(self, fact_cls):
        dm = self.layer
        if not dm.is_reflected:
            dm.reflect()
        facttable_name = fact_cls.get_name()
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
            print(sql)
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
                        add_fields += 'ADD COLUMN {} {}, '.format(col.get_fk_field_name(),'integer')
            add_fields = add_fields.rstrip(', ')
            params['add_fields'] = add_fields

            if add_fields:
                sql = """ALTER TABLE {dm}.{facttable} {add_fields};""".format(**params)
                self.execute(sql, 'alter <blue>{}</>'.format(facttable_name))



    def __get_fact_column_names_with_types(self, fact_cls):
        fields = ''
        for name, field in fact_cls.__ordereddict__.items():
            if isinstance(field, DmReference):
                fields += '{} integer, '.format(field.get_fk_field_name())
        for col_name, col in fact_cls.__ordereddict__.items():
            if isinstance(col, Column):
                if not col.name:
                    col.name = col_name
                fields += '{} {}, '.format(col.name, col.type)
        fields = fields[:-2]
        return fields


    def __get_indexes(self, fact_cls,params):
        fields = ''
        for name, field in fact_cls.__ordereddict__.items():
            if isinstance(field, DmReference):
                params['fk_fieldname'] = field.get_fk_field_name()
                fields += 'CREATE INDEX ix_{fk_fieldname} ON {dm}.{facttable}({fk_fieldname});\n'.format(**params)
        return fields

    def __get_constraints(self,fact_cls,params):
        fields = ''
        for name, field in fact_cls.__ordereddict__.items():
            if isinstance(field, DmReference):
                params['fk_fieldname'] = field.get_fk_field_name()
                params['dim_tabel'] = field.dim_cls.get_name()
                fields += 'FOREIGN KEY ({fk_fieldname}) REFERENCES {dm}.{dim_tabel}(ID),\n'.format(**params)
        fields = fields.rstrip(',\n')
        return fields

