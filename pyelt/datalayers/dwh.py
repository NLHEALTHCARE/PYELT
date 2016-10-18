import time
from collections import OrderedDict
from typing import Dict, List, Any

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text

# from etl_mappings.general_configs import config as general_config
from pyelt.datalayers.database import Database, Schema
from pyelt.datalayers.sor import Sor


class DwhLayerTypes():
    SOR = 'SOR' #type: str
    RDV = 'RDV' #type: str
    DV = 'DV' #type: str
    SYS = 'SYS' #type: str

class Dwh(Database):
    def __init__(self, config: Dict[str, str] = {}) -> None:
        # if len(config) == 0:
        #     config = general_config
        self.config = config  # type: Dict[str, str]
        if 'conn_dwh' in self.config:
            conn_string = self.config['conn_dwh']  # type: str
            super().__init__(conn_string)
        self.sors = {}  # [Schema('sor', self)] #type: Dict[str, Schema]
        # self.default_sor = None #self.sors[0] #type: Schema
        self.rdv = Schema('rdv', self) #type: Schema
        self.dv = Schema('dv', self) #type: Schema
        self.sys = Schema('sys', self) #type: Schema
        self.datamarts = {}  # [Schema('dm', self)] #type: Dict[str, Schema]
        # self.default_sor = None #self.sors[0] #type: Schema

    def get_or_create_sor_schema(self, config: Dict[str,str] = {}) -> 'Schema':
        if not 'sor_schema' in config:
            return self.get_sor_schema()
        if config['sor_schema'] != 'sor':
            sor_name = config['sor_schema']
            sor = Schema(sor_name, self)
            self.sors[sor_name] = sor
        return sor

    def get_sor_schema(self, name=''):
        found = None
        if len(self.sors) == 1:
            for sor in self.sors.values():
                found = sor
        elif len(self.sors) > 1:
            found = self.sors[name]
        return found

    def get_or_create_datamart_schema(self, dm_name: str) -> 'Schema':
        dm = Schema(dm_name, self)
        self.datamarts[dm_name] = dm
        return dm


    def execute(self, sql: str, log_message: str=''):
        self.log('-- ' + log_message.upper())
        self.log( sql)

        start = time.time()
        connection = self.engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()

        rowcount = cursor.rowcount
        self.log('-- duur: ' + str(time.time() - start) +  '; aantal rijen:' + str(cursor.rowcount))
        self.log('-- =============================================================')
        cursor.close()
        return rowcount

    def execute_returning(self, sql: str, log_message: str = ''):
        self.log('-- ' + log_message.upper())
        self.log(sql)

        start = time.time()
        connection = self.engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()

        result = cursor.fetchall()
        self.log('-- duur: ' + str(time.time() - start) + '; aantal rijen:' + str(cursor.rowcount))
        self.log('-- =============================================================')
        cursor.close()
        return result

    def execute_read(self, sql, log_message='') -> List[List[Any]]:
        self.log('-- ' + log_message.upper())
        self.log(sql)

        start = time.time()
        # plan = text(sql)
        # result = engine.execute(sql)

        connection = self.engine.raw_connection()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql)
        result = cursor.fetchall()

        self.log('-- duur: ' + str(time.time() - start) + '; aantal rijen:' + str(cursor.rowcount))
        self.log('-- =============================================================')
        cursor.close()
        return result

    def confirm_execute(self, sql: str, log_message: str='') -> None:
        ask_confirm = False
        if 'ask_confirm_on_db_changes' in self.config:
            ask_confirm = self.config['ask_confirm_on_db_changes']
        else:
            ask_confirm = 'debug' in self.config and self.config['debug']
        if ask_confirm:
            sql_sliced = sql
            if len(sql) > 50:
                sql_sliced = sql[:50] + '...'
            else:
                sql_sliced = sql
            result = input('{}\r\nWil je de volgende wijzigingen aanbrengen in de database?\r\n{}\r\n'.format(log_message.upper(), sql_sliced))
            if result.strip().lower()[:1] == 'j' or result.strip().lower()[:1] ==  'y':
                self.execute(sql, log_message)
                # print(log_message, 'uitgevoerd')
            else:
                raise Exception('afgebroken')
        else:
            self.execute(sql, log_message)

    def log(self, msg: str) -> None:
        pass

    def create_schemas_if_not_exists(self, schema_name: str ='') -> None:
        self.reflect_schemas()
        if schema_name and not schema_name in self.reflected_schemas:
            sql = """CREATE SCHEMA {};""".format(schema_name)
            self.confirm_execute(sql, 'nieuw schema aanmaken')
            sor = Sor(schema_name, self)
            self.sors[schema_name] = sor
        if not self.dv.name in self.reflected_schemas:
            sql = """CREATE SCHEMA {};""".format(self.dv.name)
            self.confirm_execute(sql, 'nieuw dv schema aanmaken')
        if not self.sys.name in self.reflected_schemas:
            sql = """CREATE SCHEMA {};""".format(self.sys.name)
            self.confirm_execute(sql, 'nieuw sys schema aanmaken')
            sql = self.get_create_sys_ddl()
            self.confirm_execute(sql, 'nieuwe sys tabellen aanmaken')

    def get_create_sys_ddl(self) -> str:
        # ivm cross import reference conficts staat deze sql hier en niet in ddl.py
        sql = """
        CREATE TABLE sys.runs
        (
          runid numeric(8,2) NOT NULL,
          rundate timestamp without time zone NOT NULL DEFAULT now(),
          finish_date timestamp without time zone,
          exceptions boolean,
          sor_versions text,
          dv_version numeric(8,2),
          pyelt_version text,
          CONSTRAINT "PK" PRIMARY KEY (runid)
        )
        WITH (
          OIDS=FALSE
        );

        CREATE TABLE sys.currentversion
        (
          schemaname text NOT NULL,
          version numeric(8,2) NOT NULL,
          date timestamp without time zone,
          CONSTRAINT currentversion_pkey PRIMARY KEY (schemaname, version),
          CONSTRAINT currentversion_schemaname_key UNIQUE (schemaname)
        )
        WITH (
          OIDS=FALSE
        );
        INSERT INTO sys.runs (runid, rundate) VALUES (0, now());
        """ #type: str

        return sql

    def get_layer_version(self, layer_name) -> None:
        sql = """SELECT version FROM sys.currentversion WHERE schemaname = '{}'""".format(layer_name)
        rows = self.execute_read(sql, 'get version')
        if len(rows) > 0:
            return float(rows[0][0])
        else:
            return None

    def increase_layer_version(self, layer_name) -> None:
        old_version_number = self.get_layer_version(layer_name)
        if old_version_number:
            new_version_number = old_version_number + 0.01
            sql = """UPDATE sys.currentversion SET version = {} WHERE schemaname = '{}'""".format(new_version_number, layer_name)
            self.execute(sql, 'update version')
        else:
            new_version_number = 1.0
            sql = """INSERT INTO sys.currentversion (schemaname, version, date) VALUES ('{}', 1, now());""".format(layer_name)
            self.execute(sql, 'insert version')
        return new_version_number
