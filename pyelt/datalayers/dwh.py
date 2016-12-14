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
    """We onderscheiden 5 soorten database schemas:

    * Sor: laag met historische staging tabellen
    * Dv: laag met hubs, sats en links
    * Valset: laag met referentie data (code, omschrijving combinaties)
    * Dm: laag met dims en facts
    * sys: laag met logging info over de runs

    De dwh kan meerdere sor lagen bevatten. aan te raden per import-bron 1 sor laag
    De dwh kan ook meerdere dv-lagen bevatten, zoals de raw-dv, de dv en de valuesets
    De dwh kan meerdere dm lagen bevatten: per bedrijfsonderdeel kan er een eigen dm laag worden gemaakt.
    Er is altijd maar 1 sys laag
    """
    SOR = 'SOR' #type: str
    DV = 'DV' #type: str
    VALSET = 'VALSET'  # type: str
    DM = 'DM' #type: str
    SYS = 'SYS' #type: str

class Dwh(Database):
    def __init__(self, config: Dict[str, str] = {}) -> None:
        self.config = config  # type: Dict[str, str]
        if 'conn_dwh' in self.config:
            conn_string = self.config['conn_dwh']  # type: str
            super().__init__(conn_string)
        self.schemas = {}
        self.schemas['dv'] = Schema('dv', self, DwhLayerTypes.DV)  # type: Schema
        self.schemas['valset'] = Schema('valset', self, DwhLayerTypes.VALSET)  # type: Schema
        self.schemas['sys'] = Schema('sys', self, DwhLayerTypes.SYS)  # type: Schema
        # self.sys = Schema('sys', self)  # type: Schema
        # self.sors = {}  #type: Dict[str, Schema]
        # self.dvs = {}  #type: Dict[str, Schema]
        # self.valsets = {}  # type: Dict[str, Schema]
        # self.datamarts = {}  #type: Dict[str, Schema]
        # self.dvs['dv'] = Schema('dv', self)  # type: Schema
        # self.valsets['valset'] = Schema('valset', self)  # type: Schema

    def get_or_create_sor_schema(self, name) -> 'Schema':
        # if not 'sor_schema' in config:
        #     return self.get_sor_schema()
        if name in  self.schemas:
            return self.schemas[name]
        else:
            sor = Schema(name, self, DwhLayerTypes.SOR)
            self.schemas[name] = sor
        return sor

    # def get_sor_schema(self, name=''):
    #     found = None
    #     if name in self.sors:
    #         found = self.sors[name]
    #     return found

    def get_schema(self, name=''):
        found = None
        if name in self.schemas:
            found = self.schemas[name]
        return found

    @property
    def dv(self):
        return self.get_schema('dv')

    @property
    def rdv(self):
        return self.get_schema('rdv')

    @property
    def valset(self):
        return self.get_schema('valset')

    @property
    def sys(self):
        return self.get_schema('sys')

    def get_or_create_datamart_schema(self, dm_name: str) -> 'Schema':
        if dm_name in self.schemas:
            return self.schemas[dm_name]
        else:
            dm = Schema(dm_name, self, DwhLayerTypes.DM)
            self.schemas[dm_name] = dm
            return dm

    def create_schemas_if_not_exists(self, schema_name: str ='') -> None:
        self.reflect_schemas()
        if schema_name and not schema_name in self.reflected_schemas:
            sql = """CREATE SCHEMA {};""".format(schema_name)
            self.confirm_execute(sql, 'nieuw schema aanmaken')
            sor = Sor(schema_name, self)
            self.sors[schema_name] = sor
        for dv_schema_name in self.schemas.keys():
            if not dv_schema_name in self.reflected_schemas:
                sql = """CREATE SCHEMA {};""".format(dv_schema_name)
                self.confirm_execute(sql, 'nieuw ' + dv_schema_name + ' schema aanmaken')
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


    def log(self, msg: str) -> None:
        pass
