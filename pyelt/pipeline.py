import datetime
import inspect
import math
from collections import OrderedDict
from typing import List

from main import get_root_path
# from sample_domains import _ensemble_views
from pyelt.datalayers.database import * #Schema, DbFunction
# from pyelt.datalayers.dm import Dim, Fact
from pyelt.datalayers.dv import * #HubEntity, Link, EnsembleView, HybridLink, DvValueset
from pyelt.datalayers.dwh import Dwh, DwhLayerTypes
from pyelt.datalayers.sys import *
from pyelt.datalayers.valset import DvValueset

from pyelt.helpers.pyelt_logging import Logger, LoggerTypes
from pyelt.helpers.validations import DomainValidator, MappingsValidator
from pyelt.mappings.sor_to_dv_mappings import SorToValueSetMapping, EntityViewToEntityMapping, EntityViewToLinkMapping, SorToEntityMapping, SorToLinkMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
from pyelt.mappings.validations import SorValidation, DvValidation, Validation
from pyelt.process.ddl import * # DdlSor, DdlDv, Ddl, DdlDatamart
from pyelt.process.etl import EtlSourceToSor, EtlSorToDv
from pyelt.sources.databases import SourceDatabase




# class Singleton(object):
#
#
#   def __init__(self):
#       if not Singleton._instance:
#           Singleton._instance = self
#       else:
#           for k, v in Singleton._instance.__dict__.items():
#               self.__dict__[k] = v






class Pipeline():
    """
De *pipeline* omvat alle lagen en alle bronnen. De pipeline bevat de databaseverbinding naar de datavault-datawarehouse, die bestaat uit de verschillende lagen:
- sor(s)
- rdv
- dv
- datamart(s)
Voorbeeld::
    pipeline = Pipeline()
    datawarehouse = pipeline.dwh
    rdv = datawarehouse.rdv
    dv = datawarehouse.dv
.. Note:: Een pipeline is een singleton implementatie; er is altijd maar 1 pipeline object voor alle processen. Elke nieuwe pipeline, die wordt aangemaakt, zal dezelfde waardes bevatten.
   """

    _instance = None

    def __new__(cls, *args, **kwargs):
        """ returns cls._instance

        Definieert wat de instances van de parameters *pyelt_config*, *dwh*, *runid*, *pipes*, *logger* en *sql_logger* zijn.
        :param config: de globale pyelt configuratie (dict) met daarin oa. dwh connectie string
        """
        if not cls._instance:
            # singleton implementatie
            cls._instance = super(Pipeline, cls).__new__(
                cls)
            if args:
                config = args[0]
            else:
                config = kwargs['config']

            # cls._instance.pyelt_config = config #: pyelt_config
            cls._instance.config = config
            cls._instance.dwh = Dwh(config)
            cls._instance.runid = 1.00  # type: float
            cls._instance.pipes = OrderedDict()  # type: Dict[str, Pipe]
            cls._instance.logger = None  # type: Logger
            cls._instance.sql_logger = None  # type: Logger
            cls._instance.domain_modules = {}
            cls._instance.datamart_modules = {}
        return cls._instance



    def get_or_create_pipe(self, source_system, config={}) -> 'Pipe':
        """
        Maakt een nieuwe *pipe* aan, indien deze nog niet bestaat, of retourneert een reeds bestaande *pipe* op basis van unieke naam van bron systeem

        :param source_system: naam van het *bronsysteem*; bv 'timeff'
        :param config: dict met de gebruikte configuratie
        :returns: Pipe

        """
        source_system = source_system.replace('sor_', '')
        if 'sor_schema' not in config:
            config['sor_schema'] = 'sor_' + source_system
        if not source_system in self.pipes:
            pipe = Pipe(source_system, self, config)
            self.pipes[source_system] = pipe
        return self.pipes[source_system]

    def run(self, parts=['sor', 'valuesets', 'hubs', 'links', 'views', 'viewlinks']) -> None:
        """
        Deze functie start de run van de pipeline. Er wordt ddl uitgevoerd, een logbestand aangemaakt, een sql_logbestand en de etl per pipe wordt uitgevoerd..


        :param parts: lijst van te runnen onderdelen, indien leeg gelaten worden alle onderdelen gerund.

        Voorbeeld::

            pipeline.run(['sor', 'refs'])

        Nu wordt alleen de sor laag gerunt en de referentie tabel in de dv gevuld.

        """
        self.send_start_mail()
        self.dwh.create_schemas_if_not_exists()
        for schema in self.dwh.schemas.values():
            if schema.schema_type == DwhLayerTypes.SYS:
                self.create_sys_tables(schema)
        self.runid = self.create_new_runid()
        self.logger = Logger.create_logger(LoggerTypes.MAIN, self.runid, self.config)
        self.sql_logger = Logger.create_logger(LoggerTypes.SQL, self.runid, self.config, to_console=False)

        if not self.validate_domains():
            return
        if not self.validate_mappings_before_ddl():
            return

        self.logger.log('<b>START RUN {0:.2f}</>'.format(self.runid))
        self.logger.log('<b>START DDL</>')
        for schema in self.dwh.schemas.values():
            if schema.schema_type == DwhLayerTypes.VALSET:
                self.create_valueset_from_domain(schema)
        for schema in self.dwh.schemas.values():
            if schema.schema_type == DwhLayerTypes.DV:
                self.create_dv_from_domain(schema)

        for pipe in self.pipes.values():
            self.logger.log('DDL PIPE ' + pipe.source_system, indent_level=1)
            self.dwh.create_schemas_if_not_exists(pipe.sor.name)
            pipe.create_sor_from_mappings()

            pipe.run_extra_sql()
            pipe.create_db_functions()

            self.logger.log('FINISH DDL PIPE ' + pipe.source_system, indent_level=1)

        for name, module in self.datamart_modules.items():
            self.dwh.create_schemas_if_not_exists(name)
        self.create_datamarts()

        self.logger.log('FINISH DDL')
        self.logger.log('')

        is_valid = self.validate_mappings_after_ddl()
        if not is_valid:
            return


        #to do asyncstatus_msg
        self.logger.log('<b>START ETL</>')
        for pipe in self.pipes.values():
            self.logger.log('=====================================')
            self.logger.log('===== START PIPE {}'.format(pipe.source_system))
            self.logger.log('=====================================')
            pipe.run(parts)
            self.logger.log('=====================================')
            self.logger.log('===== FINISH PIPE {}'.format(pipe.source_system))
            self.logger.log('=====================================', )
        self.logger.log('FINISH ETL')
        self.logger.log('')
        self.end_run()

        if self.logger.errors:
            self.error_logger = Logger.create_logger(LoggerTypes.ERROR, self.runid, self.config, to_console=True)
            self.error_logger.log_simple('<red>RUN {0:.2f} READY WITH {1} ERRORS:'.format(self.runid, len(self.logger.errors)))
            self.error_logger.log_simple('SEE: {}'.format(self.logger.filename))
            index = 1
            for err_msg in self.logger.errors:
                self.error_logger.log_simple('ERROR ' + str(index))
                self.error_logger.log_simple(err_msg)
                index += 1
            self.error_logger.log_simple('')
            self.error_logger.log_simple('--------------------------------------------------------------------------')
            self.error_logger.log_simple('---E-N-D---R-U-N----------------------------------------------------------')
            self.error_logger.log_simple('--------------------------------------------------------------------------')
            self.error_logger.log_simple('')
        else:
            Logger.pprint('<green><b>READY</> ')

        self.send_log_mail()

    def validate_domains(self) -> bool:
        """Valideert of de geregistreerde domeinen van alle pipes geldig zijn.

        Wordt uitgevoerd voorafgaande aan de ddl.

        :return: Boolean
        """

        self.logger.log('PRE-RUN VALIDATE DOMAINS')
        validation_msg = ''
        validator = DomainValidator()
        for domain_module in self.domain_modules.values():
            validation_msg += validator.validate(domain_module)

        if validation_msg:
            self.logger.log(validation_msg, indent_level=1)
            return False
        else:
            self.logger.log('<green>Domains zijn OK.</>', indent_level=1)
        self.logger.log('')
        return True

    def validate_mappings_before_ddl(self) -> bool:
        """ Valideert of de mappings van alle pipes geldig zijn.

        Wordt uitgevoerd voorafgaande aan de ddl

        :return: Boolean
        """
        self.logger.log('VALIDATE MAPPINGS BEFORE DDL')
        validation_msg = ''
        for pipe in self.pipes.values():
            validation_msg += pipe.validate_mappings_before_ddl()
        if validation_msg:
            self.logger.log('  ' + validation_msg)
            return False
        else:
            self.logger.log('  <green>Mappings zijn OK.</>')
        self.logger.log('')
        return True

    def validate_mappings_after_ddl(self) -> bool:
        """ Valideert of de mappings van alle pipes geldig zijn.

        Wordt uitgevoerd voorafgaande aan de etl, maar nadat de ddl heeft gerund.

        :return: Boolean
        """
        self.logger.log('VALIDATE MAPPINGS')
        validation_msg = ''
        for pipe in self.pipes.values():
            validation_msg += pipe.validate_mappings_after_ddl()
        if validation_msg:
            self.logger.log('  ' + validation_msg)
            return False
        else:
            self.logger.log('  <green>Mappings zijn OK.</>')
        self.logger.log('')
        return True

    def create_new_runid(self) -> float:
        """
        Maakt een nieuw *runid* aan. De runid wordt met 0,01 verhoogd voor iedere volgende run op dezelfde dag. De eerste run op een dag krijgt het eerste gehele getal volgend op de maximale aanwezige *runid* in de tabel.

        Runid wordt bewaard in de database in het sys-schema

        :return: self.runid
        """

        sql = 'SELECT max(runid) as max_runid, max(rundate) as max_rundate from sys.runs'
        rows = self.dwh.execute_read(sql, 'get max run id')
        if len(rows) > 0 and rows[0][0]:
            row = rows[0]
            max_runid = float(row[0])
            max_rundate = row[1]
        else:
            max_runid = 0
            max_rundate = datetime.datetime(1900,1,1, 0, 0, 0)
        if max_rundate.date() == datetime.date.today():
            self.runid = round(max_runid + 0.01, 2)
        else:
            self.runid = math.floor(max_runid) + 1

        git_hashes = self.__get_git_hashes()
        sql = """INSERT INTO sys.runs (runid, rundate, pyelt_version) VALUES ({}, now(), '{}')""".format(self.runid, git_hashes)
        self.dwh.execute(sql, 'insert new run id')
        return self.runid

    def __get_git_hashes(self):
        git_hashes = {}
        import subprocess, os
        for name in os.listdir(get_root_path()):
            if os.path.isdir(name):
                subdirs = os.listdir(name)
                if '.git' in subdirs:
                    os.chdir(get_root_path() + '/' + name)
                    git_commit_number = subprocess.check_output(["git", "rev-parse", 'HEAD']).decode('ascii')
                    git_hashes[name] = git_commit_number
        return str(git_hashes).replace("'", '"')



    def end_run(self):
        """
        Beëindigt de run en maakt een update van de database tabel sys.runs met een *finish_date* en eventuele *exceptions*.

        """
        Validation().report_run(self)

        exceptions = len(self.logger.errors) > 0
        dv_version = self.dwh.get_layer_version(self.dwh.dv.name)
        if not dv_version:
            dv_version = 'NULL'
        sor_versions = ''
        for pipe in self.pipes.values():
            sor_version = self.dwh.get_layer_version(pipe.sor.name)
            sor_versions += '{}={};'.format(pipe.sor.name, sor_version)
        sql = """UPDATE sys.runs SET finish_date = now(), exceptions = '{1}', dv_version = {2}, sor_versions = '{3}' WHERE runid={0}""".format(self.runid,
                                                                                                                                               exceptions, dv_version, sor_versions)
        self.dwh.execute(sql, 'update run set finish date')


    def send_start_mail(self):
        if not 'email_settings' in self.config:
            return
        elif 'send_mail_before_run' in self.config['email_settings'] and self.config['email_settings']['send_mail_before_run']:
            params = self.config['email_settings']
            params['to'] = params['to'].replace(';', ',')

            from sys import platform
            if platform == "linux" or platform == "linux2":
                linux_cmd = """echo -e "De pyelt run is gestart" | mail -s "De pyelt run is gestart" -r "{from}" "{to}" """.format(**params)
                import os
                os.system(linux_cmd)

    def send_log_mail(self):
        if not 'email_settings' in self.config:
            return
        elif 'send_log_mail_after_run' in self.config['email_settings'] and self.config['email_settings']['send_log_mail_after_run']:
            params = self.config['email_settings']
            params['to'] = params['to'].replace(';', ',')
            params['attachments_command'] = ' -a "' + get_root_path() + self.config['log_path'] + self.logger.filename + '"'

            if self.logger.errors:
                params['subject'] += ' ER IS IETS FOUT GEGAAN '
                params['attachments_command'] += ' -a "' + get_root_path() + self.config['log_path'] + self.error_logger.filename + '"'

            from sys import platform
            if platform == "linux" or platform == "linux2":
                linux_cmd = """echo -e "{msg}" | mail {attachments_command} -s "{subject}" -r "{from}" "{to}" """.format(**params)
                import os
                os.system(linux_cmd)

    def register_domain(self, module, schema_name = 'dv'):
        """
        Registreert de module met het domein.

        :param module: de module met daarin de domein classes; bijvoorbeeld: "domain_huisartsen"

        Je kunt meerdere modules registreren door de functie vaker aan te roepen::

            pipeline.register_domain(domain_huisartsen)
            pipeline.register_domain(domain_ziekenhuizen)

        Tijdens de dll zullen alle tabellen uit beide domeinen aangemaakt worden.
        """

        module_name = "{}.{}".format(schema_name, module.__name__)
        self.domain_modules[module_name] = module
        self.dwh.set_schema(schema_name, DwhLayerTypes.DV)
        # init module
        for name, cls in inspect.getmembers(module,  inspect.isclass):
            if HubEntity in cls.__mro__:
                cls.__dbschema__ = schema_name
                cls.Hub.__dbschema__ = schema_name
                for sat in cls.__sats__.values():
                    sat.__dbschema__ = schema_name

            if LinkEntity in cls.__mro__:
                cls.__dbschema__ = schema_name
                cls.Link.__dbschema__ = schema_name
                for sat in cls.__sats__.values():
                    sat.__dbschema__ = schema_name


    def register_valset_domain(self, module, schema_name = 'valset'):
        """
        Registreert de module met het domein.

        :param module: de module met daarin de domein classes; bijvoorbeeld: "domain_huisartsen"

        Je kunt meerdere modules registreren door de functie vaker aan te roepen::

            pipeline.register_domain(domain_huisartsen)
            pipeline.register_domain(domain_ziekenhuizen)

        Tijdens de dll zullen alle tabellen uit beide domeinen aangemaakt worden.
        """

        module_name = "{}.{}".format(schema_name, module.__name__)
        self.domain_modules[module_name] = module
        self.dwh.set_schema(schema_name, DwhLayerTypes.VALSET)
        # init module-classes: zet dbschema
        for name, cls in inspect.getmembers(module,  inspect.isclass):
            if DvValueset in cls.__mro__ :
                if cls.__dbschema__  != schema_name:
                    cls.__dbschema__ = schema_name


    def register_datamart(self, module, schema_name = ''):
        if not schema_name:
            schema_name = module.__name__
        self.datamart_modules[schema_name] = module
        self.dwh.get_or_create_sor_schema(schema_name)
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if AbstractOrderderTable in cls.__mro__ :
                cls.__dbschema__ = schema_name

    def create_sys_tables(self, schema):
        ddl = Ddl(self, schema)
        ddl.create_or_alter_table(Sys.Runs)
        ddl.create_or_alter_table(Sys.Currentversion)

    def create_valueset_from_domain(self, schema):
        """
        Voert ddl uit van de dv laag. Maakt eventuele nieuwe tabellen aan (hubs, sats en links) gebaseerd op de gedefiniëerde domeinen.

        """
        self.logger.log('START CREATE VALSET'.format(self.runid), indent_level=2)
        ddl = DdlValset(self, schema)

        ddl.create_or_alter_table_exceptions(schema)
        domain_modules = {k:v for k,v in self.domain_modules.items() if k.startswith(schema.name + '.')}

        # VALUESETS
        for module_name, module in domain_modules.items():
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if DvValueset in cls.__mro__:
                    ddl.create_or_alter_valueset(cls)

    def create_dv_from_domain(self, schema):
        """
        Voert ddl uit van de dv laag. Maakt eventuele nieuwe tabellen aan (hubs, sats en links) gebaseerd op de gedefiniëerde domeinen.

        """
        self.logger.log('START CREATE DV'.format(self.runid), indent_level=2)
        ddl = DdlDv(self, schema)

        ddl.create_or_alter_table_exceptions(schema)
        domain_modules = {k:v for k,v in self.domain_modules.items() if k.startswith(schema.name + '.')}

        #CREATE HUBS AND SATS
        for module_name, module in domain_modules.items():
            # if not module_name.startswith(schema.name + '.'):
            #     continue
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if HubEntity in cls.__mro__:
                    ddl.create_or_alter_entity(cls)

        # LINKS
        # Dezelfde for-loop wordt hieronder herhaald, want eerst moeten alle hubs zijn aangemaakt voordat de links aangemaakt kunnen worden met ref. integriteit op de database
        for module_name, module in domain_modules.items():
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if LinkEntity in cls.__mro__:
                    ddl.create_or_alter_link(cls)

        if 'create_views' in self.config and self.config['create_views']:
            #CREATE VIEWS
            # Dezelfde for-loop wordt hieronder herhaald, want eerst moeten alle parent hubs zijn aangemaakt voordat de vies met child hubs kunnen worden aangemaakt
            for module_name,module in domain_modules.items():
                for name, cls in inspect.getmembers(module, inspect.isclass):
                    if HubEntity in cls.__mro__ and cls != HubEntity:
                        ddl.create_or_alter_view(cls)

            # Dezelfde for-loop wordt hieronder herhaald, want eerst moeten alle views en links zijn aangemaakt voordat de ensemble_view gemaakt kan worden
            for module_name,module in domain_modules.items():
                for name, cls in inspect.getmembers(module, inspect.isclass):
                    if cls.__base__ == EnsembleView:
                        ddl.create_or_alter_ensemble_view(cls)

        self.logger.log('FINISH CREATE DV'.format(self.runid), indent_level=2)

    def create_datamarts(self):
        """
        Voert ddl uit van de datamart laag. Maakt eventuele nieuwe tabellen aan (dims, facts) gebaseerd op geregistreerde model(len).

        """
        self.logger.log('START CREATE DATAMARTS', indent_level=2)
        for name, module in self.datamart_modules.items():
            ddl = DdlDatamart(self, self.dwh.get_or_create_datamart_schema(name))
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if cls.__base__ == Dim:
                    ddl.create_or_alter_dim(cls)

            # Dezelfde for-loop wordt hieronder herhaald, want eerst moeten alle dims zijn aangemaakt voordat de facts aangemaakt kunnen worden met ref. integriteit op de database
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if cls.__base__ == Fact:
                    ddl.create_or_alter_fact(cls)

        self.logger.log('FINISH CREATE DATAMARTS', indent_level=2)



class Pipe():
    """
Een pipeline bevat 1 of meerdere pipes. Een pipe is een verbinding met 1 bronsysteem, maar bevat wel alle lagen in de datavault.

Bijvoorbeeld, we maken een pipe aan met de naam 'timeff', met als bronsysteem een oracle database en 'sor_timeff' als de naam van het sor schema::

        timeff_config = {
            'source_connection': 'oracle://SID:pwd@server/db',
            'default_schema': 'MTDX',
            'sor_schema': 'sor_timeff'
        }
        pipe = pipeline.get_or_create_pipe('timeff', timeff_config)
        sor = pipe.sor
        print(sor.name)
        >> sor_timeff
        print(sor.source_db.default_schema.name)
        >> MTDX

.. Note:: Indien bij het aanmaken van de pipe het sor schema nog niet bestaat, dan wordt deze eerst aangemaakt.

    """

    def __init__(self,source_system, pipeline, config = {}):
        """Stelt de begin status van *pipe* in.

        :param source_system: de naam van huidige gebruikte source_system; bijvoorbeeld "timeff"
        :param pipeline: de huidige pipeline volgens de gebruikte config
        :param config: de dict met de configuratie voor deze pipe met daarin oa de bronsysteem config

        .. Note:: Maak een pipe altijd aan via de *pipeline.get_or_create_pipe*
        """
        self.source_system = source_system
        self.pipeline = pipeline
        self.config = config
        self.mappings = []
        self.validations = []
        self.source_db = None
        self.source_path = ''
        if 'source_connection' in config:
            temp_data_transfer_path = ''
            if 'datatransfer_path' in pipeline.config:
                temp_data_transfer_path = pipeline.config['datatransfer_path']
            self.source_db = SourceDatabase(config['source_connection'], config['default_schema'], temp_data_transfer_path)
        elif 'source_path' in config:
            self.source_path = config['source_path']
        self.sor = pipeline.dwh.get_or_create_sor_schema(config['sor_schema'])

        self.db_functions = {}
        self.extra_ddl_statements = []  # type: List[str]
        self.run_after_sor = []


    @property
    def runid(self):
        """

        :return: self.pipeline.runid
        """
        return self.pipeline.runid

    def register_domain(self, module, schema_name = 'dv'):
        """
        Registreert de module met het domein. Geeft door aan Pipeline
        :param module: de module met daarin de domein classes; bijvoorbeeld: "domain_huisartsen"
        :param schema_name: Het dv-schema in Postgresql. Bijvoorbeeld dv of rdv
        """
        self.pipeline.register_domain(module, schema_name)


    def register_db_functions(self, module, schema: Schema = None) -> None:
        """
        Registreert de module met database functies. Dat is een module met een of meerder classes die over erven van DbFunction.

        Tijdens de ddl worden de functies aangemaakt op de database.

        :param module: de module met database functies. ; bijvoorbeeld: "timeff_functies"
        :param schema: Het schema in Postgresql waarop de hier geregistreerde functies van toepassing zijn; bijvoorbeeld: 'dv' of 'sor_timeff'

        """
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__base__ == DbFunction:  # geen superclasses zelf meenemen
                function = cls()
                if schema:
                    function.schema = schema
                self.register_db_function(function)

    def register_extra_ddl(self, extra_ddl: List[str]) -> None:
        """
        Registreert  additionele uit te voeren (eenmalige) sql code die op de database wordt uitgevoerd

        :param extra_sql: een lijst met strings  van geldige sql code

        """
        self.extra_ddl_statements.extend(extra_ddl)

    def register_db_function(self, func: 'DbFunction') -> None:
        """
        Registreert de database functie.

        :param func: de functie die geregistreert wordt
        """
        self.db_functions[func.name] = func

    def register_run_after_sor(self, func) -> None:
        """
        Registreert  additionele uit te voeren (eenmalige) sql code die op de database wordt uitgevoerd

        :param extra_sql: een lijst met strings  van geldige sql code

        """
        self.run_after_sor.append(func)

    def create_sor_from_mappings(self):
        """
        Voert ddl uit van de sor-laag. Maakt eventuele nieuwe sor-tabellen aan aan de hand van de gedefiniëerde mappings zoals bijvoorbeeld in "timeff_mappings_old.py".

        """
        self.pipeline.logger.log('START CREATE SOR'.format(self.pipeline.runid), indent_level=2)
        ddl = DdlSor(self)
        ddl.create_or_alter_table_exceptions(self.sor)
        for mapping in self.mappings:
            if isinstance(mapping, SourceToSorMapping):
                ddl.create_or_alter_sor(mapping)
        self.pipeline.logger.log('FINISH CREATE SOR'.format(self.pipeline.runid), indent_level=2)

    def create_db_functions(self):
        """
        DDL functie. Maakt nieuwe database functies aan.

        """
        ddl = Ddl(self, self.sor)
        ddl.create_or_alter_functions(self.db_functions)

    def run_extra_sql(self):
        """
        Runt de aanwezige extra sql code.
        """
        ddl = Ddl(self, self.sor)
        for sql in self.extra_ddl_statements:
            if sql[0]:
                ddl.execute(sql[1], "EXTRA SQL STATEMENT")

    def run(self, parts = ['sor', 'valuesets', 'hubs', 'links', 'views', 'viewlinks']):
        """
        :param parts: lijst van keywords. De eventuele aanwezigheid van een keyword in deze lijst bepaald of het log bestand dat hoort bij dit keyword gemaakt wordt en of de bijhorende DDL uitgevoerd wordt.

        """

        if 'sor' in parts:
            #SOR
            etl = EtlSourceToSor(self)
            self.pipeline.logger.log('START FROM SOURCE TO SOR', indent_level=1)

            for mapping in self.mappings:
                if isinstance(mapping, SourceToSorMapping):
                    self.pipeline.logger.log('START <blue>{}</>'.format(mapping), indent_level=3)
                    etl.source_to_sor(mapping)
                    etl.validate_duplicate_keys(mapping, self.sor)
                    self.pipeline.logger.log('FINISH <blue>{}</>'.format(mapping), indent_level=3)
            for validation in self.validations:
                if isinstance(validation, SorValidation):
                    etl.validate_sor(validation)

            for func in self.run_after_sor:
                func(self)

            self.pipeline.logger.log('FINISH FROM SOURCE TO SOR', newline=True, indent_level=1)

        #DV
        ddl = DdlDv(self)
        etl = EtlSorToDv(self)
        if 'valuesets' in parts:
            self.pipeline.logger.log('START FROM SOR TO VALUESETS', indent_level=1)
            #DV refs
            for mapping in self.mappings:
                if isinstance(mapping, SorToValueSetMapping):
                    etl.sor_to_valuesets(mapping)
            self.pipeline.logger.log('FINISH FROM SOR TO REFS', newline=True, indent_level=1)

        # DV Entities (Hubs en Sats)
        if 'hubs' in parts:
            self.pipeline.logger.log('START FROM SOR TO HUBS', indent_level=1)
            for mapping in self.mappings:
                if type(mapping) == SorToEntityMapping:
                    DdlSor(self).try_add_fk_sor_hub(mapping)
                    etl.sor_to_entity(mapping)
            for validation in self.validations:
                if isinstance(validation, DvValidation):
                    etl.validate_dv(validation)
            self.pipeline.logger.log('FINISH FROM SOR TO HUBS', newline=True, indent_level=1)

        if 'views' in parts:
            self.pipeline.logger.log('START FROM HUBS TO HUBS', indent_level=1)
            for mapping in self.mappings:
                if isinstance(mapping, EntityViewToEntityMapping):
                    ddl.create_or_alter_entity(mapping)
                    ddl.create_or_alter_view(mapping)
                    etl.view_to_entity(mapping)
            self.pipeline.logger.log('FINISH FROM HUBS TO HUBS', newline=True, indent_level=1)

        #DV Links
        if 'links' in parts:
            self.pipeline.logger.log('START FROM SOR TO LINKS', indent_level=1)
            for mapping in self.mappings:
                if type(mapping) == SorToLinkMapping:
                    DdlSor(self).try_add_fk_sor_link(mapping)
                    etl.sor_to_link(mapping)
            self.pipeline.logger.log('FINISH FROM SOR TO LINKS', newline=True, indent_level=1)

        if 'viewlinks' in parts:
            self.pipeline.logger.log('START FROM HUBS TO LINKS', indent_level=1)
            for mapping in self.mappings:
                if isinstance(mapping, EntityViewToLinkMapping):
                    ddl.create_or_alter_link(mapping)
                    etl.view_to_link(mapping)
            self.pipeline.logger.log('FINISH FROM HUBS TO LINKS', newline=True, indent_level=1)

        for mapping in self.mappings:
            if isinstance(mapping, SourceToSorMapping):
                etl.copy_to_exceptions_table(mapping.sor_table, self.sor)

        #delete /tmp/datatranfser
        if self.source_db:
            path = self.source_db.get_or_create_datatransfer_path()


    def validate(self):
        """
        Voert achtereenvolgens uit: :class:'validate_domains' en validate_mappings. Geneereert de validatie-boodschap.

        :return: validation_msg. leeg indien validatie goed is
        """
        validation_msg = self.validate_domains()
        validation_msg += self.validate_mappings_after_ddl()
        return validation_msg

    def validate_mappings_before_ddl(self):
        """
        Valideert of de gebruikte mappings correcte domein classes gebruikt (entities, hubs, sats en links).

        zie: :class:`Pipeline.validate_mappings`

        :return: validation_msg
        """
        validator = MappingsValidator()
        validation_msg = validator.validate_before_ddl(self.mappings)
        return validation_msg

    def validate_mappings_after_ddl(self):
        """
        Valideert of de gebruikte mappings correcte domein classes gebruikt (entities, hubs, sats en links).

        zie: :class:`Pipeline.validate_mappings`

        :return: validation_msg
        """
        validator = MappingsValidator()
        validation_msg = validator.validate_after_ddl(self.mappings)
        return validation_msg



