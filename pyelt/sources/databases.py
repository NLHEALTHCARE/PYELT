import csv
import os
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import reflection

from pyelt.datalayers.database import Database, Schema, Table, Column, DBDrivers


# from etl_mappings.general_configs import config as pyelt_config




def oracle_type_to_postgres_type(type: str) -> str:
    type = type.lower().strip()
    if 'string' in type:
        return 'text'
    elif 'varchar' in type:
        return 'text'
    elif 'number' in type:
        return 'numeric'
        # todo integers
    elif 'datetime' in type:
        return 'timestamp'
    else:
        return type


def sqlserver_type_to_postgres_type(type: str) -> str:
    type = type.lower().strip()
    # todo aanvullen
    if type == '1':
        return 'text'
    elif type == '3':
        return 'integer'
    else:
        return 'text'



class SourceDatabase(Database):
    def __init__(self, conn_string='', default_schema='public', temp_datatransfer_path=''):
        super().__init__(conn_string, default_schema)
        self.driver = self.get_driver_from_conn_string(conn_string)
        self.user_name = self.get_user_name_from_conn_string(conn_string)
        self.temp_datatransfer_path = temp_datatransfer_path


    def get_driver_from_conn_string(self, conn_string):
        driver = DBDrivers.POSTGRESS
        if conn_string.startswith('ora'):
            driver = DBDrivers.ORACLE
        elif conn_string.startswith('mssql'):
            driver = DBDrivers.SQLSERVER
        elif conn_string.startswith('postgres'):
            driver = DBDrivers.POSTGRESS
        elif conn_string.startswith('mysql'):
            driver = DBDrivers.MYSQL
        return driver

    def get_user_name_from_conn_string(self, conn_string):
        user_name = conn_string.split(':')[1]
        user_name = user_name.replace('/', '')
        return user_name

    def get_or_create_datatransfer_path(self):
        # path = pyelt_config['datatransfer_path']
        path = self.temp_datatransfer_path
        if not path:
            from main import get_root_path
            path = get_root_path() + '/data/transfer/'
        path += '/' + self.name
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def execute_read(self, sql, log_message=''):
        if self.driver == DBDrivers.POSTGRESS:
            return super().execute_read(sql, log_message)
        connection = self.engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result


class SourceSchema(Schema):
    def __init__(self, name, db):
        super().__init__(name, db)

class SourceTable(Table):
    def __init__(self, name, schema, db, alias=''):
        super().__init__(name, schema, db)
        self.alias = alias

    def to_csv(self, path='', md5_only=False, filter='', ignore_fields=[], debug=False):
        path = self.db.get_or_create_datatransfer_path()
        if not self.alias:
            self.alias = self.name
        file_name = '{}/{}.csv'.format(path, self.alias)
        if md5_only:
            file_name = '{}/{}_hash.csv'.format(path, self.name)
            if 'view_pers_hash' in file_name:
                debug = True

        head = [col.name for col in self.columns if col.name not in ignore_fields]
        if md5_only:
            head = self.key_names + ['_hash']
        with open(file_name, 'w', newline='', encoding='utf8') as fp:
            csv_writer = csv.writer(fp, delimiter=';')
            csv_writer.writerow(head)
            data = self.load(md5_only=md5_only, filter=filter, ignore_fields=ignore_fields, debug=debug)
            csv_writer.writerows(data)
        return file_name

    def load(self, md5_only=False, filter='', ignore_fields=[], debug=False):
        rows = []
        field_names = [col.name for col in self.columns if col.name not in ignore_fields]
        field_names_str = ','.join(field_names)

        if md5_only:
            key_sql = ','.join(self.primary_keys())
            concat_fields_sql = '||'.join(field_names)
            if self.db.driver == DBDrivers.ORACLE:
                # sql = """SELECT {2}, RAWTOHEX(UTL_RAW.CAST_TO_RAW(sys.dbms_obfuscation_toolkit.md5(INPUT_STRING => {3}))) as hash
                # FROM {0}.{1} """.format(self.schema.name, self.name, key_sql, concat_fields_sql)
                field_names_str = """{0}, RAWTOHEX(UTL_RAW.CAST_TO_RAW(sys.dbms_obfuscation_toolkit.md5(INPUT_STRING => {1}))) as hash""".format(key_sql, concat_fields_sql)
            elif self.db.driver == DBDrivers.POSTGRESS:
                concat_fields_sql = ''
                for field in field_names:
                    concat_fields_sql += "coalesce({}::text, '')".format(field) + '||'
                concat_fields_sql = concat_fields_sql[:-2]
                # sql = """SELECT {2}, md5({3}) as hash
                # FROM {0}.{1} """.format(self.schema.name, self.name, key_sql, concat_fields_sql)
                # elif self.db.driver == DBDrivers.SQLSERVER:
                #     concat_fields_sql = ','.join(self.field_names())
                #     sql = """SELECT {2}, CONVERT(NVARCHAR(32), HashBytes('MD5', CONCAT({3})), 2) as hash
                #     FROM {0}.{1} """.format(self.schema.name, self.name, key_sql, concat_fields_sql)
                field_names_str = """{0}, MD5({1}) as hash""".format(key_sql, concat_fields_sql)

            elif self.db.driver == DBDrivers.SQLSERVER:
                concat_fields_sql = ''
                for field in self.field_names():
                    concat_fields_sql += "rtrim(coalesce(convert(varchar(max),  {}".format(field) + "), ''))+"
                concat_fields_sql = concat_fields_sql[:-1]
                # sql = """SELECT {2}, CONVERT(NVARCHAR(32), HashBytes('MD5', {3} ), 2) as hash
                #     FROM {0}.{1} """.format(self.schema.name, self.name, key_sql, concat_fields_sql)
                field_names_str = """{0}, CONVERT(NVARCHAR(32), HashBytes('MD5', {1}), 2) as hash""".format(key_sql, concat_fields_sql)

        if isinstance(self, SourceQuery):
            sql = """SELECT {} FROM ({}) AS {}""".format(field_names_str, self.sql, self.name)
            if self.db.driver == DBDrivers.ORACLE:
                sql = """SELECT {} FROM ({}) {}""".format(field_names_str, self.sql, self.name)
        else:
            sql = """SELECT {} FROM {}.{}""".format(field_names_str, self.schema.name, self.name)

        if filter:
            filter = filter.replace('WHERE', '')
            sql += ' WHERE ' + filter
        # print(sql)
        if debug:
            if self.db.driver == DBDrivers.SQLSERVER:
                sql = sql.replace('SELECT', 'SELECT TOP 100')
            else:
                sql += " OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY"

        result = self.db.engine.execute(sql)
        for row in result:
            rows.append(row)
        return rows


class SourceQuery(SourceTable):
    def __init__(self, db, sql, name):
        super().__init__(name, '', db)
        self.alias = name
        self.db = db
        if sql.strip().endswith(';'):
            sql = sql.strip()[:-1]
        self.sql = sql
        # self.name = 'query'
        self.columns = []


    def reflect(self):
        self.columns = []
        cursor = self.db.engine.raw_connection().cursor()
        if self.db.driver == DBDrivers.ORACLE:
            sql = self.sql + ' OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY'
        elif self.db.driver == DBDrivers.SQLSERVER:
            sql = self.sql_to_top_select(self.sql, 1)
        elif self.db.driver == DBDrivers.POSTGRESS:
            sql = self.sql + ' OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY'
        # sql = self.sql + ' AND 1=0'
        # cursor.execute(sql)

        result = self.db.engine.execute(sql)
        cursor = result.cursor
        for sa_col in cursor.description:
            if self.db.driver == DBDrivers.ORACLE:
                col = Column(sa_col[0], oracle_type_to_postgres_type(str(sa_col[1])))
            elif self.db.driver == DBDrivers.SQLSERVER:
                col = Column(sa_col[0], sqlserver_type_to_postgres_type(str(sa_col[1])))
            else:
                col = Column(sa_col.name, sa_col.type_code)
            self.columns.append(col)
        self.is_reflected = True

    def load2(self, md5_only=False, filter='', ignore_fields=[], debug=False):
        rows = []
        sql = self.sql

        if filter:
            if 'where' in sql.lower():
                sql += ' AND ' + filter
            else:
                sql += ' WHERE ' + filter

        # if 'debug' in pyelt_config and pyelt_config['debug']:
        if debug:
            if self.db.driver == DBDrivers.SQLSERVER:
                sql = self.sql_to_top_select(self.sql, 100)
            else:
                sql += " OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY"

        result = self.db.engine.execute(sql)
        for row in result:
            rows.append(row)
        return rows

    def sql_to_top_select(self, sql, top=1):
        "Voor SQL SERVER"
        # eerst mooie string van maken
        sql = sql.replace('\t', ' ')
        sql = sql.replace('\n', ' ')
        sql = sql.replace('\r', ' ')
        sql = sql.replace('  ', ' ')
        sql = sql.strip().lower()

        if sql.startswith('select distinct top') or sql.startswith('select top'):
            # als er al aan het begin een top instaat: niets doen. Wees je bewust van subqueries
            pass
        elif sql.startswith('select distinct'):
            # allern begin van sql vervangen, wees je bewust van subqueries
            sql = 'select distinct top ' + str(top) + sql[15:]
        elif sql.startswith('select'):
            sql = 'select top ' + str(top) + sql[6:]

        return sql
