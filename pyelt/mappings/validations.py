from typing import Union

from pyelt.datalayers.database import Schema, Table, Column, Condition



class Validation():
    class Operators:
        equals = '='
        not_equals = '!='
        gt = '>'
        lt = '<'
        like = 'like'
        not_like = 'not like'

    def __init__(self, tbl: 'Table' = None, schema: 'Schema' = None) -> None:
        if isinstance(tbl, str):
            tbl = Table(tbl, schema)
        self.tbl = tbl  # type: Table
        self.msg = ''  # type: str
        self.sql_condition = ''  # type: str


    def __str__(self):
        return self.sql_condition

    def report_run(self, pipeline):
        runid = pipeline.runid
        sql = "SELECT schema, table_name, message, key_fields, fields FROM dv._exceptions WHERE _runid = {0} ORDER BY schema;".format(runid)
        rows = pipeline.dwh.execute_read(sql)
        msg = ''
        if rows:
            pipeline.logger.log_simple('<red>ROW VALIDATION ERRORS DURING ETL</>')
            pipeline.logger.log_simple('<red>(validation errors at sor-layers are not imported in dv-layer!)</>')
        for row in rows:
            msg = 'schema: <blue>{}</>, table : <blue>{}</>, message: <blue>{}</>, key: <blue>{}</>, row: <blue>{}</>'.format(row[0], row[1], row[2], row[3], row[4])
            pipeline.logger.log_simple(msg)
            # msg += 'VALIDATION ERRORS DURING ETL: FOUTEN: ' + str(row) + '<br>'
        return ''




class SorValidation(Validation):
    def __init__(self, tbl: 'Table' = None, schema: 'Schema' = None) -> None:
        super().__init__(tbl, schema)
        self.check_for_duplicate_keys = []


    def set_check_for_duplicate_keys(self, keys):
        if isinstance(keys, str):
            self.check_for_duplicate_keys = [keys]
        else:
            self.check_for_duplicate_keys = keys

    def get_keys(self):
        keys = ','.join(self.check_for_duplicate_keys)
        return keys



class DvValidation(Validation):
    def set_condition(self, field: Union[Column, str], operator: str = '=', sql_value="''"):
        if isinstance(field, str):
            self.sql_condition = field
        elif isinstance(field, Condition):
            self.sql_condition = field.name
            self.table = field.table
        else:
            params = {'field': field.name, 'operator': operator, 'sql_value': sql_value}
            self.table = field.table
            if field.name == 'bk':
                pass
            self.sql_condition = "{field} {operator} {sql_value}".format(**params)
