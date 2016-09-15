from pyelt.datalayers.dwh import Dwh

__author__ = 'hvreenen'

# class FieldTransformationType():
#     INLINE = 'INLINE' #type: str
#     FUNCTION = 'FUNCTION'#type: str
#
# class FunctionLanguage():
#     PLSQL = 'PLSQL'#type: str
#     PLPYTHON = 'PLPYTHON'#type: str

class FieldTransformation():
    def __init__(self, name: str = '', sql: str = '') -> None:
        self.name = name #type: str
        self.field_name = 'id' #type: str
        # self.table_alias = ''
        self.descr = '' #type: str
        self.filter = '' #type: str
        # self.type = FieldTransformationType.INLINE #type: str
        self.steps = {} #type: Dict[int, FieldTransformStep]
        if sql:
            # self.parse_sql(sql)
            step = self.new_step(sql)


    def parse_sql(self, sql: str):
        pos_start = sql.find('(')
        pos_end = sql.rfind(')')
        func_name = sql[:pos_start]
        func_inner = sql[pos_start + 1:pos_end]
        step = self.new_step(func_inner)
        self.steps[step.sort_order] = step

    def new_step(self, sql: str) -> 'FieldTransformStep':
        step = FieldTransformStep(sql=sql)
        step.sort_order = len(self.steps) + 1
        self.steps[step.sort_order] = step
        return step

    def get_sql(self, alias: str='')->str:
        sql = ''
        index = 0
        steps = sorted(self.steps.values(), key = lambda x: x.sort_order)
        for step in steps:
            # step_sql = step.sql
            step_sql = step.get_sql(alias)
            step_sql = step_sql.replace(self.field_name, "{fld}")
            if (index > 0):
                if '{fld}' in step_sql:
                    sql = step_sql.replace("{fld}", sql)
                else:
                    sql = step_sql.replace("{step" + str(index) + "}", sql)
            else:
                 sql = step_sql
                 sql = sql.replace("{fld}", self.field_name)
            index += 1
        return sql

    def __repr__(self):
        return self.get_sql('')


# def create_function_at_db(self, dwh: 'Dwh') -> None:
#         #todo afmaken
#         params = {} #type: Dict[str, str]
#         sql = """CREATE OR REPLACE FUNCTION {schema}.{name}({params})
#   RETURNS {return_type} AS
# $BODY$
# {body}
# $BODY$
#   LANGUAGE {lang} VOLATILE;""".format(**params)
#         dwh.execute(sql)

class FieldTransformStep(FieldTransformation):
    def __init__(self, sortorder: int = 0, name: str = '', sql: str = '') -> None:
        FieldTransformation.__init__(self, name)
        self.sql = sql
        self.sort_order = sortorder
        # self.parse_sql(sql)

    def parse_sql(self, sql: str) -> None:
        func_name = ''
        func_params = []
        pos_start = sql.find('(')
        pos_end = sql.rfind(')')
        func_name = sql[:pos_start]
        func_params_sql = sql[pos_start + 1:pos_end]
        func_param_names = func_params_sql.split(',')
        for func_param_name in func_param_names:
            if not func_param_name: continue
            func_param = FuncParam(func_param_name.strip())
            func_params.append(func_param)
        self.func_name = func_name
        self.func_params = func_params

    def get_sql(self, alias: str='') -> str:
        return self.sql
        # func_params_sql = ''
        # for func_param in self.func_params:
        #     if func_param.is_db_field and alias:
        #         func_params_sql += '{}.{}, '.format(alias, func_param)
        #     else:
        #         func_params_sql += '{}, '.format(func_param)
        # func_params_sql = func_params_sql[:-2]
        # sql = "{}({})".format(self.func_name, func_params_sql)
        # return sql

# class FuncParam():
#     def __init__(self, name: str = '') -> None:
        #         self.name = name #type: str
        #         self.is_db_field = "'" not in name #type: bool
        #         if self.is_digit(name):
        #             self.is_db_field = False
        #
        #
        #     def __str__(self) -> str:
        #         return self.name
        #
        #     def is_digit(self, s: str) -> bool:
        #         try:
        #             f = float(s)
        #             return True
        #         except:
        #             return False


class Lookup(FieldTransformation):
    def __init__(self, name, dict={}, sor=None):
        super().__init__(name=name)
        self.new_step("(select ref_code_doel from {}.{} where ref_code_doel = '{}')".format(sor, name, '{fld}'))

    def get_ddl(self):
        sql = """
        CREATE TABLE {}.{}_ref_mappings
        (ref_code_bron text,
        ref_code_doel text)
        """.format(self.name)

    def get_etl(self):
        values = ''
        for code, descr in self.dict.items():
            values += "('{}', '{}'),\r\n".format(code, descr)
        values = values[:-3]
        params = {}
        params['values'] = values
        sql = """

            CREATE TEMP TABLE {sor}.{name}_ref_mappings_temp
        (ref_code_bron text,
        ref_code_doel text);

            INSERT INTO {sor}.{name}_ref_mappings_temp (ref_code_bron, ref_code_doel)
            VALUES {values};

            INSERT INTO {sor}.{name}_ref_mappings (ref_code_bron, ref_code_doel)
            SELECT ref_code_bron, ref_code_doel
            FROM {sor}.{name}_ref_mappings_temp
            WHERE NOT EXISTS (SELECT 1 FROM {sor}.{name}_ref_mappings maps WHERE maps.naam = '{ref_type}');

            DROP TABLE _ref_values_temp;
              """.format(**params)
