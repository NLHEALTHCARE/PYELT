from sqlalchemy.engine import reflection

from pyelt.datalayers.database import Table, Schema, Column


class Sor(Schema):
    pass


class SorTable(Table):
    def __init__(self, name: str, schema: 'Schema' = None) -> None:
        super().__init__(name, schema)


class SorQuery(Table):
    def __init__(self, sql: str, schema: 'Schema' = None, name: str = '') -> None:
        super().__init__(name, schema)
        self.sql = sql
        self.main_table = ''

    def get_main_table(self):
        if not self.main_table:
            #parse. We nemen de eerste tabel in de FROM als de hoofdtabel. Deze hoofdtabel krijgt een fk naar de hub
            sql = self.sql.upper().replace('  ', ' ')
            pos_start = sql.index(' FROM ') + len(' FROM ')
            pos_end = sql.index(' ', pos_start)
            main_table = sql[pos_start:pos_end].strip().lower()
            if '.' in main_table:
                main_table = main_table.split('.')[1]
            self.main_table = main_table
            self.name = main_table
        return self.main_table

    def reflect(self):
        self.columns = []
        cursor = self.db.engine.raw_connection().cursor()

        sql = self.sql + ' OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY'

        result = self.db.engine.execute(sql)
        cursor = result.cursor
        for sa_col in cursor.description:
            col = Column(sa_col.name, sa_col.type_code)
            self.columns.append(col)
        self.is_reflected = True

    def reflect_2(self) -> None:
        self.columns = []  # type: List[Column]
        inspector = reflection.Inspector.from_engine(self.db.engine)
        columns = inspector.cls_get_columns(self.name, self.schema.name)
        pks = inspector.get_primary_keys(self.name, self.schema.name)
        indexes = inspector.get_indexes(self.name, self.schema.name)
        self.key_names = []
        for col in columns:
            # col = Column(col['name'], str(col['type']), self)
            col = Column(col['name'], col['type'], self)
            col.is_key = (col.name in pks)
            if col.is_key:
                self.key_names.append(col.name)
            # if self.key_names:
            #     col.is_key = (col.name in self.key_names)
            self.columns.append(col)
        for index in indexes:
            self.indexes[index['name']] = index
        self.is_reflected = True

        #     self.name = name
        #
        # def __str__(self):
        #     return self.name
