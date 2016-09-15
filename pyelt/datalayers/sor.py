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
        columns = inspector.get_columns(self.name, self.schema.name)
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
