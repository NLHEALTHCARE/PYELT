"""In deze module algeleide van database.py om orm deel uit te testen.
Woprt momenteel niet toegepast.
"""
from pyelt.datalayers.database import *


class TranactionalDatabase(Database):
    """Database met transactie mogelijkheid:

    voorbeeld::

        db.start_transaction()
        db.execute_without_commit('INSERT INTO ...')
        db.commit()
    """

    def start_transaction(self):
        connection = self.engine.raw_connection()
        self.__conn = connection
        self.__cursor = connection.cursor()

    def execute_without_commit(self, sql: str, log_message: str = ''):
        self.log('-- ' + log_message.upper())
        self.log(sql)

        start = time.time()
        if not self.__cursor:
            self.start_transaction()
        self.__cursor.execute(sql)

        self.log('-- duur: ' + str(time.time() - start) + '; aantal rijen:' + str(self.__cursor.rowcount))
        self.log('-- =============================================================')

    def commit(self, log_message: str = ''):
        # connection = self.engine.raw_connection()
        self.__conn.commit()
        self.__conn.close()

class ORMColumn(Column):

    def __eq__(self, other):
        # zet om in sql
        return Condition(self.name, '=', other, table=self.table)

    def __ne__(self, other):
        # zet om in sql
        return Condition(self.name, '!=', other, table=self.table)
        # return Condition('({} != {})'.format(self.name,other))

    def __gt__(self, other):
        # zet om in sql
        return Condition(self.name, '>', other, table=self.table)
        # return Condition('({} > {})'.format(self.name,other))

    def __ge__(self, other):
        # zet om in sql
        return Condition(self.name, '>=', other, table=self.table)
        # return Condition('({} >= {})'.format(self.name,other))

    def __lt__(self, other):
        # zet om in sql
        return Condition(self.name, '<', other, table=self.table)
        # return Condition('({} < {})'.format(self.name,other))

    def __le__(self, other):
        # zet om in sql
        return Condition(self.name, '<=', other, table=self.table)
        # return Condition('({} <= {})'.format(self.name,other))

    def is_in(self, item):

        return Condition(self.name, 'in', item, table=self.table)

    def between(self, item1, item2):
        item = '{} AND {}'.format(item1, item2)
        return Condition(self.name, 'between', item, table=self.table)

    def join(self, item):
        sql = self.name + '||' + item.name
        if isinstance(item, (list, tuple)):
            sql = self.name + '||' + '||'.join(item)
        return SqlSnippet(sql, table=self.table)

####################################
class Condition():
    def __init__(self, field_name='', operator='', value=None, table=None):
        self.table = table
        if operator:
            if isinstance(value, str):
                sql_condition = "{} {} '{}'".format(field_name, operator, value)
            elif isinstance(value, list):
                value = str(value)
                value = value.replace('[', '(').replace(']', ')')
                sql_condition = "{} {} {}".format(field_name, operator, value)
            else:
                sql_condition = "{} {} {}".format(field_name, operator, value)
        else:
            sql_condition = field_name
        sql_condition = sql_condition.replace(' = None', ' IS NULL')
        sql_condition = sql_condition.replace(' != None', ' IS NOT NULL')
        self.name = sql_condition

    def __and__(self, other):
        # zet om in sql
        if isinstance(other, Condition):
            return Condition('({} AND {})'.format(self.name, other.name), table=self.table)
        else:
            return Condition('({} AND {})'.format(self.name, other), table=self.table)

    def __or__(self, other):
        # zet om in sql
        if isinstance(other, Condition):
            return Condition('({} OR {})'.format(self.name, other.name), table=self.table)
        else:
            return Condition('({} OR {})'.format(self.name, other), table=self.table)

    def get_table(self):
        return self.table

class SqlSnippet():
    def __init__(self, sql='', table=None):
        self.table = table
        self.sql = sql

    def get_table(self):
        return self.table