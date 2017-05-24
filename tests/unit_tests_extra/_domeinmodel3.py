from tests.unit_tests_extra._domeinmodel import *
from pyelt.datalayers.database import Columns, Column
from pyelt.datalayers.dv import HubEntity, Sat, HybridSat, Link, LinkReference


class ColumnTypes(HubEntity):
    class Default(Sat):
        text = Columns.TextColumn()
        date = Columns.DateColumn()
        datetime = Columns.DateTimeColumn()
        int = Columns.IntColumn()
        float = Columns.FloatColumn()
        ref = Columns.RefColumn('types')
