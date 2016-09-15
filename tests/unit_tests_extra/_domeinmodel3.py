from tests.unit_test_extra._domeinmodel import *
from pyelt.datalayers.database import Columns, Column
from pyelt.datalayers.dv import DvEntity, Sat, HybridSat, Link, LinkReference


class ColumnTypes(DvEntity):
    class Default(Sat):
        text = Columns.TextColumn()
        date = Columns.DateColumn()
        datetime = Columns.DateTimeColumn()
        int = Columns.IntColumn()
        float = Columns.FloatColumn()
        ref = Columns.RefColumn('types')
