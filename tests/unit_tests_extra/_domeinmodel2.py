from tests.unit_tests_extra._domeinmodel import *
from pyelt.datalayers.database import Columns, Column
from pyelt.datalayers.dv import DvEntity, Sat, HybridSat, Link, LinkReference


class DiagnoseStellen(Handeling):
    class Diagnose(Sat):
        diagnosecode = Columns.TextColumn()
        diagnosenaam = Columns.TextColumn('diagnaam')
