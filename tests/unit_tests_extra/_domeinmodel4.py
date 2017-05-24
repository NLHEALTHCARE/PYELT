from tests.unit_tests_extra._domeinmodel import *
from pyelt.datalayers.database import Columns, Column
from pyelt.datalayers.dv import HubEntity, Sat, HybridSat, Link, LinkReference


class DiagnoseStellen(Handeling):
    class Diagnose(Sat):
        diagnosecode = Columns.TextColumn()
        diagnosenaam = Columns.TextColumn('diagnaam')
        extraveld = Columns.TextColumn()

    class Extra(Sat):
        nog_een_veld = Columns.IntColumn()
        camelCaseVeld = Columns.TextColumn()
        goed = Columns.TextColumn('dit_')
