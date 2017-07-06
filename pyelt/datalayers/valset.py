from typing import Dict

from pyelt.datalayers.database import Columns, Column
from pyelt.datalayers.dv import AbstractOrderderTable


class DvValueset(AbstractOrderderTable):
    __dbschema__ = 'valset'
    _id = Columns.SerialColumn()
    _runid = Columns.FloatColumn(unique=True, indexed=True)
    _source_system = Columns.TextColumn()
    _valid = Columns.BoolColumn()
    _validation_msg = Columns.TextColumn()
    _insert_date = Columns.DateTimeColumn()
    _finish_date = Columns.DateTimeColumn()
    _deleted_runid = Columns.FloatColumn()
    _active = Columns.BoolColumn(default_value=True, indexed=True)
    _revision = Columns.IntColumn()
    valueset_naam = Columns.TextColumn(unique=True, indexed=True, default_value="''")
    code = Columns.TextColumn(unique=True, indexed=True)
    omschrijving = Columns.TextColumn()

class DvPeriodicalValueset(DvValueset):
    ingangsdatum = Columns.DateColumn(unique=True, indexed=True)
    einddatum = Columns.DateColumn(indexed=True)

