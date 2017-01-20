from pyelt.datalayers.database import Columns
from pyelt.datalayers.dv import AbstractOrderderTable


class Sys():
    class Runs(AbstractOrderderTable):
        __dbschema__ = 'sys'
        runid = Columns.FloatColumn(pk=True)
        rundate = Columns.DateTimeColumn(nullable=False)
        finish_date = Columns.DateTimeColumn(indexed=True)
        exceptions = Columns.BoolColumn()
        sor_versions = Columns.TextColumn()
        dv_version = Columns.FloatColumn()
        pyelt_version = Columns.TextColumn()

    class Currentversion(AbstractOrderderTable):
        __dbschema__ = 'sys'
        schemaname = Columns.TextColumn(pk=True,unique=True)
        version = Columns.FloatColumn(unique=True)
        date = Columns.DateTimeColumn()
