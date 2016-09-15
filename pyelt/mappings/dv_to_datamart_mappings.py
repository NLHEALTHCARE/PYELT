from typing import Union, List, Dict

from sample_domains.test_domain import Patient, Traject
from pyelt.datalayers.database import Column, Table, Database, View, Schema
from pyelt.datalayers.dv import DvEntity, Link, HybridSat, LinkReference, Sat, DynamicLinkReference
from pyelt.datalayers.dwh import Dwh
from pyelt.datalayers.sor import SorTable
from pyelt.helpers.exceptions import PyeltException
from pyelt.mappings.base import BaseTableMapping, FieldMapping, ConstantValue
from pyelt.mappings.transformations import FieldTransformation


# backup gemaakt op 21062016: "sor_to_dv_mappings_old21062016.py"

class EntityToDimMapping(BaseTableMapping):
    pass


class Fact():
    def __init__(self, name):
        self.dims = []
        self.cols = []


class Dim():
    def __init__(self, name):
        self.cols = []


def test():
    fact_inschrijving = Fact('inschrijving')
    fact_inschrijving.dims.append(Dim('specialisatie'))
    fact_inschrijving.dims.append(Dim('organisatie'))
    fact_inschrijving.dims.append(Dim('dag_maand'))
    fact_inschrijving.fact = 'aantal'
    mapping = EntityToDimMapping(Traject, fact_inschrijving)
    mapping.map_fact_field('Count', fact_inschrijving.fact)
    mapping.map_dim(Traject.Default.start, fact_inschrijving['dag_maand'].cols['datum'])
