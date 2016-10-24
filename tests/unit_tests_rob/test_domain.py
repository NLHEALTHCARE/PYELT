# from domainmodel_fhir.element_domain import CodeableConcept, Coding, Period, HumanName, ContactPoint, Address, \
#     Attachment, BackBoneElement
# from domainmodel_fhir.resource_domain import AbstractPerson
# from domainmodels.entity_domain import RefTypes  # todo: RefTypes in fhir_domain.py zelf definieren?
# from domainmodels.hl7rim_base_domain import *
from pyelt.datalayers.database import Columns
from pyelt.datalayers.dv import DvEntity, Sat, HybridSat, Link, LinkReference


""" voor testen van jsonb"""

####
class Entity(object):
    """HL7v3 RIM abstract base class"""
    # class Hl7(Sat):
    #     entity_class = Columns.TextColumn(default_value=EntityClass.entity)
    #     entity_determiner = Columns.TextColumn(default_value=EntityDeterminer.specific)
    # hl7_instance = Hl7()


class AbstractPerson:  # deze class komt uit resource_domain.py in domainmodel_fhir van de clinical_datavault
    class GenderTypes:
        male = 'male'
        female = 'female'
        other = 'other'
        unknown = 'unknown'
####


class Patient(DvEntity, Entity):   # FHIR type: DomainResource (http://hl7.org/fhir/domainresource.html#1.20)


    class Default(Sat):
        patient_nummer = Columns.TextColumn()  # dit staat niet zo in FHIR, hier geplaatst voor het testen.
        # active = Columns.BoolColumn()  # patient record active?
        # gender = Columns.RefColumn(RefTypes.geslacht_types)
        birthdate = Columns.DateColumn()
        gender = Columns.TextColumn(default_value=AbstractPerson.GenderTypes.unknown)

        deceased_boolean = Columns.BoolColumn()
        deceased_datetime = Columns.DateTimeColumn()
        extra = Columns.JsonColumn()
        extra2 = Columns.JsonColumn()

        multiple_birth_boolean = Columns.BoolColumn()
        multiple_birth_integer = Columns.IntColumn()


