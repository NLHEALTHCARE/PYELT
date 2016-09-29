PYELT
=====


Usage
^^^^^

This example will create and fill the historical staging area::

    pipeline = Pipeline(config)
    pipe = pipeline.get_or_create_pipe('test_source', source_config)

    source_file = CsvFile(get_root_path() + '/sample_data/patienten1.csv', delimiter=';')
    source_file.reflect()
    source_file.set_primary_key(['patientnummer'])
    mapping = SourceToSorMapping(source_file, 'persoon_hstage', auto_map=True)
    pipe.mappings.append(mapping)

    pipeline.run()
    
More examples can be found on `the GitHub repository of NL Healthcare <https://github.com/NLHEALTHCARE/PYELT/tree/master/samples//>`_.


Introduction
^^^^^^^^^^^^

Pyelt is a Python DDL and ETL framework for **creating and loading Data Vaults** for datawarehousing.

Pyelt supports **several data-layers**, including Source-of-Record (SOR), Raw datavault (RDV), Business datavault (BDV) and Datamarts (DM) 

Pyelt can import data from several **different source systems** such as fixed length files, csv-files, and different databases.

Pyelt is developed to run on a **postgreSQL database**.

Pyelt uses the SQLAlchemy.core only for the connection and for reflection. All other SQL statements (ddl, copy, insert and update statements) are created by the pyelt framework itself.

**Write your own mappings** to transfer and transform data from sources via staging into the data ware house.

Content 
----------------------------

(`current documentation on pythonhosted <http://pythonhosted.org/pyelt//>`_ is only in dutch):

- `concepts <https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/00concepts.rst/>`_
- `config <https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/01config.rst/>`_
- `pipeline <https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/02pipeline.rst/>`_
- `domain <https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/03domain.rst/>`_
- `mappings <https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/03mappings.rst/>`_
- `run proces <https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/04etl_proces.rst/>`_

work in progress:

- api docs (https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/09api.rst/>_



Background
^^^^^^^^^^
The pyelt framework is presently under development at NL Healthcare, with the aim to implement our next-generation datawarehouse (DWH2.0). It serves as the foundation for our work in the area of clinical business intelligence (CBI) and machine-learning.

Architectural cornerstones of this project are:

- the Data Vault (DV) design pattern of `Hans Hultgren <https://hanshultgren.wordpress.com/>`_ 
- Domain-specific modelling of the DV, following `HL7 v3 Reference Information Model <https://www.hl7.org/documentcenter/public_temp_0BB49CB1-1C23-BA17-0C2E211163D07382/calendarofevents/himss/2009/presentations/Reference%20Information%20Model_Tue.pdf/>`_ and the Dutch Detailed Clinical Model `Zorginformatiebouwstenen <https://zibs.nl/>`_ (in Dutch).
