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
    
More examples can be found in https://github.com/NLHEALTHCARE/PYELT/tree/master/samples/


Introduction
^^^^^^^^^^^^

Pyelt is a Python DDL and ETL framework for **creating and filling Data Vault** - Data Ware Houses.

Pyelt supports **more data-layers** (sor (staging), raw datavault, business datavault, datamart) 

Pyelt can import data from several **different source systems** such as fixed length files, csv-files, and different databases.

Pyelt is developed to run on a **postgres database**.

Pyelt uses the SQLAlchemy.core only for the connection and for reflection. All other SQL statements (ddl, copy, insert and update statements) are created by the pyelt framework itself.

**Write your own mappings** to transfer and transform data from sources via staging into the data ware house.

Content 
----------------------------

(further documentation is only in dutch):

- concepts (https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/00concepts.rst)
- config (https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/01config.rst)
- pipeline(https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/02pipeline.rst)
- domain (https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/03domain.rst)
- mappings (https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/03mappings.rst)
- run proces (https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/04etl_proces.rst)

not yet:

- api docs (https://github.com/NLHEALTHCARE/PYELT/tree/master/docs/source/09api.rst)



Background
^^^^^^^^^^
Pyelt was developed as a framework to support the realisation of datawarehouse 2.0 from NL Healthcare Clinics.
This project is based on:

- the Data Vault design pattern (https://hanshultgren.wordpress.com/) of Hans Hultgren
- a Domain design according to (inter)national standards. The main part of this design is based on the international "HL7 RIM" standard (https://www.hl7.org/documentcenter/public_temp_0BB49CB1-1C23-BA17-0C2E211163D07382/calendarofevents/himss/2009/presentations/Reference%20Information%20Model_Tue.pdf) and the natioal  "Zorginformatiebouwstenen" standard(dutch; https://zibs.nl).