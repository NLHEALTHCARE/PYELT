PYELT
====


Documentation
^^^^^^^^^^^^

Pyelt is a Python DDL and ETL framwork for creating and filling Data Vault - Data Ware Houses.

Pyelt supports more data-layers (sor (staging), raw datavault, business datavault, datamart) 

Pyelt can import data from several different source systems such as fixed length files, csv-files, and different databases.

Pyelt is developed to run on a postgres database.

Pyelt uses SQLAlchemy.core only for connection and reflection. All other SQL statements (ddl, copy, insert and update statements) are created by the pyelt framework itself

Write your own mappings to transfer and transform data from sources into staging into the data ware house.

Content:

- 'concepts </docs/source/00concepts.rst>'_
- config](docs/source/01config.rst)
- [ ] [pipeline](docs/source/02pipeline.rst)
- [ ] [domein](docs/source/03domain.rst)
- [ ] [mappings](docs/source/03mappings.rst)
- [ ] [etl_proces](docs/source/04etl_proces.rst)

Nog doen:
- [ ] [api](docs/source/09api.rst)


Background
^^^^^^^^^^
Pyelt was developed as a framework to support the realisation of datawarehouse 2.0 from NL Healthcare Clinics.
This project is based on::
- 'Data Vault design pattern <https://hanshultgren.wordpress.com/>'_ of Hans Hultgren
- [ ] Domain design with (inter)national standards, mostly [HL7 RIM](https://www.hl7.org/documentcenter/public_temp_0BB49CB1-1C23-BA17-0C2E211163D07382/calendarofevents/himss/2009/presentations/Reference%20Information%20Model_Tue.pdf) en
[Zorginformatiebouwstenen](https://zibs.nl)
- [ ] Een op Python 3 en PostgreSQL gebaseerde ETL library (pyelt), waarmee de datavault en het domein model geimplementeerd worden
- [ ] [sqlalchemy](http://www.sqlalchemy.org/) Alleen de core modules worden gebruikt, niet de orm modules

