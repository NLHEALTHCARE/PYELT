# PYELT

Pyelt behelst de ontwikkeling van versie 2.0 van het datawarehouse van NL Healthcare (DWH2.0). Dit project is gebaseerd op:
- [ ] [Data Vault design pattern ](https://hanshultgren.wordpress.com/) van Hans Hultgren
- [ ] Een domein model op basis van (inter)nationale standaarden, met name [HL7 RIM](https://www.hl7.org/documentcenter/public_temp_0BB49CB1-1C23-BA17-0C2E211163D07382/calendarofevents/himss/2009/presentations/Reference%20Information%20Model_Tue.pdf) en
[Zorginformatiebouwstenen](https://zibs.nl)
- [ ] Een op Python 3 en PostgreSQL gebaseerde ETL library (pyelt), waarmee de datavault en het domein model geimplementeerd worden
- [ ] [sqlalchemy](http://www.sqlalchemy.org/) Alleen de core modules worden gebruikt, niet de orm modules


## Pyelt documentatie

Pyelt is een Python ETL tool voor het creeeren en vullen van een Data Vault- Data Ware House.

Pyelt is geschikt voor data vaults die bestaan uit meerdere lagen (sor, raw, business, datamart) gevuld vanuit meerdere bronsystemen.

Bronsystemen kunnen andere databases zijn, maar ook files zoals csv en fixed-length.

Pyelt is gebaseerd op de postgres database.

Pyelt maakt gebruikt van SQLAlchemy.core voor connection en reflection. SQL statements voor ddl en etl worden echter in python code opgebouwd.
Pyelt gebruikt hiervoor tabel- en veldmappings.

Inhoud:

- [ ] [config](docs/source/01config.rst)
- [ ] [pipeline](docs/source/02pipeline.rst)
- [ ] [domein](docs/source/03domain.rst)
- [ ] [mappings](docs/source/03mappings.rst)
- [ ] [etl_proces](docs/source/04etl_proces.rst)

Nog doen:
- [ ] [api](docs/source/09api.rst)


    

