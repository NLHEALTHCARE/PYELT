Welkom bij de pyelt documentatie!
=================================
Pyelt is een Python ETL tool voor het creeeren en vullen van een Data Vault- Data Ware House.

Pyelt is geschikt voor data vaults die bestaan uit meerdere lagen (sor, raw, business, datamart) gevuld vanuit meerdere bronsystemen.

Bronsystemen kunnen andere databases zijn, maar ook files zoals csv en fixed-length.

Pyelt is gebaseerd op de postgres database.
Pyelt maakt gebruikt van SQLAlchemy.core voor connection en reflection. SQL statements worden echter in python code opgebouwd.

Inhoud:

.. toctree::
    :maxdepth: 2

    00concepts
    01config
    02pipeline
    03domain
    03mappings
    04etl_proces

    98blablabl

    99api

verder
------

- logging
- runs
- bronlagen: file of database
- sor, rdv en dv lagen
- datamarts



Indexes en Zoeken
=================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

