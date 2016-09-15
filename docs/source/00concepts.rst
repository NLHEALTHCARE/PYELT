Concepts of DWH 2.0 & Pyelt
===========================

Layered composition
---------------

DWH2.0 is built in layers. The data moves several times from one database schema to another. We can distinguish the
following layers:

Sources:
    Source systems; can comprise of databases, but also files such as for instance csv files.

Sor:
    This is the staging area. The temp and the historical staging tables are located here. It is strongly advised to
    create a seperate sor database (schema) for each individual source-system.

Rdv:
    Raw data vault. Some data will first move from the sor layer to the rdv. This occurs when modifications are
    needed (for example certain keys) to correctly integrate the data in the next layer.


Dv:
    Datavault. The actual datavault with hubs, sats and links.

Datamart:
    Datamart with reports.


Concepts
---------

Pipeline:
    The pipeline consists of the entire etl of all source systems and all database layers. A pipeline consists of 1 or
    more pipes.


Pipe:
    #todo: afbeelding toevoegen?
    A pipe exists for each individual source. A pipe consists of a source layer, a sor layer, a part of the rdv
    and the dv tables, respectively.
    In the image above a pipe is circled in orange.



Datavault concepts
-------------------

Hub:
    A table with a business key (bk), system fields (for example: "insert_date"), and a technical key.

BK:
    Business key. A business key is unchangeable, is human_readable and is used in each department of a company.

Sat:
    A satelite table. This table is linked to a hub and contains every alteration in the data a in a row. The key is
    composed of the technical key of the hub and the daily runid.

Link:
    A link between 2 or more hubs.

Hybrid Sat:
    A sat with an extra type field in the key (for example: telephonenumbers of a person).

Hybrid links:
    A link with an extra type field


Hierarchical Links:
    A link that connects a hub with itself.


Entity:
    Collection of 1 hub with its sats. (This is not a table)


Ensemble:
    Collection of multiple entities with corresponding links.

Domainmodels
------------

Representation of the datavault in python classes. The hubs, sats and links are first defined in python and then the
datavault tables a created according to those definitions.


Mappings
--------

TableMapping:
    Mapping of a source table or sourcer file on a target table.


FieldMapping:
    Mapping of a source field or source column on a target column.


Transformation:
    Mapping from a source to a target field where the source first will be modified.


BkMapping:
    #todo:

LinkMapping:
    #todo:

SatMapping:
    #todo:





