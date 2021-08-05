=================
Delta Connector (prototype)
=================

Overview
--------

The Delta connector allows querying data stored in a Delta tables.

Configuration
-------------

To configure the Delta connector, create a catalog properties file
``etc/catalog/delta.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=iceberg
    hive.metastore.uri=hostname:port
    presto-delta.location=/path/to/dir/above/delta/table
    presto-delta.table=delta-table-dir-name


Configuration Properties
------------------------

The following configuration properties are available:

======================= ==================================
Property Name           Description
======================= ==================================
``hive.metastore.uri``  The URI(s) of the Hive metastore.
======================= ==================================

``hive.metastore.uri``
^^^^^^^^^^^^^^^^^^^^^^
This is carried over from prototyping.

Iceberg Configuration Properties
--------------------------------

============================== ================================================= ============
Property Name                  Description                                       Default
============================== ================================================= ============
``presto-delta.location``      The path to the directory above the Delta table's directory       ``/tmp/delta``

``presto-delta.table``         The name of the directory within presto-delta.location holding the Delta table.    ``boston-housing``
============================== ================================================= ============
