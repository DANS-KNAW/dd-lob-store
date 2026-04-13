dd-lob-store
=============

Manages a DANS Data Vault Large Object Store

Purpose
-------
Transfers large files from Dataverse instances to the Data Vault Large Object Store.

Interfaces
----------

This service has the following interfaces:

#### Command API

* _Protocol type_: HTTP
* _Internal or external_: **internal**
* _Purpose_: to manage the service including starting transfers

#### Admin console

* _Protocol type_: HTTP
* _Internal or external_: **internal**
* _Purpose_: application monitoring and management

### Consumed interfaces

#### DMFTAR

* _Protocol type_: Local command invocation
* _Internal or external_: **external**
* _Purpose_: to create DMFTAR archives for buckets.

#### rsync

* _Protocol type_: Local command invocation
* _Internal or external_: **external**
* _Purpose_: to transfer files from the local upload directory to [SURF Data Archive]{:target=_blank}

[SURF Data Archive]:  {{ surf_data_archive }}
 

Processing
----------

TODO
