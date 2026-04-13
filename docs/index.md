dd-lob-store
=============

Manages a DANS Data Vault Large Object Store

Purpose
-------
Transfers large files from Dataverse instances to the Data Vault Large Object Store.

Interfaces
----------

This service has the following interfaces:

![](img/overview.png){width="70%"}

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

### Request creation

The service receives requests for transfer via the API. It first checks if the URL has a base URL that corresponds with the targeted Data Station. If not, the
request is rejected immediately. Next, the service checks whether the SHA-1 checksum is already present in the targeted LOB-store. If so, the job is immediately
changed to DONE. Otherwise, the job is created as PENDING in the database.

### File download

There is one worker thread that periodically checks for PENDING jobs and processes them from older to newer. It first creates a new bucket-directory in the
download-directory if none exists yet. It then starts downloading the file in chunks of the configured size using range requests. The chunk files are named
`<sha1sum>.<seqnr>`, e.g., `3f24c343d7e7e1d8606c894689625de5a53df28c.1`, `3f24c343d7e7e1d8606c894689625de5a53df28c.2`, etc. After all chunks are downloaded
they are concatenated into a single file named `<sha1sum>`, e.g., `3f24c343d7e7e1d8606c894689625de5a53df28c`. The chunk files are then deleted. The service then
verifies that the SHA-1 checksum of the concatenated file matches the expected value. If not, the job is marked as FAILED and the file is deleted.

If the bucket size exceeds the configured maximum, the service proceeds to the next step. Otherwise, more files are downloaded first.

### Archive package creation

The service now calls the configured packaging command to create the bucket archive, typically a DMFTAR archive. This archive will be placed in the upload 
directory. 

### Transfer to Data Archive

After the successful creation of the archive package, the service will now call the configured transfer command. Typically, this will be `rsync` with the option
activated that allows for resumption after a connection failure. If the process is interrupted or returns a non-zero response, the command is retried after the
configured waiting period.

### Verification of transfer

Finally, on finishing the transfer, the archive package is verified using the configured verification command. 