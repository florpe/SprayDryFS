=================
SprayDryFS, the spray-drying file system
=================
-------------------------
Spray, dry, rehydrate!
-------------------------

Introduction
============

SprayDryFS attempts to square the read-only parts of the POSIX filesystem API
with the notion of content-addressable, immutable storage. The lowest unit of
storage is the chunk, identified by its hash and optionally compressed with a
shared dictionary. A file is given by a list of chunks and identified by the
hash of their concatenation; a directory is a file describing file names,
attributes, and hashes. Mounting hooks, called roots, are given by a name and
a version. With these mechanisms SprayDryFS is geared towards use cases that
benefit from versioning and transparent deduplication.

Encryption is not in the scope of SprayDryFS, but could conceivably be
implemented as an outgrowth of chunk compression support.

Implementation
==============

Storage
-------

SprayDryFS is backed by an SQLite database which is set up using the queries
in the spraydryfs.db module. Directories, files, and chunks are saved with
their hashes, providing some measure of protection against data corruption
as well as hinting at the possibility of sharing data across the network. So
far, no caching or sharing is implemented.

To enable SprayDryFS to operate on storage backends other than a regular file,
LumoSQL support is on the roadmap. Once this is implemented, SprayDryFS will
be able to run off a raw partition without a hosting filesystem.

Interface
---------

SprayDryFS should eventually support three modes of operation:

- Mounted mode, operating as a FUSE file system
- Ingesting mode, copying data from existing roots and the wider file system
  tree to a newly created root
- Training mode, creating new shared dictionaries from training data either
  present inside the database or acquired from outside sources

At present only mounted mode is properly exposed. The prerequisites for the
other two are present in the spraydryfs.spraydry and spraydryfs.rehydrate
modules. Additional possible modes of operation might be a networked mode,
essentially acting as a key-value store mapping hashes to content, an
administrative mode to compose and dissect existing roots into new ones, and
a streaming mode for direct extraction of chunks and files.

Language
--------------

Currently the entire filesystem is implemented in Python. With an eye towards
portability and performance it will be desirable to create at least a
read-only implementation in a compiled language.
