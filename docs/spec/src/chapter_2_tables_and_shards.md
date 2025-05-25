# Chapter 2: Tables and Data Shards

<!-- toc -->

In this document, when we say **table** we reference the same concept as specified by
[Apache Iceberg Table Spec](https://iceberg.apache.org/spec/), version 2[^iceberg-version]. From that spec:

![Iceberg table catalog/metadata/data layers](./figures/iceberg-metadata.png)

Project Amudai adds a forth kind of data file (to the existing formats: Avro, Parquet, and ORC).
Data shards that use the Amudai file format are each a column store that supports indexing, encoding, and compression at the same time, yielding superior performance for semi-structured and unstructure
data analytics scenarios. Not only that, they are built to reduce the overhead associated with data grooming activities, such as time-based retention, GDPR purge, merge or rebuild of small and/or large data shards, row-level deletion and compaction, etc.

A few differences between the Amudai format and existing formats are:

1. Support for data semi-structured (`dynamic`) and unstructured (`string`) data types.

1. Support for multiple forms of indexing, including indexes for semi-structured and unstructured data.

1. The flexibility to have a data shard span existing data files, reducing overhead when performing data shard merge operations.

1. The flexibility to have specific parts of a data shard be placed in multiple persistent storage
   directories ("containers"), so that frequently-accessed files (such as indexes and metadata) can be places in fast/expensive storage while more rarely-access files can reside in slower/cheaper storage.

~~~admonish todo
Complete the list above.
~~~

A main design goal of Amudai is to allow distributed/concurrent ingestion without taking global locks.
For that reason, each Amudai data shard is self-contained in the sense that given a pointer to the data
shard in persistent storage, one can query the data shard without any other information, possibly with
the exception of persistent storage credentials (if the data shard spans persistent storage containers) and decryption keys (if the data shard is encrypted.)

## Schema

The schema (ordered list of columns) of the data in a data shard is the snapshot of the table schema
when the data shard is created. Each column holds values for all rows that make up the data shard.
Missing values are replaced by a (typed) `null` value so all rows in a data shard have the same number
of columns, which can be read by reading the data shard's schema property in its metadata.

The schema of Amudai data shards is bound by the number of columns. (That is, the number of columns
can not grow without bound.)

## Persistent storage

The table's data is stored in **persistent storage**, which is an abstraction for local or
cloud-based blob storage systems such as the local filesystem or Azure Storage. The data is stored 
in one or more **data shards**, each of which holds a distinct subset of the table's rows and
can be queried in isolation of other data shards. Conversely, the union of all data shards is
the table's data [^other-stores]. A data shard occupies at least one file in persistent storage,
and possibly may span multiple such files.

Other aspects of the table besides data might also be stored in **persistent storage**, or might
be held someplace else. For example, it's possible that the table's schema and pointers to data
are stored in a RDBMS. This is beyond the scope of this work, however.

More about persistent storage can be found in the [next chapter](./chapter_3_persistent_storage.md).

## Immutability of data shards and their files

Except for persistent storage "root" containers, their credentials, and possibly the decryption keys 
necessary to decrypt the data shard, all data shards are exactly specified by their data files.
Those data files are **immutable**: Once the data shard is created, all of its files can never change
(except for deletion when the data shard is removed.) This ensures that all readers can access the
data shard files without coordination with any writing process.

Any logical change to a data file consists of creating a **new** data shard (possibly based in part on the
existing files in persistent storage) and atomically  replacing the old data shard with a new one.

## Identifying/referencing a column

TODO: By name, by GUID?

## Null values

## Row ordering

## Partitioning information

## Tagging

## Additional properties

## Virtual columns

## Row-level deletion

## Encryption

~~~admonish todo
Data encryption: File-level encryption, column-level encryption, others?
~~~

## Row identifiers

~~~admonish todo
Decide if we want to spec them as mandatory from the get-go or not.
~~~

---



~~~admonish todo
We need to define if rows have an internally-managed identity column or not, mandatory or optional.

Also:
1. Hidden columns (like internally-managed identity column)
1. Virtual columns (a single property set at the shard/stripe/block level which holds true for _all_ values in the data shard)
1. Hints about the shard's membership of a particular data partitioning policy and the key space it represents, if any
1. Hints about sorting keys of the shard, if any
1. Null values representation
1. CRC/signature for important groups of values (such as the shard's schema) for quick comparison purposes
1. Column identifiers (name, GUID, anything else?)
~~~


[^iceberg-version]: Once Iceberg Table Spec Version 3 is complete, we expect to transition to this version as well.
[^other-stores]: For the purpose of this document, all the table's data is held in data shards
encoded according to this spec. In reality, of course, a table might hold data in other formats
as well, some of them might not be columnar, or even per-table.
