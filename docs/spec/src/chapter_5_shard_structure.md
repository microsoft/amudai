# Chapter 5: Shard Structure

This chapter presents a preliminary design document outlining the high-level structure of the Amudai data shard.

<!-- toc -->

## Overview

A shard is a horizontal partition of a table, logically storing a collection of records associated with that table. Each shard is composed of four main categories of information:

1. **Metadata**: This includes the schema, statistics, and other relevant information.
2. **Encoded Data**: These are the encoded sequences of values corresponding to the data fields.
3. **Indexes**: Structures that facilitate efficient data retrieval.
4. **Navigational Structures**: Auxiliary data structures designed to efficiently locate and access various components of the aforementioned categories.

This chapter concentrates on the high-level data structures associated with each of the aforementioned categories, exploring their conceptual hierarchy and the recommended methods for accessing and navigating these structures. We will exclude the "wire format" specification and refrain from detailing any file-level layout of the shard in this section.

### Design Considerations

Drawing from the industry's state-of-the-art practices and our own extensive experience, we have identified several detailed design considerations that complement the high-level goals of the shard format. These are listed in no particular order.

- It is not uncommon for a query to scan thousands of data shards, fetching only a handful of records from each. In such scenarios, the overhead involved in fetching and decoding the shards' metadata can become dominant. Our goal is to minimize this overhead as much as possible.
  
- In a similar vein, when performing a "narrow" projection on a "wide" shard, it is important to avoid the penalty associated with deserializing the metadata of irrelevant fields.

- Most established formats are primarily designed for full scans from cold storage. However, for Amudai, efficient random or semi-random access to disk-cached data shards - such as after evaluating an inverted index - is equally important.

- TBD: preference for "zero-deserialization" data structures.

- TBD: field alignments in zero-deserialization data structures.

- TBD: "page" alignment of the data buffers (for direct IO).

- TBD: granular checksums (for metadata structures, index pages and data blocks).

#### Immutability

```admonish todo title="To elaborate"
Shard immutability:
  1. Copy-on-write as a primary means of "editing" the shard.
  2. Specific cases where in-place editing is allowed, if at all.
```

## Prerequisites

### Content references

Throughout the format, it is often necessary to reference other sub-artifacts within the shard. These may include additional metadata, compressed data buffers, indexes, and more. To facilitate this, we introduce a generic mechanism for referencing arbitrary content within the shard. This mechanism will be consistently applied throughout the format design. At a minimum, a content reference includes:

- A moniker for the enclosing file (blob). This can be either an absolute or relative URI, with a special case for a relative URI being "self," which refers to the current file. For a detailed explanation of moniker design, please refer to [Chapter 3: Persistent Storage](./chapter_3_persistent_storage.md).
- A byte range specifying the content in question within the enclosing file. In line with common conventions, ranges in Amudai are always defined with an inclusive start position and an exclusive end position.

The precise representation of a _content reference_ is determined by the format of the containing message. For example, in protobuf, a dedicated message type will be defined for this purpose.

### Metadata serialization

In the shard format, there is no single "atomic" metadata structure that encapsulates the entire shard. Implementing such a structure would be counterproductive to our requirements for efficient data access. Instead, the format utilizes multiple distinct yet interconnected metadata elements. Each of these elements is independently serialized using `Protobuf`. The message proto definitions for all these elements can be found in [Chapter 6](./chapter_6_shard_format_details.md).

### Container File Format

All metadata and data elements referenced in this document, which are native to Amudai, are stored in files using a consistent container format. Each file includes an 8-byte header and a footer, which contains a 4-byte magic number `[0x61, 0x6d, 0x75, 0x64]` and a 4-byte format version.

## Shard structure

### Data stripes

The records within a shard are further segmented into multiple horizontal data stripes. Each shard contains at least one data stripe, with no set limit on stripe size; a single stripe can encompass an entire shard. It is recommended that writers create shards with a single stripe whenever possible.

The data stripe represents another layer in the data artifact hierarchy, primarily designed to facilitate the `shard merge` operation. Given that Amudai employs shard-level inverted indexes, an effective strategy to enhance their efficiency is to aim for larger shards — within reasonable practical constraints — thereby expanding the index's data scope. The `shard merge` operation achieves this by amalgamating several existing shards into a larger one and reconstructing the shard-level indexes. This process leverages the referencing of existing stripes to avoid the overhead of copying and/or re-encoding data buffers.

It is important to highlight that the data stripes mechanism is intentionally not designed for scan parallelization, nor is the stripe intended as a unit for work distribution. These objectives can be achieved by dividing the logical record range, which is generally orthogonal to the concept of stripes.

### Limits

The shard format uses 64-bit unsigned integers to represent item counts, logical ranges, and byte ranges. As a result, the theoretical maximum limit for the number of logical records within a shard, and each stripe, as well as the maximum number of logical positions for each field and the maximum size of each data file, is `2^64 - 1`.

In practice, we aim to support and recommend adhering to the following limits:
- Shards containing up to 10 billion records.
- A maximum of 50,000 fields in the schema, including nested fields.
- A total compressed data size of up to 1 terabyte per shard.
- A total size of custom shard properties of up to a few megabytes.

### Shard elements hierarchy

Below is the logical structure of a shard. Items highlighted in `code span` represent the actual stored elements, including metadata messages and data buffers.

- Shard directory
  - `Directory table-of-contents`
    - Shard record count
    - Stripe count
    - 'Schema' message reference
    - 'Indexes' reference
    - 'Stripes table' reference
    - 'Field descriptors' reference
    - 'Url list' reference
  - `Schema`
    - Hierarchical tree of fields, as defined in [Chapter 4: Type System](./chapter_4_type_system.md).
  - `Custom attributes and tags` *(property bag)*
  - `Shard-level indexes`
    - *List of index descriptors*
  - `Stripes table`
    - Stripe descriptor (x stripe count)
      - record count
      - stripe directory reference
  - Field descriptors
    - `List of field descriptor refs`
    - `Field descriptor` (x number of fields)
      - Shard schema field id
      - Stats and summaries *(aggregated from all stripes)*. *Most properties are optional, depending on factors such as data type, writer configuration (e.g., encoding policy), the data itself, and the writer's sophistication*.
        - Constant value. *If specified, the field maintains a constant value throughout the shard, eliminating the need for additional data artifacts*.
        - Logical position count *(pertinent for nested fields, equivalent to the shard record count for top-level fields)*.
        - min/max values
        - null count
        - distinct value count
          - Low-fidelity HyperLogLog (HLL)
        - Low-fidelity histogram
        - Bloom filters and other sketches
          - Split-Block Bloom Filters for membership testing
        - Type-specific properties (e.g. `NaN` count, `true` count, character set stats)
        - etc.
  - Stripe directory (x number of stripes)
    - Stripe field descriptors
      - `List of field descriptor refs`
      - `Field descriptor` (x number of fields)
        - Shard schema field id
        - Stats and summaries *(at the stripe level)*
          - *similar to shard-level stats/summaries*
        - Data encodings
          - Primary encoding. *Buffers vary depending on the data type and the data itself. For example, a List type might include an offset buffer and an optional presence buffer*.
            - Generic encoding buffer descriptor
              - Encoding kind
              - Content reference (data blob URI + data range).
              - (optional) Presence buffer descriptor
              - (optional) Dictionary descriptor *(possibly more than one)*
          - Alternative encoding(s).
            - alternative or foreign encoding buffer descriprtors. *e.g. reference to Parquet file + row group + column.*
        - Stripe-level field indexes
            - Index descriptor 0
            - ...


The subsections below provide a more detailed discussion of these elements.

#### Shard Directory Table of Contents (TOC)

The Table of Contents (TOC) serves as the starting point for accessing any shard operation. It can be saved either in its own file, as part of a larger "shard directory" file, or within the entire encoded shard blob. Typically, the TOC is positioned at the very end of the file. Following it are the size of the serialized TOC and its checksum, concluding with the standard Amudai file footer.

#### Schema

Each field node in the schema is assigned a unique integer identifier, beginning with zero. These identifiers are consistently used across the shard to reference the schema fields.

In addition to the standard fields, the Amudai shard specifies several optional dedicated internal fields at the top level of the schema. These fields can be included in a shard independently of the overarching table schema. They are clearly identified, and their `Field` definitions include relevant designation attributes. While these internal fields function similarly to regular fields in terms of storage format features and mechanisms, they may hold special significance for interpretation. We define the following internal fields:

- **Deleted Records Marker**
  - Formal Type: Boolean
  - Field Name: Not applicable (should be empty)
  - Meaning: If this field is present and its value is `true`, it indicates that the record is logically deleted from the shard and should not be displayed to the consumer under normal conditions. All readers must recognize this field, and writers must handle it appropriately.

- **Ingestion Timestamp**
  - Formal Type: 64-bit timestamp
  - Field Name: Not applicable (may be empty)
  - Meaning: The significance of this field depends on the database management system (DBMS). Readers may choose to ignore it, but writers must ensure it is preserved.

- **Unique Record ID**
  - Formal Type: FixedBinary<16> *(tentative)*.
  - Field Name: Not applicable (may be empty)
  - Meaning: Within the context of a table, this field provides a nominal record identity derived from the identity of the containing shard and its logical position within the shard.

#### Tags and Custom Properties
```admonish todo title="To elaborate"
We aim to support the definition of properties on multiple levels (shard, stripe, field) (while enforcing certain size limits - TBD). Properties classify into 2 categories: "standard" (system) and "custom" (user).
```

#### Shard-Level Index Descriptors

Typically, a shard can contain multiple indexes. Each index descriptor must specify at least the following:

- The type of index (e.g., B-tree style inverted term index, Hashmap index, etc.)
- The fields that are indexed
- References to the actual index artifacts
- Index-specific metadata

As a result, an index can cover one or more fields, and each field can be indexed by zero or more indexes.

Subsequent chapters will focus on the detailed index design and the types of indexes that we intend to initially support.

#### Field Descriptors

The *Field Descriptors List* is a logical component consisting of two adjacent stored structures:
- A sequence of individually encoded `FieldDescriptor` messages.
- A list of offsets into this sequence, where each "slot" index corresponds to a schema field ID. At the stripe level, fields that are not present have a descriptor with a length of zero.

This representation is used for both shard-level and stripe-level field descriptors.

At the stripe level, a `FieldDescriptor` connects an abstract schema field definition with its actual storage representation, including statistics and summaries of the values in that stripe. At the shard level, the `FieldDescriptor` only contains statistics for the field across the entire shard.

To access a subset of fields within a shard, a reader should follow these steps:
1. Resolve the fields in the shard schema.
2. Apply shard-level pre-filtering of the fields, if necessary:
   - Load the list of shard-level field descriptor references.
   - Identify the location of relevant field descriptors by indexing this list with the field IDs.
   - Retrieve and deserialize the necessary shard-level field descriptors.
   - Evaluate query filters against the available statistics and summaries.
3. For each stripe:
   - Load the list of stripe-level field descriptor references.
   - Identify the location of relevant field descriptors by indexing this list with the field IDs.
   - Retrieve and deserialize the necessary stripe-level field descriptors.
   - Optionally, evaluate query filters against the available statistics and summaries.
   - Choose an appropriate field decoder based on the encoding information and available encoding buffers.
   - Access the encoded value buffers and read and decode the relevant data blocks.

> [Forum] We discussed an option to have any property-bag to defined in a way that we can either model it as a part of the descriptor, or have a pointer to an external file with URL structure.

#### Membership Filters and Sketches

Field descriptors support various probabilistic data structures to enable efficient query filtering:

- **Split-Block Bloom Filters (SBBF)**: These provide approximate membership testing with configurable false positive rates. SBBFs are particularly efficient due to their cache-friendly design, touching only a single 32-byte block per query. They are stored as part of the field descriptor and can be used for:
  - Pre-filtering at the shard level before accessing stripes
  - Eliminating fields from consideration during query planning
  - Supporting semi-join operations without full data access

- **HyperLogLog Sketches**: Used for cardinality estimation, these compact data structures provide approximate distinct value counts with controlled error rates.

The placement of these sketches within field descriptors (rather than as separate index structures) allows for:
- Efficient access during query planning
- Natural aggregation across stripes at the shard level
- Minimal overhead when scanning field metadata

Not all fields may have these sketches - high-cardinality fields where the sketch size would be too large may skip sketch creation based on configurable heuristics.

#### URL List
The URL list is a section within the shard directory that catalogs all the blob URLs, whether they are absolute or relative, that are referenced by this shard.

### Data Encodings

Each `Field Descriptor` includes one or more `Data Encoding` sections. These sections specify the storage location of the values and the method for interpreting the compressed buffers. Typically, there will be only one `Data Encoding` section. However, additional sections may offer alternative representations for the same sequence of values.

### Encoded Data Buffers

Encoded data buffers store the actual sequences of primitive values for each stripe. A `Data Encoding` section references several of these buffers. While the detailed design is discussed in another chapter, here is a summary of the main points:

- For a given sequence of primitive values in a stripe, multiple encoded buffers may exist to represent different aspects of the sequence:
  - **Encoded Value Buffer**: This buffer holds the primary sequence of encoded values. See [Chapter 7](./chapter_7_data_encoding.md) for the value encoding specification.
  - **Presence Buffer (Optional)**: Typically a compressed bitmap, this buffer indicates the presence of null values, depending on the chosen encoding. Alternatively, null markers may be embedded within the value blocks by selecting a dedicated unused value.
  - **Encoded Offsets Buffer**: This buffer is used for variable-sized types such as strings and binary data.
  - **Dictionary Buffer (Optional)**: If dictionary encoding is used, this buffer contains the dictionary values.
- Each encoded buffer may have its own internal sub-structure to facilitate efficient random access, such as a sequence of individually compressed blocks and a lookup table that maps logical positions to the corresponding blocks.
- The start of each encoded buffer within the Amudai file container format must be aligned to a 64-byte boundary.

### File-Level Organization

The design outlined above does not impose a specific file-level structure. In theory, each component of the shard format — such as metadata messages, data buffers, and others — could be stored in separate files or blobs. However, this approach would be highly inefficient for readers, requiring multiple round-trips to access the storage, particularly when handling metadata. Below, we present two recommended approaches to improve efficiency.

#### Two-Level Shard Organization

In this approach, the top level consists of a single "shard directory" file. This file includes a sequence of stripe directories, along with their field descriptors, shard-level field descriptors, shard index descriptors, the schema, and a table of contents that references all these elements. The second level comprises a collection of stripe data files, with one file for each stripe. Each stripe data file contains encoded buffers for each stripe field, arranged sequentially. For example, a shard with just one stripe and no indexes will have two files: a shard directory and a stripe data file.

#### Single-Blob Shard

One can encode an entire shard into a single file, an approach best suited for relatively small or short-lived shards. The file structure would be approximately as follows:

- Buffers for Stripe 0, Field 0
- ...
- Buffers for Stripe 0, Field X
- Directory for Stripe 0 (containing field descriptors for the above buffers)
- ...
- Buffers for Stripe N, Field 0
- ...
- Directory for Stripe N
- Table of Stripes
- Shard Field Descriptors
- Shard Index Descriptors
- Shard Schema
- Shard Table of Contents

### Format Extensibility and Evolution

This section outlines the elements of the format that can be extended and our vision for its evolution. Generally, when extensibility is possible and unless otherwise specified, the guiding principles are:
- Readers unfamiliar with new features can safely ignore them.
- Assume that writers encountering unfamiliar elements should refuse to proceed with potentially destructive operations (such as editing, re-encoding, merging, rebuilding, or splitting).

#### Type System

New extension types (see [Type System](./chapter_4_type_system.md)) can be added without disrupting existing consumers. Generally, expanding the set of basic types should be avoided to prevent compatibility issues with current readers.

#### Metadata Structures

Expanding metadata structures is feasible and should adhere to standard Protobuf rules. Any new properties added must not invalidate or disrupt the semantics of existing ones.

#### Indexes

Index descriptors are composed of two parts: a fixed portion, which specifies the index type and its essential properties, and an index-specific portion, defined using the Protobuf `Any` type. One key property of an index is whether it is **optional** or **required**:

- **Optional Index**: Most indexes fall into this category. They are not crucial for decoding the data correctly and are primarily used to optimize queries. If a reader encounters an unfamiliar optional index, it can safely be ignored. Writers have the option to omit such an index (for example, when merging shards), although this is generally not advisable. The best practice is to avoid any potentially destructive actions.

- **Required Index**: This type of index may contain essential data needed for decoding, beyond just speeding up query execution. An example is the embedding vector index, which stores actual embedding vectors grouped by cluster, while the original field only points to the vector's location within the index to conserve space. If a reader encounters an unsupported required index, it should assume that it cannot properly decode the field. Writers are not permitted to remove such indexes.

#### Special (Internal) Fields

New kinds of internal fields can typically be added without affecting existing consumers, except when the field semantics pertains to security, privacy or correctness of data interpretation.

#### Probabilistic Data Structures

New sketch types can be added to the `membership_filters` and other filter collections. The design principles are:
- Sketches must be self-describing (include algorithm version and parameters)
- Unknown sketch types can be safely ignored by readers
- Writers should preserve sketches they don't understand when performing non-destructive operations
- Critical sketches that affect correctness should be clearly marked in their annotation

#### Low-Level Data Encodings

```admonish todo
This area requires dedicated brainstorming and discussion, as it presents significant challenges. Here are three potential strategies:
1. **Conservative Approach**: Avoid introducing new encodings without a compelling reason. When a new encoding is added, label it as "experimental" and keep it disabled by default. Allow several years for it to propagate through libraries and for the ecosystem to adapt before enabling it for general use.
2. **Plugin-Based Approach**: As suggested by Lance v2, treat encoders/decoders as plugins, with no built-in encodings. Declare all data encodings as part of the stored shard, leaving it to the reader to manage missing plugins (e.g., by notifying the consumer of missing dependencies or downloading the required plugins at runtime).
3. **Compromise Approach**: Combine the conservative strategy with a trade-off. If a new encoding is necessary for query performance and there is no concern regarding storage capacity or ingestion performance, implement it by writing data buffers in two representations: one with legacy encoding for fallback and another with the new encoding.
```
