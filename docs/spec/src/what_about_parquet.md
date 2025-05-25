# Parquet's Limitations as a Modern Data Lake Format

<!-- toc -->

## Overview

When designing a new storage format, a critical question arises: why not simply use, extend, or improve an existing solution? Parquet, currently the most popular lake format, often seems like the default choice. However, while Parquet has served the industry well, its limitations become increasingly apparent when addressing the demands of modern data workloads. This document outlines the deficiencies of Parquet as a versatile, future-proof lake format, highlighting areas where it falls short and necessitates a more robust solution.

## Semi-structured Data

By "semi-structured data," we're referring to complex objects, time-series data, tensors, embedding vectors, and deeply nested property bags. Parquet's approach to encoding nested structures using repetition and definition levels creates challenges for efficient decoding. Nested field traversal and access, especially to specific elements within complex nested structures, becomes particularly problematic. The inherent complexity of navigating these levels often leads to inefficient decoding process of the Parquest's nested field encoding.

## Point Lookups

Parquet's encoding schemes are fundamentally optimized for sequential scans, not for semi-random access patterns. While recent additions like page indexes offer some mitigation, these improvements are primarily effective for flat schemas. When point lookups are required on nested fields, we're often forced back to sequential scans and full decoding of Parquet's nested field encoding. This is a significant performance bottleneck, especially when dealing with sparse data access patterns.

## Extensible Metadata and Hierarchical Statistics

An ideal storage format should allow for attaching extensible data statistics and summaries at any level of granularity: from individual data blocks and column slices (e.g., a field within a row group) to the entire file. This goes far beyond simple min/max values or even Bloom filters. Modern query engines can leverage sophisticated statistical summaries like HyperLogLog, Count-Min sketches, and t-digests to optimize query execution. Parquet's limited metadata capabilities restrict the potential for such optimizations, forcing query engines to rely on less efficient methods.

## Index Artifacts

Parquet lacks a standardized and extensible mechanism for augmenting data with index artifacts, most notably inverted term indexes for full-text search. While workarounds exist, such as maintaining indexes and their metadata outside of Parquet, these solutions quickly become complex, fragile, and difficult to manage. The absence of native index support forces users to build and maintain separate indexing systems, adding significant overhead and complexity.

## Wide Columns

Parquet lacks adequate provisions for storing columns containing large binary values. While this may not be a concern for traditional data warehousing scenarios, modern workloads involving IoT, observability, and AI frequently encounter such data. The inability to efficiently handle large binary blobs is a significant limitation that restricts Parquet's applicability in these rapidly growing domains. This is not a theoretical concern; our experience with Kusto over the past decade has clearly demonstrated the need for robust support for wide columns.

## Wide Schemas

We've repeatedly encountered scenarios where customers need to define tables with 10,000+ fields, while only projecting a handful of fields per query. While column-oriented storage addresses the *data* access aspect, the overhead of parsing schemas and metadata descriptors becomes a significant bottleneck when querying such tables with thousands of Parquet files. The "pay only for what you use" philosophy breaks down in this scenario. Parquet's inability to efficiently handle wide schemas leads to unnecessary processing overhead, impacting query performance and scalability.

## Efficient Indexing for RAG Applications

This point is nuanced, but the similarity search scenario, particularly in the context of Retrieval Augmented Generation (RAG), highlights the issue. Consider a table where records are enriched with embedding vectors, often multiple vectors per record (e.g., for "title" and "content"). Similarity search requires a dedicated index on these vectors, which usually stores the vectors (or their quantized representation) in a different order (e.g., as a hierarchy of clusters). However, there's still a need to associate each vector with the corresponding record in the data file. Parquet lacks a mechanism for such "detached" content storage, forcing us to either duplicate the vectors (which can comprise 50-80% of the data) or rely on fragile external workarounds.

## Tiered Storage Support

Many storage services offer flexible tiering schemes, such as Azure Blob Storage's Premium, Standard, and Cold tiers. An ideal storage format should be able to leverage these tiers by placing different sub-artifacts of the data shard in different storage locations based on anticipated usage patterns. For example, frequently accessed columns in hot storage, the bulk of data in standard storage, and rarely used indexes in cold storage. The final logical "artifact" should then be assembled from these disparate physical locations.

## Alternative Data Encodings: One Size Does Not Fit All

Beyond simple data types like numbers and strings, a single data representation is rarely optimal. In many cases, support for alternative data encodings is needed (e.g., a semi-structured field stored as a JSON string, a VARIANT-style binary blob, and a "shredded" nested columnar layout). Parquet's lack of support for alternative data encodings prevents storage and query engines from making informed trade-offs between ingestion cost, storage cost, data access speed, and query performance.

## Efficient Editing and Composition

Beyond record-level soft deletion, many "edit" operations need to be efficiently performed at scale: re-typing columns, merging smaller data artifacts, adding new columns, etc. The key ingredient missing in Parquet is support for "copy-on-write" operations as part of the storage format. The ability to represent a logical data shard as a composition of smaller immutable artifacts (e.g., individual column slices, metadata elements) naturally allows for efficient "edits" with versioning.

## Encoding and Compression Innovations

Parquet relies on outdated and slowly progressing compression and encoding schemes. Recent advancements like FastLanes, FSST, ALP, and cascading compression are not incorporated.

## Special System Fields

Most systems dealing with incoming data need to enrich records with special system-level fields, such as arrival/ingestion timestamps, data origin locations, and unique record IDs. While not a critical omission, the lack of a clear, standardized, and extensible mechanism to introduce "system" fields is another challenge. This forces users to implement custom solutions, adding complexity and hurting interoperability.

## Extensibility

Many of the points above implicitly highlight Parquet's lack of extensibility. This section summarizes the general absence of extensibility mechanisms in the current Parquet format, beyond simple "custom metadata" capabilities. This limitation is particularly painful when trying to solve problems related to indexing, tiered storage, and alternative encodings. Parquet's rigid structure prevents it from adapting to evolving data needs and technological advancements, making it a less-than-ideal choice for future-proof data platforms.
