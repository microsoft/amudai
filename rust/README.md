# Overview

This directory contains all the Rust code that supports the Amudai implementation.

# Crates

## amudai-common

Common definitions (traits and common data structures), relied upon by all amudai-* crates.

## amudai-io

I/O abstractions, mainly:
- `ReadAt`: positional reader with the ability to fetch a specified byte range from a file/blob.
- `SealingWrite`: sequential writer with a `seal()` operation, committing the write activity.

Provides a couple of simple implementations: memory-based and file-based.

## amudai-objectstore

*Object Store* abstraction: a hypothetical "storage service" client capable of issuing `Reader`'s and `Writer`'s for a given object URL.

## amudai-format

Generated code for the shard format data structure definitions and thin wrappers on top of those.

## amudai-encodings

Implementation of the data encodings for compressed/encoded data blocks.

## amudai-sequence

Lightweight `Sequence` and `SequenceBuilder` data structures to represent slices of basic values being read and decoded from the shard fields.

## amudai-blockstream

Implementation of the "encoded buffer" concept: stripe-scoped sequence of primitive values stored as a list of encoded blocks, along with the block map.

## amudai-shard

Shard creation (builder and encoder) and access (decoding).

## amudai-text-index

Inverted text index implementation.

**Note**: `amudai-shard` does not take direct dependency on this crate, for two reasons:
1. Indexes are pluggable and open-ended, the core shard format doesn't need to be aware of their details.
2. The implementation of this index takes dependency on the `amudai-shard` itself to store its own artifacts.

In the future, we may consider a simple DI mechanism to make this more streamlined (e.g. `register_index_provider`/`open_index` in the `amudai-shard`).

## amudai-arrow-compat

Adapters, converters and utilities for Arrow/Amudai interop. This crate depend on `amudai-format` and `amudai-common`, as well as various `arrow-*` crates.

## amudai-arrow

Implementation of the `Arrow` interface for the shard access.

## amudai

Top-level convenient "facade" crate.

## `support_crates`

Collection of shared library/utility crates that amudai-* can freely depend on.

### arrow-processing

Arrow utilities and extensions for more streamlined processing of arrays.
Does not take any dependency on amudai-* crates.

### codegentool

Code-generation tasks for Amudai development.

# Layering and dependencies

```
 _____________________________________________________________________
              text-index (:shard)    arrow (:shard)
 _____________________________________________________________________
                                   │
 __________________________________▼__________________________________
                                 shard
 _____________________________________________________________________
                                   │
 __________________________________▼__________________________________
              blockstream (:io, format, encodings)
 _____________________________________________________________________
                                   │
 __________________________________▼__________________________________
  encodings (:io, format)  arrow-compat(:format)  sequence(:format)
 _____________________________________________________________________
                                   │
 __________________________________▼__________________________________
                   objectstore (:io)    format(:io)
 _____________________________________________________________________
                                   │
 __________________________________▼__________________________________
                                  io
 _____________________________________________________________________
                                   │
 __________________________________▼__________________________________
                                common
 _____________________________________________________________________
```
