# Introduction

Project Amudai introduces an advanced columnar storage format, evolving from the mature and battle-tested internal storage of Kusto (Azure Data Explorer and Microsoft Fabric EventHouse), a format that currently stores exabytes of data within Microsoft. The primary motivation behind Amudai is to address the limitations of existing data lake formats, such as Apache Parquet, especially when dealing with modern, complex data workloads. Amudai aims to provide a high-performance, open-standard solution for analytics over structured, semi-structured (e.g., JSON, dynamic types), unstructured (e.g., free text), and embedding vectors, while also paving the way for capabilities like efficient time series analytics and graph queries.

Key features of Amudai include its sophisticated approach to data encoding, compression and indexing, offering a rich set of specialized encodings to optimize for various data types and distributions. It provides robust support for advanced indexing, including inverted term indexes crucial for fast text search, and vector similarity indexes, as well as predicate pushdown on semi-structured data. The format is designed for immutability, enabling efficient "copy-on-write" operations, shard merging by referencing existing data stripes, and simplified caching.

Amudai is built with extensibility in mind, allowing for new data types, metadata structures, and indexes to be added over time. It supports efficient handling of wide schemas and columns, tiered storage (placing hot/cold data artifacts in different storage locations), and aims for seamless integration with table formats like Apache Iceberg. This makes Amudai a versatile and future-proof foundation for demanding analytical systems.

This repository contains an implementation of the Amudai storage format along with various related utilities.

Please refer to [Amudai book](./docs/spec/) (under `docs/spec/`) for details.


> ### **⚠️ Important Notice: Development in Progress**
>
> This project is currently under **active development**. **APIs, data formats, and other interfaces are subject to frequent breaking changes** as we iterate on features and architecture. We do not recommend relying on this project's API or data format for production use at this time.
>
> **Use at your own risk** and expect things to break or change without notice until we reach a stable release. Contributions and feedback are very welcome!


# Development

## Prerequisites

### Protocol Buffers and Flatbuffers

If you're planning to modify the format definitions that use Protocol Buffers and FlatBuffers, make sure you have the necessary tools installed on your machine for successful code generation. Specifically, you'll need to have `protoc` (the Protocol Buffers compiler) and `planus` (the FlatBuffers compiler) available in your `$PATH`.

#### Linux (Azure Linux, aka Mariner)

```
sudo tdnf install protobuf-devel

cargo install planus-cli

```

#### Windows

Download `protoc` binary from https://github.com/protocolbuffers/protobuf/releases/latest (e.g. https://github.com/protocolbuffers/protobuf/releases/download/v31.1/protoc-31.1-win64.zip), unpack and place `protoc.exe` in your `%PATH%`.

Alternatively, you can run `choco install protoc` from an elevated prompt.

```
cargo install planus-cli
```

#### macOS

TBD

## Repo structure

**`docs`** folder contains the format "book".

**`rust`** folder contains the Rust implementation. Please refer to its [README](./rust/README.md).

**`proto_defs`** contains the Protocol Buffers and FlatBuffers definitions for the Amudai format data structures.

**`tools`** includes a variety of tools and scripts that can be useful during development activities.

## Building

### Rust

*Note: on Linux, you may need to perform `az login` followed by `./tools/cargologin.sh` in order to be able to fetch crates from msazure pkgs feed*.

From the repo root:

```
cargo build --all-targets
```

or 

```
cargo build --release --all-targets
```

## Linter (style and common mistakes)

```
cargo clippy
```

## Running the unit tests

```
cargo test
```

(or `cargo nextest run`, if you have [nextest](https://nexte.st/) setup)

## Code generation

This repository includes code generated from various types of "interface definition" files, particularly Protocol Buffers and FlatBuffers. Currently, the generated code is included in the repository. To regenerate it, please refer to the [amudai-codegentool](./rust/tools/amudai-codegentool/README.md) documentation.

# Trademarks

This project may contain trademarks or logos for projects, products, or services. 
Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft’s Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general). 

Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party’s policies.
