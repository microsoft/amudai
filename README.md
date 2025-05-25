# Introduction

This repository contains an implementation of the Amudai storage format along with various related utilities.

Please refer to [Amudai book](./docs/spec/) (under `docs/spec/`) for details.

# Development

## Prerequisites

### Protocol Buffers and Flatbuffers

If you're planning to modify the format definitions that use Protocol Buffers and FlatBuffers, make sure you have the necessary tools installed on your machine for successful code generation. Specifically, you'll need to have `protoc` (the Protocol Buffers compiler) and `planus` (the FlatBuffers compiler) available in your `$PATH`.

#### Linux (Azure Linux, aka Mariner)

```
sudo yum install protobuf-devel

cargo install planus-cli

```

#### Windows

Download `protoc` binary from https://github.com/protocolbuffers/protobuf/releases/latest (e.g. https://github.com/protocolbuffers/protobuf/releases/download/v29.1/protoc-29.1-win64.zip), unpack and place `protoc.exe` in your `%PATH%`.

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

This repository includes code generated from various types of "interface definition" files, particularly Protocol Buffers and FlatBuffers. Currently, the generated code is included in the repository. To regenerate it, please refer to the [codegentool](./rust/support_crates/codegentool/README.md) documentation.

# Trademarks

This project may contain trademarks or logos for projects, products, or services. 
Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft’s Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general). 

Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party’s policies.
