# amudai-cmd

A command-line utility for working with Amudai format data shards. This tool provides a collection of useful utilities for development, debugging, and troubleshooting scenarios, rather than a comprehensive production suite. It includes functionality for ingesting, inspecting, analyzing, and consuming Amudai shards.

## About Amudai

Amudai is a high-performance, open-standard columnar storage format designed for modern analytical workloads. It evolved from the battle-tested internal storage format of Microsoft's Kusto (Azure Data Explorer) and is designed to efficiently handle structured, semi-structured (JSON), unstructured (free text), and vector data.

A **shard** in Amudai is a fundamental unit of data storage - a self-contained, immutable collection of columnar data organized into **stripes**. Each shard contains:
- Schema metadata
- One or more data stripes (horizontal slices of data)
- Field descriptors and statistics
- Optional indexes for efficient querying

## Installation

Build from source using Cargo:

```bash
cd rust/tools/amudai-cmd
cargo build --release
```

The compiled binary will be available at `target/release/amudai-cmd` (or `amudai-cmd.exe` on Windows).

## Commands

### `ingest` - Convert data files to Amudai shards

Converts CSV, JSON, NDJSON, JSONL, or MDJSON files into an Amudai shard format.

**Basic usage:**
```bash
amudai-cmd ingest --file data.csv output_shard.amudai
amudai-cmd ingest --file data.json --file data2.json output_shard.amudai
```

**Schema options:**

1. **Automatic schema inference** (default):
   ```bash
   amudai-cmd ingest --file data.csv output_shard.amudai
   ```

2. **Inline schema specification**:
   ```bash
   amudai-cmd ingest --schema "(id: long, name: string, score: double, timestamp: datetime)" --file data.csv output_shard.amudai
   ```

3. **Schema from file**:
   ```bash
   amudai-cmd ingest --schema-file schema.json --file data.csv output_shard.amudai
   ```

**Supported file formats:**
- **CSV** (`.csv`) - Comma-separated values
- **JSON** (`.json`) - Single JSON object or array
- **NDJSON/JSONL** (`.ndjson`, `.jsonl`) - Newline-delimited JSON
- **MDJSON** (`.mdjson`) - Multi-line JSON (one JSON object per line)

**Supported schema types:**
- `string` → UTF-8 strings
- `int`, `int32`, `i32` → 32-bit signed integers
- `long`, `int64`, `i64` → 64-bit signed integers  
- `double`, `float64`, `f64` → 64-bit floating point
- `datetime` → Nanosecond-precision timestamps

**Examples:**
```bash
# Ingest multiple CSV files with automatic schema inference
amudai-cmd ingest --file sales_2023.csv --file sales_2024.csv sales_data.amudai

# Ingest JSON with explicit schema
amudai-cmd ingest \
  --schema "(user_id: long, event_name: string, properties: string, timestamp: datetime)" \
  --file events.ndjson \
  events_shard.amudai

# Ingest using schema from file
amudai-cmd ingest --schema-file table_schema.json --file data.csv table.amudai
```

### `inspect` - Examine shard structure and metadata

Displays detailed information about a shard's structure, schema, statistics, and content.

**Basic usage:**
```bash
amudai-cmd inspect shard.amudai
amudai-cmd inspect /path/to/shard.amudai
```

**Verbosity levels:**
- Default: Basic shard information (record counts, schema summary)
- `-v` (verbose): Detailed field information and statistics  
- `-vv` (very verbose): Complete metadata including encoding details

**Examples:**
```bash
# Basic inspection
amudai-cmd inspect data.amudai

# Verbose output with field details
amudai-cmd inspect -v data.amudai

# Maximum verbosity
amudai-cmd inspect -vv data.amudai
```

**Output includes:**
- Total record and stripe counts
- Schema information (field names, types, nested structures)
- Storage statistics (data size, compression ratios)
- Field-level metadata and statistics
- URL lists and references
- Encoding information (with `-vv`)

### `infer-schema` - Analyze files and generate schemas

Analyzes source data files and outputs an inferred schema that can be used for ingestion.

**Basic usage:**
```bash
# Output to stdout
amudai-cmd infer-schema --file data.csv

# Save to file
amudai-cmd infer-schema --file data.csv --output schema.json
```

**Examples:**
```bash
# Infer schema from multiple files
amudai-cmd infer-schema --file file1.csv --file file2.json --output combined_schema.json

# Analyze JSON structure
amudai-cmd infer-schema --file events.ndjson

# Generate schema for complex data
amudai-cmd infer-schema --file logs.jsonl --output logs_schema.json
```

### `consume` - Read and benchmark shard performance

Reads data from a shard to measure read performance and validate shard integrity.

**Basic usage:**
```bash
# Read entire shard
amudai-cmd consume shard.amudai

# Read specific number of records
amudai-cmd consume --count 10000 shard.amudai

# Performance testing with multiple iterations
amudai-cmd consume --iterations 5 --count 100000 shard.amudai
```

**Performance metrics provided:**
- Total read time
- Throughput (MB/s)
- Records per second
- Total data size processed
- Batch and record counts

**Examples:**
```bash
# Quick validation read
amudai-cmd consume --count 1000 data.amudai

# Benchmark read performance
amudai-cmd consume --iterations 10 data.amudai

# Test partial reads
amudai-cmd consume --count 50000 large_shard.amudai
```

## File Paths and URLs

All commands accept both local file paths and URLs:

```bash
# Local files
amudai-cmd inspect ./data/shard.amudai
amudai-cmd inspect C:\data\shard.amudai

# File URLs
amudai-cmd inspect file:///path/to/shard.amudai
```

Relative paths are automatically resolved to absolute paths.

## Examples and Workflows

### Complete data processing pipeline:

```bash
# 1. Analyze your data structure
amudai-cmd infer-schema --file raw_data.csv --output data_schema.json

# 2. Review and optionally modify the schema
cat data_schema.json

# 3. Ingest data using the schema
amudai-cmd ingest --schema-file data_schema.json --file raw_data.csv processed_data.amudai

# 4. Inspect the resulting shard
amudai-cmd inspect -v processed_data.amudai

# 5. Validate with a test read
amudai-cmd consume --count 1000 processed_data.amudai
```

### Working with multiple data sources:

```bash
# Combine multiple files into one shard
amudai-cmd ingest \
  --schema "(timestamp: datetime, sensor_id: string, value: double, quality: int)" \
  --file sensor_data_2023.csv \
  --file sensor_data_2024.csv \
  combined_sensors.amudai

# Inspect the combined result
amudai-cmd inspect -v combined_sensors.amudai
```

### Performance testing:

```bash
# Create a shard for testing
amudai-cmd ingest --file large_dataset.csv test_shard.amudai

# Benchmark read performance
amudai-cmd consume --iterations 5 test_shard.amudai

# Test partial read performance  
amudai-cmd consume --count 100000 --iterations 3 test_shard.amudai
```


For more information about the Amudai format specification, see the documentation in the `docs/` directory of this repository.
