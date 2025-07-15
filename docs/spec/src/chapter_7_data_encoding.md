# Chapter 7: Data Encoding

This chapter presents the design of the data encoding infrastructure employed by the Amudai data format and specifies the structure of stripe-level encoding buffers according to the selected encoding type.

<!-- toc -->

## Overview

The data encoding infrastructure is designed to optimize storage and query performance in the new Data Lake format. It provides a modular framework that allows for selecting the most efficient encoding method based on the data characteristics.

### Objectives

Below are the objectives of data encodings framework, sorted in order of importance.

 - **High compression ratio**: Encodings are optimized to reduce data size.
 - **Efficient decompression**: Data access is optimized for low latency.
 - **Low encoding overhead**: Efficient algorithms minimize processing time and resource usage.
 - **Extensibility**: The framework accommodates new data types and encoding techniques.

The encoding framework categorizes encoding schemes into two main types: specialized encodings and general-purpose encodings.

#### Specialized Encodings

Specialized encodings are algorithms tailored for specific data types to optimize decoding performance and maintain competitive compression ratios. These encodings have the following characteristics:

 - **Type-Specific Optimization**: Designed to exploit the characteristics of specific data types for improved efficiency.
 - **Optimized Encoding**: Select the best encoding for each block based on data distribution.
 - **Hardware Acceleration**: Implement algorithms that leverage hardware features to enhance decoding speed.
 - **Smaller Encoded Blocks**: Utilize small-sized blocks that are optimized to fit into CPU caches, increasing cache efficiency and reducing memory latency.

#### General-Purpose Encodings

General-purpose encodings are type-agnostic compression methods suitable for a wide range of data types. 
Characteristics of general-purpose encodings:

 - **Lower Decompression Efficiency**: Generally slower to decompress compared to specialized encodings.
 - **Larger Block Sizes**: Require larger block sizes (e.g., 256 KiB) to achieve effective compression, which may not be ideal for fine-grained, query-oriented data access.

Despite these drawbacks, general-purpose encodings are supported to offer higher compression ratios when needed. Users can configure the encoding settings to balance compression performance and efficiency based on their requirements.

##### Dictionary Support in LZ4 and Zstandard

LZ4 and Zstandard encodings support dictionary-based compression and decompression, enabling the following advantages:

 - **Storage Efficiency**: By using a shared dictionary, storage space is conserved as common patterns are stored once.
 - **Fine-Grained Data Access**: Smaller encoded blocks can be effectively compressed, reducing unnecessary data access during queries.

## Encoding Framework

### Block-Based Design

Columnar data is encoded into a buffer partitioned into blocks, each containing a range of column values. Blocks range from a few hundred to a few thousand values, allowing for efficient encoding and decoding operations. Each block is encoded independently using either a general-purpose encoding or a specialized encoding algorithm, as specified by user settings:

 - **General-Purpose Encoding**: Applies a standard encoding method to all blocks uniformly.
 - **Specialized Encoding**: Selects the optimal encoding method for each block based on data characteristics during the encoding process.

On data access, the system reads the metadata within each block to determine the appropriate decoder for that block.

### Encoded Buffer Structure

The encoded buffer for column values consists of the following sections:

 - **Data Section**: A stream of concatenated encoded blocks.
 - **Blocks Map**: A data structure that maps between logical record position to encoded block offset and length.

Each block contains:

 - **Metadata Header**:
    - Encoding type used to compress the data in that block.
    - A bit indicating whether the encoding cascades down or this is the "leaf" encoding, in case it's not obvious
      for the encoding (some encodings never cascade down, in which case this flag is not needed).
    - Encoding-specific parameters (e.g., a FOR base values or null indicators).
 - **Data Section**:
    - Encoded data using the specified encoding technique.
      - Data section offset is defined by the metadata header length, which is well known for the current encoding type.
      - Data section length can be calculated based on the blocks map section information.

Encodings ensure that metadata sections are aligned, so every encoded section is aligned as well at 64 bits at least.

Blocks may also use cascading encoding with sub-blocks, where sub-blocks are encoded with the same framework.
The lowest level is encoded with so called "leaf" encoding.

The encoded buffer is structured as follows:

**Encoded Buffer**

|Byte offset|Section|
|-|-|
|0|Encoded values blocks|
|Encoded values blocks length|Blocks map|

The length of the data section as well as of the blocks map is provided as part of the shard's metadata.

**Encoded Block**

|Byte offset|Section|
|-|-|
|0|Encoding type (`u16`).|
|2|Flag of whether this is "leaf" encoding. Ommitted if encoding type is one of generic schemes.|
|3|Optional encoding parameters (specified by an encoding below).|
|4+Encoded parameters length|Encoded data|

Encoding types are translated into their respective integer value codes:

|Code|Encoding|Full Name|
|-|-|-|
|1|`Plain`|[Plain Encoding](#plain-encoding)|
|2|`FLBitPack`|[BitPacking Encoding](#bit-packing-encoding)|
|3|`FOR`|[Frame of Reference Encoding](#frame-of-reference-for)|
|4|`FFOR`|[Fused Frame of Reference Encoding](#fused-frame-of-reference-ffor)|
|5|`Delta`|[Delta Encoding](#delta-encoding)|
|6|`RunLength`|[Run Length Encoding](#run-length-encoding-rle)|
|7|`SingleValue`|[Single Value Encoding](#single-value-encoding)|
|8|`TruncateU8`|[Truncate Encoding](#truncate-encoding)|
|9|`TruncateU16`|[Truncate Encoding](#truncate-encoding)|
|10|`TruncateU32`|[Truncate Encoding](#truncate-encoding)|
|11|`TruncateU64`|[Truncate Encoding](#truncate-encoding)|
|12|`ALP`|[Adaptive Lossless Floating Point Encoding](#adaptive-lossless-floating-point-encoding-alp)|
|13|`ALPRD`|[Adaptive Lossless Floating Point For Real Doubles Encoding](#adaptive-lossles-floating-point-for-real-doubles-encoding-alprd)|
|14|`BlockDictionary`|[Block Dictionary encoding](#block-dictionary-encoding)|
|15|`FSST`|[Fast Static Symbol Table Encoding](#fast-static-symbol-table-fsst)|
|16|`ZSTD`|[Zstd Encoding](#general-purpose-encodings)|
|17|`LZ4`|[LZ4 Encoding](#general-purpose-encodings)|
|18|`ZigZag`|[ZigZag Encoding](#zigzag-encoding)|
|19|`Sparse`|[Sparse Encoding](#sparse-encoding)|


Since for dictionary and for generic-scheme encodings all stripe blocks are encoded using the same method, there's
no need for encoded blocks to have any metadata at all; encoded blocks only contain compressed data.

### Block-Level Checksum Validation

Optional: there will be a flag in the `encoded buffer descriptor` (these are elements inside the stripe-level `FieldDescriptor`)
that specifies whether encoded buffer has block-level checksums. If it's set, each compressed block will
be suffixed by a 4-byte checksum as specified in chapter 6. The checksum is computed over the metadata+data
buffer (it is only for the "outermost" metadata+data enclosure), and this checksum is considered part of
the "storage span" of the block. E.g. if for some block `i` the block map gives a storage range 100..200,
then range 100..196 is the metadata+data buffer, range 196..200 is a 4-byte checksum.

## Block Map

As previously mentioned, an encoded buffer consists of a sequence of compressed data blocks. Each block represents a specific logical range of values and occupies a corresponding physical byte range within the file.

The `BlockMap` is a data structure designed for efficient navigation among these blocks. Conceptually, a block map consists of two numerical lists: logical sizes (the number of values contained in each block) and block sizes (the compressed size in bytes of each block). We define `logical_offsets` as the prefix sums of logical sizes, and `block_offsets` as the prefix sums of block sizes. Both lists begin at zero and end with the total number of logical values and the total compressed size of all blocks, respectively. Consequently, each of these offset lists has a length equal to one more than the number of blocks.

For a given block with ordinal `i`, its logical range is defined by `logical_offsets[i]..logical_offsets[i + 1]`, and its physical location within the file is defined by `block_offsets[i]..block_offsets[i + 1]`. The primary purpose of the `BlockMap` is to translate a logical value position into the corresponding block ordinal containing that value, and to retrieve the block's logical range and file offsets.

The `BlockMap` is organized as a B-tree-like structure, detailed below:

### BlockMap Structure

#### Block Map Segment

A `BlockMapSegment` represents a subsequence of compressed block descriptors.

Structure of `BlockMapSegment`:
- `first_logical_pos`: `u64`
- `first_block_offset`: `u64`
- `min_logical_size`: `u32`
- `min_block_size`: `u32`
- `block_count`: `u16`
- `logical_size_bits`: `u8` // Possible values: 8, 16, or 32
- `block_size_bits`: `u8` // Possible values: 8, 16, or 32
- Packed logical sizes: `[u8; (logical_size_bits / 8 * block_count)]`
- Padding to align stream position to a 4-byte boundary
- Packed block sizes: `[u8; (block_size_bits / 8 * block_count)]`
- Padding to align stream position to an 8-byte boundary

#### Segment List

A `SegmentList` contains references to the `BlockMapSegment` records.

Structure of `SegmentList`:
- `segment_logical_offsets`: `[u64; segment_count + 1]` // Logical start and end offsets for each segment
- `segment_record_offsets`: `[u64; segment_count + 1]` // Start and end positions of corresponding `BlockMapSegment` records, relative to the start of the block map

#### Footer

Structure of `Footer`:
- `value_count`: `u64` // Total number of logical values
- `block_count`: `u64` // Total number of blocks
- `segment_count`: `u64` // Total number of segments
- `segment_size`: `u64` // Number of blocks per segment

## Specialized Encodings

Amudai leverages several specialized encodings inspired by BtrBlocks, tailored to specific data types and distributions.


### Plain Encoding

This encoding actually does nothing - the data is written as is.
The encoding is used in cases when the data cannot be compressed efficiently, or in cases when user decides to trade compression efficiency for read/write speed.


### Bit-Packing Encoding

This is the "workhorse" encoding commonly used as "leaf" encoding by other encodings.

 - **Use Case**: Efficient storage of integers when values can be represented with fewer bits than standard integer sizes.
 - **Method**: Encode integers using the minimal number of bits required for their value range and pack them contiguously without padding. This reduces storage space by eliminating unused bits.
 - **Suitable for Data Types**: Integer data types.

**Encoding parameters**

|Byte offset|Section|
|-|-|
|0|Max count of bits needed to represent each value in the block|

**Implementation**

The implementation can leverage the [FastLanes](https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf) compression framework, which utilizes CPU vectorization
techniques without requiring any SIMD-specific code. FastLanes is designed to efficiently handle data
compression by taking advantage of modern CPU architectures.


### Bit-Packing with Exceptions (?)

Like Bit-Packing, but allows a short list of values whose binary representation exceeds the minimal bits count needed for packing values in current block.
These values are appended to the end of the block along with their positions.

> [Forum]: Evaluate the need for this encoding based on Kusto-data telemetry.


### Frame-of-Reference (FOR)

 - **Use Case**: Data with small value ranges (e.g. timestamps in traces).
 - **Method**: Store a base value and encode offsets relative to it, often using bit-packing.
 - **Suitable for Data Types**: Integer and floating point data types.

**Encoding parameters**

|Byte offset|Section|
|-|-|
|0|Base value (`u32` or `u64` depending on the data type)|

**Encoded data structure**

Deltas are stored using a cascading encoding (`BitPacking`).


### Fused Frame-of-Reference (FFOR)

The encoding is the same as combination of [Frame-of-Reference](#frame-of-reference-for) and [BitPacking](#bit-packing-encoding) for deltas, but both encodings are "fused" into a single SIMD operation handled by the FastLanes approach.

**Encoding parameters**

|Byte offset|Section|
|-|-|
|0|Base value (`u32` or `u64` depending on the data type)|


### Delta Encoding

 - **Use Case**: Sorted or nearly sorted data.
 - **Method**: Store differences between consecutive values.
 - **Suitable for Data Types**: Integer data types.

**Encoding parameters**

|Byte offset|Section|
|-|-|
|0|Base value (`u32` or `u64` depending on the data type)|

**Encoded data structure**

Deltas are stored using a cascading encoding (`BitPacking`).


### Run-Length Encoding (RLE)

 - **Use Case**: Data with repeated consecutive values.
 - **Method**: Encode data as pairs of value and count.
 - **Suitable for Data Types**: Integer data types.

Here, a cascading encoding can be used to encode values and counts separately.

**Encoding parameters**

None.

**Encoded data structure**

|Byte offset|Section|
|-|-|
|0|"Runs" encoded sequence length (`u32`)|
|4|Values encoded sequence length (`u32`)|
|8|"Runs" sequence (`run[i]` - number of times value `i` in the block is repeated) encoded using `Bit-Packing`|
|8+"runs" sequence length|Values sequence encoded using suitable encoding|


### Single Value Encoding

 - **Use Case**: The whole block consists of a single value.
 - **Method**: Write a constant value.
 - **Suitable for Data Types**: All data types.

Single value can be efficiently represented using RLE encoding, however constant encoding eliminates the need
to specify run size as it equals to the block size.

**Encoding parameters**

Empty in case of fixed sized types. For variable sized types:

|Byte offset|Section|
|-|-|
|0|Value size (`u32`)|

**Encoded data structure**

Value bytes are written as is.


### Truncate Encoding

 - **Use Case**: Converting a signed integer into unsigned counterpart with reduced number of bits. This allows to use cascading encodings like BitPacking.
 - **Method**: Cast a signed integers into a smaller unsigned integer.
 - **Suitable for Data Types**: Signed integer values.

**Encoding parameters**

|Byte offset|Section|
|-|-|
|0|Whether the values are encoded using cascading encoding or not (`u8`)|

**Encoded data structure**

Unsigned integers are written either as is or using a cascading encoding.


### ZigZag Encoding

 - **Use Case**: Converting a signed integer into unsigned counterpart.
 - **Method**: Convert a signed integer into unsigned by multiplying it by `2` and adding `1` if the original number is negative.
 - **Suitable for Data Types**: Signed integer values.

**Encoding parameters**

|Byte offset|Section|
|-|-|
|0|Whether the values are encoded using cascading encoding or not (`u8`)|

**Encoded data structure**

Unsigned integers are written either as is or using a cascading encoding.


### Adaptive Lossless Floating-Point Encoding (ALP)

 - **Use Case**: Any floating-point value originated as decimal.
 - **Method**: Enchanced version of Pseudo-Decimal encoding in conjunction with FastLanes in second stage.
 - **Suitable for Data Types**: Floating point data types.

According to the [original paper](https://dl.acm.org/doi/pdf/10.1145/3626717), ALP outperforms
other algorithms like Chimp or Patas in compression ratio and in decompression speed. ALP struggles
in case floating point values have many duplications.

> [Forum]: Open question - what algorithm is better for compressing feature vectors.


### Adaptive Lossless Floating-Point for Real Doubles Encoding (ALPRD)

This is an extension of [ALP](#adaptive-lossless-floating-point-encoding-alp) used for cases for which `ALP` produces high number of exceptions, and thus becomes ineffective.


### Chimp

 - **Use Case**: Time-series floating point data (where values don't change significantly compared to previous value).
 - **Method**: Apply XOR operations on consecutive values, then compress the results using bit-packing.
 - **Suitable for Data Types**: Floating point data types.

[Original paper](https://www.vldb.org/pvldb/vol15/p3058-liakos.pdf)


### Block Dictionary Encoding

 - **Use Case**: Applicable for columns with numerous repeated values.
 - **Method**: Values are substituted with indices that reference a dictionary containing unique values.
 - **Suitable Data Types**: Applicable to all data types except `Boolean`.

This encoding embeds the dictionary (unique values and their respective codes) into the encoded block.


### Fast Static Symbol Table (FSST)

This is an extension of dictionary encoding.

 - **Use Case**: Strings with common substrings or patterns.
 - **Method**: Compress by extracting and encoding repeated terms within strings.
 - **Suitable for Data Types**: String data types.


### Null Suppression

 - **Use Case**: Sparse data with many null values.
 - **Method**: Skip storage for nulls, recording only their positions.
 - **Suitable for Data Types**: Null data type.


### Null Bitmap

 - **Use Case**: Efficiently track presence of nulls.
 - **Method**: Use one bit to indicate a null position within a block.
 - **Suitable for Data Types**: Null data type.


## General-Purpose Encodings

Supported general-purpose encodings include:

 - **LZ4**: Offers fast compression and decompression with moderate compression ratios.
 - **Zstandard (ZSTD)**: Provides a good balance between compression speed and compression ratio.
 - **Snappy**: Provides balanced choice between moderate compression speed and moderate compression ratio.

The decision on using generic-purpose encoding is made at the upper level for all the blocks in current stripe.
If the encoding supports shared dictionary feature, then stripe column values are written into two encoded buffers:

 - Dictionary used for encoding stripe column values.
 - Dictionary codes (value translations) encoded using some cascading encoding.

## Future Developments

The framework is open for future enhancements, such as:

 - Adding new encoding techniques for different data types.
 - Incorporating adaptive encoding selection algorithms.
 - Supporting hardware-accelerated compression methods.
