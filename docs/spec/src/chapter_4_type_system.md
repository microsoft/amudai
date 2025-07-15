# Chapter 4: Type System

<!-- toc -->

The type system is one of the foundational aspects of the storage format. The proposal outlined below draws inspiration from the type systems of Iceberg, Parquet, and primarily Arrow, which has become the de facto standard for data frame interchange. We should note that Arrow's type system specification is quite detailed, as it addresses the precise in-memory representation of arrays. In this document we can maintain a more abstract perspective.

At the most fundamental level, we can differentiate between _basic_ types and _extension_ types. While some systems refer to these as "physical" and "logical" types, the definitions can vary, potentially leading to confusion. The reasoning behind this distinction will be elaborated upon later in this document. Basic types are essential to the storage format. When a schema element possesses a basic type, an _extension type_ can be considered an annotation that enhances semantics, refines the set of applicable operations (such as scalar functions), and limits the range of permissible values.

Basic types are categorized into primitive and composite types. Some primitive types and all composite types are parameterized by numeric constants and/or other types. For each basic type, we specify:
 - Bit width of its plain (uncompressed) representation, when relevant
 - Value domain (set of valid values)
 - Type parameters, when applicable
 - Design of the nested columnar encoding (for composite types)

In terms of notation, this document adopt the Arrow convention for parameterized types, which combines a familiar "generics" syntax with named fields where applicable. For example, a point structure with two coordinates can be expressed as `Point: Struct<x: f32, y: f32>`. A list of such points is `List<Struct<x: f32, y: f32>>`.

## Basic types

Proposed set of basic types in Amudai:
 - Primitive types:
   - `Boolean`: `true` or `false`. When cast to an integer, `true` is `1` and false is `0`.
   - Integers: `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `i64`, `u64`.
   - Floating point numbers: IEEE 754 single- and double-precision `f32` and `f64`.
   - `Binary`: variable-length sequence of arbitrary bytes. 
   - `FixedSizeBinary<N>`: fixed-length (`N` bytes) sequence of arbitrary bytes.
   - `String`: arbitrary-length character sequence encoded as UTF-8. 
   - `GUID` : Little-endian Guid.
   - `DateTime`: ticks (100-nansoseconds passed since 0001-01-01)
 - Composite types:
   - `List<T>`: variable-length list with elements of type `T` (where `T` is any of the supported basic types). 
   - `FixedSizeList<N, T>`: fixed-length list of type `T` (with `N` elements).
   - `Struct<f0: T0, f1: T1, ...>`: structure with named fields (aka a "record type").
   - `Map<K, V>`:  variable-size sequence of key-value pairs.
   - `Union<f0: T0, f1: T1, ...>`: union type where each value can be one of `T0`, `T1`, ... types.

Values of any type can be null; all of the above types are "nullable" by default.
**QQ**: is there any benefit in modeling non-nullable values at the type system level (considering we will have null stats for each stored element)?

Extension types will be discussed later in this document.

~~~admonish example
a sequence of json-like values
```json
{ "id": 1, "props": { "name": "n1", "color": "green" }, "points": [1, 2, 3] }
{ "id": 2, "props": { "name": "n2", "metrics": [1.5, 3.0] }, "points": [1, 2, 3] }
{ "id": 3, "props": { "name": "n3", "color": "red", "metrics": [1.0, 2.0] }, "points": [1, 2, 3] }
```

may have the inferred type `Struct<id: i64, props: Struct<name: String, color: String, metrics: List<f32>>, points: List<i64>>`. Alternatively, it can also be represented as `Map<String, Union<i64, Map<String, Union<String, List<f32>>>, List<i64>>>`, though this is an extremely inefficient representation.
~~~

Ultimately, an Amudai shard represents a horizontal slice of a table and logically stores a collection of records belonging to that table. The "schema" of a shard is a subset of the table's schema (some columns may be missing from the shard), and there are two common ways to represent this top-level record schema:
1. As a single `Struct<column0: T0, column1: T1, ...>`
2. As a simple list of fields, where each field is a (name:type) pair.

Functionally, these are equivalent. Option (2) is more reader-friendly for those designed to handle simple tables of primitive (non-composite) fields and who do not wish to engage with nested type semantics.

The type system specified above needs to be encoded by the data structures in the shard's metadata. The type of each field is represented with a hierarchy of `DataType` nodes, where the child nodes represent the type parameters of the parent node. The leaf nodes are always one of the primitive types. Each `DataType` node may have an optional _extension type_ annotation attached to it (in particular, the top-level `DataType` of each field).

## The rationale for basic and extension types

As previously mentioned, nearly all systems from which we draw inspiration incorporate some concept of "physical" and "logical" types. We define the _basic type_ as the fundamental "shape" of the value, which is natively supported by the storage format. In contrast, the _extension type_ serves as an annotation on any node within the hierarchical schema of basic type nodes.

On the reader path, this annotation is generally more relevant to the higher layers of the stack (e.g., query engines), while storage primarily concerns itself with the basic type of the field. On the writer path, an extension type may provide a hint for a more suitable data encoding and indexing.

The most critical aspect of this distinction relates to format evolution and extensibility. Once Amudai is publicly released, changes or additions to the basic types will be exceedingly rare, necessitating community consensus and a lengthy approval process. Such changes could potentially disrupt existing readers. We aim to avoid a scenario where the introduction of new features into the product (Kusto) is hindered by the standardization process and concerns about backward compatibility.

Here are a couple of examples to illustrate the concept:

~~~admonish example title="Example 1"
Suppose we decide to introduce a new high-level GeoPoint type in the future, which is conceptually defined as:
```
struct GeoPoint {
    latitude: f32,
    longitude: f32,
    tag: String,
}
```
with its own set of supported operations. A Kusto table with the schema `(num: i64, name: String, location: GeoPoint)` can then be represented in the storage shards as `Struct<num: i64, name: String, location: Struct<latitude: f32, longitude: f32, tag: String>>`, where the `location` node in the `DataType` tree is annotated with the `GeoPoint` extension type.
~~~

```admonish example title="Example 2"
A column containing a text embedding vector is typically modeled as a field with the type `List<bf16>` (or possibly `FixedSizeList<1024, f32>`, or another suitable shape for a floating-point vector). However, for the query engine to function effectively and accurately, it must be aware of at least two additional pieces of information about that field:
- It is an embedding vector.
- The exact name and version of the model used to compute the embeddings.

Thus, the basic type of the field in the shard's schema remains `List<bf16>`, but it is annotated with an extension type that might appear as `EmbeddingVec(model=openai-text-embedding-3-small)`.
```

## Kusto specifics

The initial list of extension types that we plan to define is dictated by the current Kusto storage data types and the need for a smooth transition. This list includes:
 - `KustoTimeSpan` (basic type: `i64`, 100-nansosecond ticks).
 - `KustoDecimal` (basic type: `FixedSizeBinary<16>`). [DecNumber128](https://en.wikipedia.org/wiki/Decimal128_floating-point_format).
 - `KustoDynamic` (basic type: `Binary`). Dynamic value encoded as a single compact buffer, equivalent to today's Kusto dynamic encoding.

In the next phase, we consider the following additions:
 - `Variant`: future column-shredded dynamic optimization. Semantically similar (but not equivalent) to `KustoDynamic`, aligned with the [Iceberg Variant proposal](https://docs.google.com/document/d/1QjhpG_SVNPZh3anFcpicMQx90ebwjL7rmzFYfUP89Iw). Underlying basic type: composite (nested) type inferred from the unified schema of the values within the shard. A separate detailed design proposal will be provided for this.
 - `HLL`
 - `TDigest`
 - etc.

Initially, Kusto will produce Amudai shards with fields restricted to the following basic or extension types: `bool`, `i64`, `f64`, `String`, `DateTime`, `Guid`, `KustoTimeSpan`, `KustoDecimal`, `KustoDynamic`.

## Arrow interoperability

When Amudai shards are accessed by a basic Arrow reader (i.e., by consumers unaware of Kusto), the following type mappings and conversions are performed on the fly:
 - Integer and floating-point types (e.g., `u32`, `i64`, `f64`, etc.), as well as `bool` and `string`, are exposed as the corresponding Arrow types without conversion.
 - `Guid`: exposed as either a string or as the Arrow UUID extension type. Field metadata: `{ "ARROW:extension:name": "arrow.uuid" }`.
 - `DateTime`: converted to Arrow `Timestamp(nanosecond, UTC)`.
 - `KustoTimeSpan`: converted to Arrow `Duration(nanosecond)`.
 - `KustoDecimal`: ??? (string?)
 - `KustoDynamic`: converted to a JSON string. Field metadata: `{ "ARROW:extension:name": "arrow.json" }`.

When Amudai shards are created from Arrow record batches, the following default type conversions are applied:
 - Integer and floating-point types (e.g., `u32`, `i64`, `f64`, etc.), as well as `bool` and `string`, are mapped to the corresponding Amudai types without conversion.
 - `Timestamp`, `Date32`, and `Date64` are mapped to `DateTime`.
 - `Duration` is mapped to `KustoTimeSpan`.
 - `String` fields with `{ "ARROW:extension:name": "arrow.json" }` metadata are mapped to `KustoDynamic`.
 - `FixedSizeBinary(16)` fields with `{ "ARROW:extension:name": "arrow.uuid" }` metadata are mapped to `Guid`.

## Wrapping up: shard schema definition

Here's the conceptual definition of the Amudai shard schema:

```
enum RawType {
    Bool,
    Int8,
    Int16,
    ...
    Binary,
    FixedSizeBinary,
    String,
    ...
    List,
    FixedSizeList,
    Struct,
    Union,
    Map,
}

class DataType {
    string Name;            // May be null for unnamed type parameters
    RawType RawType;
    int? FixedSize;         // Non-null for types parameterized by fixed size
    DataType[] Children;    // Non-null and non-empty for composite types
    string ExtensionType;   // May be null.
}

class Schema {
    DataType[] Fields;
}
```

## Logical encoding outline

Although it is beyond the abstract type system scope, let's discuss the logical encoding of the composite types. Adopting Arrow's terminology, an _array_ is a columnar encoding of a sequence of values of a given type. Encoding here is used in an abstract sense: we ignore all the details of chunking, compression, dictionary, sparse optimizations, and placement of the data buffers in the storage blobs, and focus on the logical value positions and their relationships across parent and child arrays.

Array of **`List<T>`**:
 Encoded as two child arrays: `offsets` of type i64 and `values` of type T. `offsets` always starts from 0, followed by the logical end offsets of the corresponding list elements in `values`.
 For a list at logical position `i` in the lists array, its values are located at the logical range `offsets[i] .. offsets[i + 1]` in the `values` array.

Array of **`FixedSizeList<N, T>`**:
 Encoded as a single `values` child array of type T.
 For a list at logical position `i` in the lists array, its values are located at the logical range `i * N .. (i + 1) * N` in the `values` array.

Array of **`Struct<f0: T0, f1: T1, ...>`**:
 Encoded as N child arrays, each of the corresponding type T0, T1, ....
 There is a 1:1 correspondence between a parent struct logical position and its fields' logical positions in the child arrays. I.e. for a struct value at logical position `i`, its first field logical position in `f0` array is also `i`, its second field logical position in `f1` array is also `i`, etc.

Array of **`Map<K, V>`**:
 Encoded as 3 child arrays: `offsets` (type i64), `keys` (type K), `values` (type V).
 For a map at logical position `i` in the maps array, its keys are located at the logical range `offsets[i] .. offsets[i + 1]` in the `keys` array and its values are located at the logical range `offsets[i] .. offsets[i + 1]` in the `values` array.
 
 `Map<K, V>` is conceptually equivalent to `List<Struct<key: K, value: V>>`.

Array of **`Union<f0: T0, f1: T1, ...>`**:
 Encoded as N+1 child arrays: `discriminator` (u8), `f0` (T0), `f1` (T1), ....
 Value of a union at logical position `i` is obtained by fetching `v = discriminator[i]`, then fetching
 the value from `f{v}` at logical position `i`.
