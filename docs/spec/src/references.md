# References

<!-- toc -->

## Established ecosystem
 - [Parquet](https://parquet.apache.org/docs/)
   - [Encoding definitions](https://github.com/apache/parquet-format/blob/master/Encodings.md)
   - *Variant* data type (under development, not formally adopted) - [encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md), [shredding](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
 - [ORC](https://cwiki.apache.org/confluence/display/hive/languagemanual+orc)
 - [Arrow](https://arrow.apache.org/docs/index.html): columnar memory format
 - [Iceberg](https://iceberg.apache.org/): open table format for analytic datasets
   - *Variant* data type: [proposal](https://docs.google.com/document/d/1QjhpG_SVNPZh3anFcpicMQx90ebwjL7rmzFYfUP89Iw/edit?tab=t.0#heading=h.rt0cvesdzsj7)

## Data encodings
 - [BtrBlocks](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf): Efficient Columnar Compression for Data Lakes
 - [Fast Static Symbol Table (FSST)](https://github.com/cwida/fsst): fast text compression that allows random access
 - [FastLanes](https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf) compression
   - Nested data type encodings in FastLanes: [paper](https://homepages.cwi.nl/~boncz/msc/2024-ZiyaMukhtarov.pdf), [video](https://www.youtube.com/watch?v=brIsMp2CG3U)
 - [ALP](https://dl.acm.org/doi/pdf/10.1145/3626717): Adaptive Lossless Floating-Point Compression
 - [Decoding billions of integers per second through vectorization](https://arxiv.org/pdf/1209.2137)
 - [Split block Bloom filters](https://arxiv.org/pdf/2101.01719)

## Parquet alternatives and competitors
 - [Vortex](https://github.com/spiraldb/vortex)
   - [Spiral engineering blog](https://blog.spiraldb.com/)
 - [Lance](https://lancedb.github.io/lance/)
   - [Lance v2: A columnar container format for modern data](https://blog.lancedb.com/lance-v2/)
 - [Nimble](https://github.com/facebookincubator/nimble)
   - [video](https://www.youtube.com/watch?v=bISBNVtXZ6M)
 - [An Empirical Evaluation of Columnar Storage Formats](https://www.vldb.org/pvldb/vol17/p148-zeng.pdf): Lessons learned from evaluation of Parquet
 and ORC for guiding future innovations in columnar storage formats

## Engines, systems and architecture
 - [Procella](https://storage.googleapis.com/gweb-research2023-media/pubtools/5226.pdf): Unifying serving and analytical data at YouTube
 - [Dremel](https://www.vldb.org/pvldb/vol13/p3461-melnik.pdf): A Decade of Interactive SQL Analysis at WebScale
 - [Exploiting Cloud Object Storage for High-Performance Analytics](https://www.durner.dev/app/media/papers/anyblob-vldb23.pdf)
 - [DuckDB](https://duckdb.org/): in-process analytical DB
   - [Lightweight Compression in DuckDB](https://duckdb.org/2022/10/28/lightweight-compression.html)
   - DuckDB internals: [slides](https://15721.courses.cs.cmu.edu/spring2023/slides/22-duckdb.pdf), [video](https://www.youtube.com/watch?v=bZOvAKGkzpQ)
   - [Table file format](https://tangdh.life/posts/database/duckdb-file-en/) (unofficial)
 - [ClickHouse](https://clickhouse.com/)
   - [JSON data type](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
   - [Lightning Fast Analytics for Everyone](https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf)
 - [Velox](https://github.com/facebookincubator/velox): composable execution engine as a library
