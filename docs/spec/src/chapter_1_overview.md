# Chapter 1: Overview

<!-- toc -->

Kusto's proprietary column store format is one of its main competitive advantages over competing technologies.
The format allows for both small size and super-fast analytics queries over structured, semi-structured, and unstructured
(text) data. The technology has matured over the last ten years of the product, and is now at its third main revision.

Project Amudai aims to improve Kusto's column store format in two ways:
(1) building on the product's long history and experience, to improve the format to allow Kusto to expand into new areas,
such as graph queries, which traditionally it has not tried to optimize for, and (2) to make this format open technology that
can be shared with other query engines both inside Microsoft and outside it.


This e-book is a working document to describe this new column store format.

## Scope

The scope of the project is to specify the new column store file format, and to publish at least one reader/writer open-source
implementation of the format. Our intention is to get this format accepted as part of [Apache Iceberg (TM)](https://iceberg.apache.org/)[^iceberg],
and we realize that this might require the project to contribute changes to Iceberg itself as well.

## Not in scope

We do not propose table metadata-level changes to Iceberg.

## How this book is organized

This book is organized as follows:

**TODO: Complete this part**

---

[^iceberg]: For the rest of this document, we will refer to Apache Iceberg (TM) simply as "Iceberg".
