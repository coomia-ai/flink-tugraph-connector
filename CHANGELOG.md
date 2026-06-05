# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Table/SQL source (v0.2)** under the same `'connector' = 'tugraph'`:
  - **Bounded vertex scan** (`ScanTableSource`) — paginated `MATCH … RETURN … ORDER BY pk SKIP/LIMIT`
    via an `InputFormat`.
  - **Dimension-table lookup** (`LookupTableSource`) — point lookup by join key for streaming
    enrichment, with an optional `LookupCache` (`lookup.cache.max-rows` / `lookup.cache.ttl`).
  - **Projection push-down** (`SupportsProjectionPushDown`) — only the requested columns are read.
  - **Filter push-down** (`SupportsFilterPushDown`) — `=, <>, >, >=, <, <=, IN` predicates between a
    column and a literal are translated to a parameterized Cypher `WHERE`; the rest stay in Flink.
  - New read options `scan.fetch-size`, `lookup.cache.max-rows`, `lookup.cache.ttl`,
    `lookup.max-retries`; `TuGraphConnection.read`, `CypherQueryBuilder` and a row-to-`RowData`
    converter. Verified end-to-end (scan + projection + lookup) against live TuGraph-DB 4.x.
  - v0.2 reads vertices only; edge source, filter push-down and nested types are planned.

## [0.1.0] - 2026-06-04

### Added
- **DataStream `Sink<T>`** (Flink Sink V2) writing vertices and edges to TuGraph-DB over Bolt, with
  a fluent `TuGraphSink.builder()`.
- **Changelog / CDC support** — vertex tables consume Flink's full upsert changelog: `INSERT` /
  `UPDATE_AFTER` become idempotent `MERGE`, `DELETE` becomes `MATCH … DETACH DELETE`, edges deleted
  with `MATCH … DELETE`. Operations are applied in arrival order; a `tugraph.deleted` metric is
  exposed. Verified end-to-end via a Flink SQL changelog against live TuGraph-DB 4.x.
- **DataStream delete API** — `GraphElement` carries a change op (like Flink's `RowKind`); emit
  `vertex.asDelete()` / `edge.asDelete()` to delete from the DataStream API, matching the SQL path.
- **Table/SQL `DynamicTableSink`** under `'connector' = 'tugraph'`, discovered via SPI, sharing the
  same writer runtime as the DataStream path.
- **Idempotent `MERGE` writes** for at-least-once delivery that is effectively exactly-once at the
  business level (no duplicate vertices/edges on replay).
- **Batching** by size, time and checkpoint barrier, with synchronous flush back-pressure.
- **Fault tolerance**: exponential-backoff retry of transient Bolt failures via `TuGraphConnection`.
- **Pluggable Cypher generation** behind `CypherStatementBuilder` with a default
  `MergeCypherStatementBuilder` tuned for TuGraph's openCypher subset: plain (non-back-quoted)
  identifiers, one idempotent parameterized `MERGE` per element, per-property `SET`, and auto-commit
  writes (TuGraph has no explicit transactions). Verified end-to-end against TuGraph-DB 4.x.
- **Schema prerequisite** documented — TuGraph is not schema-less, so vertex/edge labels must be
  created before the job runs.
- **Type mapping** for scalar Flink types and a `null`-skipping policy (`RowDataToElementConverter`).
- **Missing-endpoint policy** for edges (`skip` / `fail`) with a `tugraph.edgeSkipped` metric.
- **Metrics**: `numRecordsSend`, `tugraph.flushCount`, `tugraph.flushLatencyMs`, `tugraph.edgeSkipped`.
- **Shaded jar** relocating the Bolt driver, Netty and Reactive Streams.
- Unit tests for Cypher generation, options validation and row conversion; Testcontainers-based
  integration tests (gated on `TUGRAPH_IT=1`).
- Runnable examples for DataStream and Flink SQL.

[Unreleased]: https://github.com/coomia-ai/flink-tugraph-connector/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/coomia-ai/flink-tugraph-connector/releases/tag/v0.1.0
