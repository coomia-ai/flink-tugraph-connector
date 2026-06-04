# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-04

### Added
- **DataStream `Sink<T>`** (Flink Sink V2) writing vertices and edges to TuGraph-DB over Bolt, with
  a fluent `TuGraphSink.builder()`.
- **Table/SQL `DynamicTableSink`** under `'connector' = 'tugraph'`, discovered via SPI, sharing the
  same writer runtime as the DataStream path.
- **Idempotent `MERGE` writes** for at-least-once delivery that is effectively exactly-once at the
  business level (no duplicate vertices/edges on replay).
- **Batching** by size, time and checkpoint barrier, with synchronous flush back-pressure.
- **Fault tolerance**: exponential-backoff retry of transient Bolt failures via `TuGraphConnection`.
- **Pluggable Cypher generation** behind `CypherStatementBuilder` with a default
  `MergeCypherStatementBuilder` (parameterized `UNWIND ... MERGE ... SET x += $map`).
- **Type mapping** for scalar Flink types and a `null`-skipping policy (`RowDataToElementConverter`).
- **Missing-endpoint policy** for edges (`skip` / `fail`) with a `tugraph.edgeSkipped` metric.
- **Metrics**: `numRecordsSend`, `tugraph.flushCount`, `tugraph.flushLatencyMs`, `tugraph.edgeSkipped`.
- **Shaded jar** relocating the Bolt driver, Netty and Reactive Streams.
- Unit tests for Cypher generation, options validation and row conversion; Testcontainers-based
  integration tests (gated on `TUGRAPH_IT=1`).
- Runnable examples for DataStream and Flink SQL.

[Unreleased]: https://github.com/coomia-ai/flink-tugraph-connector/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/coomia-ai/flink-tugraph-connector/releases/tag/v0.1.0
