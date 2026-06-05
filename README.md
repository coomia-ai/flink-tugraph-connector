# flink-tugraph-connector

[![Build](https://github.com/coomia-ai/flink-tugraph-connector/actions/workflows/ci.yml/badge.svg)](https://github.com/coomia-ai/flink-tugraph-connector/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Flink](https://img.shields.io/badge/Flink-1.20.x-E6526F.svg)](https://flink.apache.org/)
[![TuGraph](https://img.shields.io/badge/TuGraph--DB-4.x%20(Bolt)-00A0E9.svg)](https://github.com/TuGraph-family/tugraph-db)
[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://adoptium.net/)

An Apache Flink connector for **[TuGraph-DB](https://github.com/TuGraph-family/tugraph-db)** that
writes streaming vertices and edges into the graph over the **Bolt** protocol. It ships both a
**DataStream `Sink<T>`** and a **Table/SQL `DynamicTableSink`**, sharing one batching, retry and
idempotent-`MERGE` runtime.

> English | [中文](#中文文档)

TuGraph-DB has no official Flink connector — the project closes that gap with a clean, production-
oriented Bolt-based implementation modelled on the maturity of `nebula-flink-connector`.

---

## Features

- **Two APIs, one runtime** — DataStream `Sink<T>` (Sink V2) and Flink SQL `DynamicTableSink` share
  the same writer, batching and connection layer.
- **Idempotent writes** — every vertex/edge is written with `MERGE`, so an at-least-once pipeline is
  **effectively exactly-once at the business level** (no duplicate vertices or edges on replay).
- **Changelog / CDC** — consumes Flink's full changelog (INSERT / UPDATE / DELETE): upserts become
  `MERGE`, deletes become `DELETE`, applied in arrival order — so a CDC source keeps the graph in sync.
- **Batched & back-pressuring** — flushes by size or time, and on every checkpoint barrier; the
  synchronous flush naturally back-pressures the upstream.
- **Fault tolerant** — transient Bolt failures are retried with exponential backoff; exhausted
  retries restart the job, and the idempotent `MERGE` absorbs the replay.
- **Pluggable Cypher dialect** — Cypher generation sits behind `CypherStatementBuilder`, so a
  TuGraph build that lacks `SET x += $map` can be supported by swapping one class.
- **Observability** — standard `numRecordsSend` plus custom `tugraph.flushCount`,
  `tugraph.flushLatencyMs` and `tugraph.edgeSkipped` metrics.
- **Self-contained jar** — the Bolt driver (and Netty) are shaded and relocated to avoid classpath
  clashes on the Flink cluster.

## Capability matrix

| Dimension | Capability | Status |
|-----------|------------|--------|
| API | DataStream `Sink<T>` | ✅ v0.1 |
| API | Table/SQL `DynamicTableSink` | ✅ v0.1 |
| Direction | Sink (write) | ✅ v0.1 |
| Write mode | Upsert (`MERGE`) for vertices and edges | ✅ v0.1 |
| Write mode | Delete / changelog (INSERT/UPDATE/DELETE) | ✅ v0.1 |
| Direction | Source (bounded scan, vertex + edge) | ✅ v0.2 |
| Dim. table | `LookupTableSource` + `LookupCache` | ✅ v0.2 |
| Consistency | at-least-once + idempotent `MERGE` | ✅ v0.1 |
| Batching | size / time / checkpoint flush | ✅ v0.1 |
| Fault tolerance | retry + idempotent replay | ✅ v0.1 |
| Type mapping | scalar types (§ below) | ✅ v0.1 |
| Type mapping | nested ARRAY/MAP/ROW | ❌ no generic list/map type in TuGraph (see below) |
| Pushdown | projection | ✅ v0.2 |
| Pushdown | filter (`=,<>,>,>=,<,<=,IN`) | ✅ v0.2 |

## Version matrix

| Component | Version |
|-----------|---------|
| Apache Flink | **1.20.x** |
| TuGraph-DB | 4.x (Bolt, default port 7687) |
| neo4j-java-driver (Bolt) | 4.4.x |
| Java | 21 |
| Gradle | 8.10 (wrapper included) |

---

## Quick start

### Start TuGraph locally (optional)

A [`docker-compose.yml`](docker-compose.yml) is included to boot a local TuGraph-DB:

```bash
docker compose up -d    # Bolt at bolt://127.0.0.1:7687 (admin / 73@TuGraph), console at :7070
```

### Build

```bash
./gradlew build          # compile + unit tests + shaded jar
./gradlew shadowJar      # just the redistributable fat jar
```

The shaded jar `build/libs/flink-tugraph-connector-<version>.jar` bundles the relocated Bolt driver.
Drop it into the Flink `lib/` directory, or add it as a dependency of your job's uber-jar.

> Flink itself is `compileOnly` (provided) — the connector never bundles Flink, only the Bolt driver.

### Add as a dependency

```kotlin
dependencies {
    implementation("com.coomia.flink:flink-tugraph-connector:0.1.0")
}
```

## Prepare the TuGraph schema

TuGraph is **not schema-less** — the vertex and edge labels (with their primary key and properties)
must exist before the job runs. Create them once with TuGraph's DDL (e.g. over Bolt or in the
console):

```cypher
CALL db.createVertexLabel('Company', 'company_id',
  'company_id', 'STRING', false,
  'name', 'STRING', true,
  'reg_capital', 'DOUBLE', true);

CALL db.createEdgeLabel('INVEST', '[["Company","Company"]]',
  'ratio', 'DOUBLE', true);
```

The connector targets TuGraph's openCypher subset, so a few rules apply to the data you send:

- **Plain identifiers only** — labels and property names must match `[A-Za-z_][A-Za-z0-9_]*`
  (TuGraph does not support back-quoted identifiers); the connector validates this and fails fast.
- **Each element is one idempotent, parameterized `MERGE`** — TuGraph mis-binds per-row values in
  `UNWIND`-batched writes and rejects `SET n += $map`, so the connector writes one statement per
  vertex/edge. They share a Bolt session per flush; writes are **auto-commit** (TuGraph has no
  explicit transactions) and replay-safe.
- **Primary key is set by `MERGE`**, never re-assigned, so it is never overwritten.

> Verified end-to-end against TuGraph-DB 4.x over Bolt (DataStream job + idempotent replay).

## DataStream API

```java
DataStream<Vertex> vertices = ...; // Vertex extends GraphElement

vertices.sinkTo(TuGraphSink.<Vertex>builder()
        .uri("bolt://127.0.0.1:7687")
        .auth("admin", "73@TuGraph")
        .graph("default")
        .batchSize(500)
        .batchIntervalMs(1000)
        .maxRetries(3)
        .build());
```

Edges are written the same way with `TuGraphSink.<Edge>builder()`. See
[`VertexSinkExample`](src/main/java/com/coomia/flink/tugraph/examples/VertexSinkExample.java) and
[`EdgeSinkExample`](src/main/java/com/coomia/flink/tugraph/examples/EdgeSinkExample.java).

To **delete** instead of upsert, emit `vertex.asDelete()` / `edge.asDelete()` — the sink issues a
`DELETE` for those records (the DataStream mirror of the SQL changelog DELETE):

```java
stream.add(new Vertex("Company", "company_id", "c1", props).asDelete()); // removes :Company c1
```

## Flink SQL

```sql
CREATE TABLE company_vertex (
  company_id  STRING,
  name        STRING,
  reg_capital DOUBLE,
  PRIMARY KEY (company_id) NOT ENFORCED
) WITH (
  'connector'       = 'tugraph',
  'uri'             = 'bolt://127.0.0.1:7687',
  'username'        = 'admin',
  'password'        = '73@TuGraph',
  'graph'           = 'default',
  'element.type'    = 'vertex',
  'vertex.label'    = 'Company',
  'sink.batch.size' = '500'
);

INSERT INTO company_vertex SELECT company_id, name, reg_capital FROM kafka_src;
```

Edge table:

```sql
CREATE TABLE invest_edge (
  src_company STRING,
  dst_company STRING,
  ratio       DOUBLE
) WITH (
  'connector'                = 'tugraph',
  'uri'                      = 'bolt://127.0.0.1:7687',
  'username'                 = 'admin',
  'password'                 = '73@TuGraph',
  'element.type'             = 'edge',
  'edge.label'               = 'INVEST',
  'edge.src.label'           = 'Company',
  'edge.src.col'             = 'src_company',
  'edge.src.key'             = 'company_id',
  'edge.dst.label'           = 'Company',
  'edge.dst.col'             = 'dst_company',
  'edge.dst.key'             = 'company_id',
  'edge.on-missing-endpoint' = 'skip'
);
```

> `edge.src.col` is the **table column** carrying the endpoint key value; `edge.src.key` is the
> **vertex property** it is matched against (defaults to `edge.src.col` when omitted).

**Multiple relation types between the same pair.** If you model relations with a single edge label
discriminated by a property (e.g. one `REL` label with a `rel_type` column), set
`'edge.merge.keys' = 'rel_type'` so the property is folded into the MERGE match
(`MERGE (a)-[e:REL {rel_type: …}]->(b)`). Without it, two relations between the same two vertices
collapse into one edge (last-write-wins). Multiple keys are comma-separated.

**Out-of-order pipelines.** When vertices and edges arrive on separate, unordered jobs, set
`'edge.on-missing-endpoint' = 'create'` to MERGE a bare endpoint vertex (key only) when it is
missing, instead of `skip` (drop) or `fail` — making an at-least-once pipeline eventually consistent.

## Changelog & deletes

Vertex tables declare a **primary key**, so they accept Flink's full **upsert changelog**: `INSERT`
and `UPDATE_AFTER` become idempotent `MERGE`s, and `DELETE` becomes `MATCH … DETACH DELETE`. Point a
CDC source (Flink CDC, Debezium/Kafka, …) at a vertex table and the graph stays in sync, deletions
included. Edge tables have no primary key and are **append / upsert only** (insert-only changelog).

Operations are applied **in arrival order** within each flush, so an insert-then-delete of the same
key resolves correctly.

```sql
-- a CDC (changelog) source keeps :Company vertices in sync, including deletes
INSERT INTO company_vertex SELECT company_id, name, reg_capital FROM company_cdc;
```

## Reading from TuGraph (Source & Lookup) — v0.2

The same `'connector' = 'tugraph'` works as a **source**: a bounded vertex scan and a
dimension-table **lookup** for streaming enrichment. Projection is pushed down (only the requested
columns are returned). Reads run as standard `MATCH … RETURN …` over Bolt; the target label must
exist (see schema prerequisite above).

```sql
-- Bounded scan: read existing vertices for batch analysis / backfill
CREATE TABLE company_src (
  company_id  STRING,
  name        STRING,
  reg_capital DOUBLE
) WITH (
  'connector'          = 'tugraph',
  'uri'                = 'bolt://127.0.0.1:7687',
  'username'           = 'admin',
  'password'           = '73@TuGraph',
  'element.type'       = 'vertex',
  'vertex.label'       = 'Company',
  'vertex.primary-key' = 'company_id',   -- ORDER BY column for stable paging
  'scan.fetch-size'    = '1000'
);

SELECT company_id, name FROM company_src;
```

```sql
-- Dimension-table lookup: enrich a stream with vertex properties from TuGraph
CREATE TABLE company_dim (
  company_id STRING,
  name       STRING,
  PRIMARY KEY (company_id) NOT ENFORCED
) WITH (
  'connector'            = 'tugraph',
  'uri'                  = 'bolt://127.0.0.1:7687',
  'username'             = 'admin',
  'password'             = '73@TuGraph',
  'element.type'         = 'vertex',
  'vertex.label'         = 'Company',
  'lookup.cache.max-rows'= '10000',      -- 0 disables the LRU cache
  'lookup.cache.ttl'     = '10 min'
);

SELECT e.event_id, e.company_id, c.name
FROM events AS e
JOIN company_dim FOR SYSTEM_TIME AS OF e.proc_time AS c
  ON e.company_id = c.company_id;
```

Edges are read the same way with `element.type = edge` (the `edge.*` options from the sink), e.g.
[`edge_source_scan.sql`](examples/sql/edge_source_scan.sql) — projection is pushed down; the key
columns map to the endpoint vertices.

> Push-down: vertex scans apply projection + filter (`=,<>,>,>=,<,<=,IN`); edge scans apply
> projection.
>
> **Nested ARRAY/MAP/ROW are not supported.** TuGraph has no generic list/map/JSON property type
> (`Unknown type name`). Its non-scalar types are not usable over Bolt either: `FLOAT_VECTOR` cannot
> be written with Cypher `SET` (*"Function not implemented yet"*) and `BLOB` cannot be returned over
> Bolt (*"ToBolt meet unsupported data type"*). Serialize such columns to a STRING upstream.

## Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `uri` | ✔ | — | Bolt URI, e.g. `bolt://host:7687` |
| `username` / `password` | ✔ | — | Bolt basic auth |
| `graph` | | `default` | Target sub-graph / database |
| `element.type` | ✔ (SQL) | — | `vertex` or `edge` |
| `vertex.label` | vertex | — | Vertex label |
| `vertex.primary-key` | | from PK constraint | Primary-key column |
| `edge.label` | edge | — | Edge label |
| `edge.src.label` / `edge.src.col` | edge | — | Source label / table column |
| `edge.src.key` | | = `edge.src.col` | Source vertex match property |
| `edge.dst.label` / `edge.dst.col` | edge | — | Destination label / table column |
| `edge.dst.key` | | = `edge.dst.col` | Destination vertex match property |
| `edge.merge.keys` | | — | Edge property columns folded into the MERGE match (e.g. `rel_type`) |
| `edge.on-missing-endpoint` | | `skip` | `skip` (record metric), `fail`, or `create` (MERGE missing endpoint) |
| `sink.batch.size` | | `500` | Flush threshold (rows) |
| `sink.batch.interval.ms` | | `1000` | Flush threshold (time); `0` disables |
| `sink.max.retries` | | `3` | Transient-failure retries |
| `connection.timeout.ms` | | `15000` | Bolt connection timeout |
| `max.connection.pool.size` | | `10` | Bolt pool size per subtask |
| `scan.fetch-size` | | `1000` | Source: vertex scan page size (v0.2) |
| `lookup.cache.max-rows` | | `0` | Lookup cache size; `0` disables (v0.2) |
| `lookup.cache.ttl` | | — | Lookup cache entry TTL (v0.2) |
| `lookup.max-retries` | | `3` | Lookup query retries (v0.2) |

## Type mapping

| Flink `LogicalType` | TuGraph / Cypher value |
|---------------------|------------------------|
| `CHAR` / `VARCHAR` / `STRING` | `String` |
| `BOOLEAN` | `Boolean` |
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` | `Long` |
| `FLOAT` / `DOUBLE` / `DECIMAL` | `Double` |
| `DATE` / `TIME` / `TIMESTAMP` | ISO-8601 `String` |
| `NULL` value | skipped (never overwrites existing) |
| `ARRAY` / `MAP` / `ROW` | not yet (v0.2) |

## Consistency & fault tolerance

- **Semantics:** at-least-once. On every checkpoint Flink calls `flush`, so all buffered rows are
  durably written before the barrier completes.
- **Idempotency:** all writes use `MERGE` (vertices by primary key, edges by endpoints + label).
  Replaying after a restart updates existing elements instead of duplicating them — **effectively
  exactly-once** for the resulting graph.
- **Atomicity:** TuGraph has no explicit transactions, so each `MERGE` auto-commits independently
  (no all-or-nothing across a flush). A failure mid-flush leaves earlier writes applied; the next
  checkpoint replay re-applies them idempotently.
- **Property semantics:** properties are **fully overwritten** (`SET = latest value`); accumulative
  ("counter") semantics are not supported — pre-aggregate upstream if you need them.

## Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `numRecordsSend` | counter | elements written to TuGraph |
| `tugraph.flushCount` | counter | number of flush operations |
| `tugraph.flushLatencyMs` | gauge | last flush latency (ms) |
| `tugraph.edgeSkipped` | counter | edges skipped due to missing endpoints |
| `tugraph.deleted` | counter | elements deleted (changelog DELETE) |

## Architecture

```
        DataStream<Vertex|Edge>              Table/SQL RowData
                │                                  │
                ▼                                  ▼
         TuGraphSink (Sink V2)        TuGraphDynamicTableSink + Factory
                │                                  │ RowData→Element
                └───────────────┬──────────────────┘
                                ▼
                       TuGraphSinkWriter   (buffer / size·time·checkpoint flush / metrics)
                                ▼
                     CypherStatementBuilder  (Element[] → parameterized UNWIND MERGE)
                                ▼
                       TuGraphConnection (Bolt + retry)
                                ▼
                          TuGraph-DB :7687
```

## Building & testing

```bash
./gradlew test                       # unit tests only (no Docker required)

# Integration tests need Docker + a reachable TuGraph image:
TUGRAPH_IT=1 ./gradlew test          # Windows PowerShell: $env:TUGRAPH_IT=1; ./gradlew test
```

Integration tests (`*IT` / `*ITCase`) boot a TuGraph container with
[Testcontainers](https://testcontainers.com/) and are skipped unless `TUGRAPH_IT=1`. Override the
image with `TUGRAPH_IMAGE` and credentials with `TUGRAPH_USERNAME` / `TUGRAPH_PASSWORD`.

For the shaded-jar layout, cluster deployment and the performance benchmark, see
[PACKAGING.md](PACKAGING.md).

## Examples

Runnable examples live under
[`examples/`](src/main/java/com/coomia/flink/tugraph/examples/) (Java) and
[`examples/sql/`](examples/sql/) (SQL scripts):

| Example | API | Shows |
|---------|-----|-------|
| [`VertexSinkExample`](src/main/java/com/coomia/flink/tugraph/examples/VertexSinkExample.java) / [`EdgeSinkExample`](src/main/java/com/coomia/flink/tugraph/examples/EdgeSinkExample.java) | DataStream | upsert vertices / edges |
| [`DataStreamDeleteExample`](src/main/java/com/coomia/flink/tugraph/examples/DataStreamDeleteExample.java) | DataStream | upsert + `asDelete()` (delete) |
| [`SqlSinkExample`](src/main/java/com/coomia/flink/tugraph/examples/SqlSinkExample.java) | Flink SQL | write vertices & edges |
| [`SqlSourceExample`](src/main/java/com/coomia/flink/tugraph/examples/SqlSourceExample.java) | Flink SQL | scan + projection/filter push-down, lookup join |
| [`source_scan.sql`](examples/sql/source_scan.sql) / [`edge_source_scan.sql`](examples/sql/edge_source_scan.sql) / [`lookup_join.sql`](examples/sql/lookup_join.sql) | Flink SQL | vertex / edge scan, dimension-table lookup |

## Roadmap

- **v0.2** — bounded `ScanTableSource` (vertex ✅ + edge ✅), `LookupTableSource` with `LookupCache`
  ✅, projection ✅ and filter ✅ (vertex) push-down. Nested ARRAY/MAP/ROW are not supported
  (TuGraph stores scalar properties).
- **v0.3 (not planned)** — TuGraph has no native change-data-capture (no binlog / subscription), so
  an unbounded / CDC source over Bolt is not feasible. Capture changes with an external CDC source
  upstream (e.g. Flink CDC / Debezium) and write them through this sink's changelog support.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Commits follow
[Conventional Commits](https://www.conventionalcommits.org/) in English.

## License

Licensed under the [Apache License 2.0](LICENSE).

---

## 中文文档

面向 **[TuGraph-DB](https://github.com/TuGraph-family/tugraph-db)** 的 Apache Flink 连接器，通过
**Bolt** 协议把流式的点 / 边写入图数据库。同时提供 **DataStream `Sink<T>`** 与
**Table/SQL `DynamicTableSink`** 两套 API，底层共用攒批、重试与幂等 `MERGE` 运行时。

> [English](#flink-tugraph-connector) | 中文

TuGraph-DB 没有官方 Flink 连接器，本项目以 `nebula-flink-connector` 的成熟度为标杆，基于 Bolt 提供
一个面向生产、代码优雅的实现来补齐这一空缺。

### 功能特性

- **两套 API、一个运行时**：DataStream `Sink<T>`（Sink V2）与 Flink SQL `DynamicTableSink` 共用同一个
  writer、攒批与连接层。
- **幂等写入**：点 / 边均以 `MERGE` 写入，因此 at-least-once 管线在**业务层等价于 exactly-once**
  （故障重放不会产生重复点边）。
- **Changelog / CDC**：消费 Flink 完整 changelog（INSERT/UPDATE/DELETE）—— upsert 走 `MERGE`、删除走
  `DELETE`，按到达顺序应用，CDC 源可让图实时同步（含删除）。
- **攒批与反压**：按条数、时间以及每次 checkpoint barrier 触发 flush；同步刷写天然对上游反压。
- **容错**：Bolt 瞬时错误指数退避重试；重试耗尽则重启作业，幂等 `MERGE` 吸收重放。
- **可插拔 Cypher 方言**：Cypher 生成位于 `CypherStatementBuilder` 之后，若目标 TuGraph 不支持
  `SET x += $map`，替换一个实现类即可降级。
- **可观测**：标准 `numRecordsSend` 加自定义 `tugraph.flushCount`、`tugraph.flushLatencyMs`、
  `tugraph.edgeSkipped` 指标。
- **自包含 jar**：Bolt 驱动（及 Netty）被 shade 并 relocate，避免与 Flink 集群上的其它依赖冲突。

### 版本矩阵

| 组件 | 版本 |
|------|------|
| Apache Flink | **1.20.x** |
| TuGraph-DB | 4.x（Bolt，默认端口 7687） |
| neo4j-java-driver（Bolt） | 4.4.x |
| Java | 21 |
| Gradle | 8.10（已内置 wrapper） |

### 本地启动 TuGraph（可选）

仓库内置 [`docker-compose.yml`](docker-compose.yml)，可一键拉起本地 TuGraph-DB：

```bash
docker compose up -d    # Bolt 端口 bolt://127.0.0.1:7687（admin / 73@TuGraph），控制台 :7070
```

### 构建

```bash
./gradlew build          # 编译 + 单元测试 + shaded jar
./gradlew shadowJar      # 仅产出可分发的 fat jar
```

产物 `build/libs/flink-tugraph-connector-<version>.jar` 已内置 relocate 后的 Bolt 驱动，可放入 Flink
`lib/` 目录，或作为作业 uber-jar 的依赖。Flink 依赖为 `compileOnly`（provided），由集群在运行时提供。

### 准备 TuGraph Schema

TuGraph **不是 schema-less** —— 点 / 边的 label（含主键与属性）必须在作业运行前已存在。用 TuGraph 的
DDL 预先创建一次（可通过 Bolt 或控制台）：

```cypher
CALL db.createVertexLabel('Company', 'company_id',
  'company_id', 'STRING', false,
  'name', 'STRING', true,
  'reg_capital', 'DOUBLE', true);

CALL db.createEdgeLabel('INVEST', '[["Company","Company"]]',
  'ratio', 'DOUBLE', true);
```

连接器面向 TuGraph 的 openCypher 子集，对写入数据有几条约束：

- **仅支持纯标识符**：label 与属性名需匹配 `[A-Za-z_][A-Za-z0-9_]*`（TuGraph 不支持反引号标识符），
  连接器会校验并快速失败。
- **每个元素一条幂等参数化 `MERGE`**：TuGraph 在 `UNWIND` 批量写时会错误绑定逐行取值、且不支持
  `SET n += $map`，因此连接器对每个点 / 边生成一条语句；同一次 flush 复用一个 Bolt 会话，写入为
  **自动提交**（TuGraph 无显式事务），重放安全。
- **主键由 `MERGE` 设定**、不会被再次赋值，因此不会被覆盖。

> 已在 TuGraph-DB 4.x（Bolt）上端到端实测通过（DataStream 作业 + 幂等重放）。

### DataStream 用法

```java
vertices.sinkTo(TuGraphSink.<Vertex>builder()
        .uri("bolt://127.0.0.1:7687")
        .auth("admin", "73@TuGraph")
        .graph("default")
        .batchSize(500)
        .batchIntervalMs(1000)
        .maxRetries(3)
        .build());
```

边用 `TuGraphSink.<Edge>builder()`，示例见
[`VertexSinkExample`](src/main/java/com/coomia/flink/tugraph/examples/VertexSinkExample.java) 与
[`EdgeSinkExample`](src/main/java/com/coomia/flink/tugraph/examples/EdgeSinkExample.java)。

要**删除**而非 upsert，发送 `vertex.asDelete()` / `edge.asDelete()` 即可（DataStream 侧对应 SQL
changelog 的 DELETE）：

```java
stream.add(new Vertex("Company", "company_id", "c1", props).asDelete()); // 删除 :Company c1
```

### Flink SQL 用法

点表与边表的建表语句见上文英文部分。要点：
- `element.type` 取 `vertex` / `edge`；
- 边的 `edge.src.col` 是**表中承载端点键值的列**，`edge.src.key` 是该值要匹配的**顶点属性名**
  （省略时默认等于 `edge.src.col`）；
- `edge.on-missing-endpoint` 取 `skip`（默认，记录指标）或 `fail`（抛异常重启）。

### 配置项、类型映射、指标

详见上文英文表格（配置项 / 类型映射 / 指标含义一致）。

### Changelog 与删除

点表声明了**主键**，因此接受 Flink 完整 **upsert changelog**：`INSERT`/`UPDATE_AFTER` → 幂等 `MERGE`，
`DELETE` → `MATCH … DETACH DELETE`。把 CDC 源（Flink CDC、Debezium/Kafka 等）接到点表，图即实时同步
（含删除）。边表无主键，为 **append / upsert only**（insert-only）。同一次 flush 内按到达顺序应用。

### 读取（Source 与维表 Lookup，v0.2）

同一个 `'connector' = 'tugraph'` 也能作为 **source**：有界点扫描 + 维表 **lookup**（流式 enrich），并下推
projection（只取所需列）。读走标准 `MATCH … RETURN …`，目标 label 须预先存在。建表 / join 写法见上文英文
部分，配置见配置表中的 `scan.fetch-size`、`lookup.cache.max-rows`、`lookup.cache.ttl`、`lookup.max-retries`。
支持读取**点与边**（`element.type=edge` 用 `edge.*` 配置）；点扫描下推 projection+filter，边扫描下推
projection。**不支持嵌套 ARRAY/MAP/ROW**:TuGraph 无通用 list/map/JSON 属性类型(`Unknown type name`);
其非标量类型也走不通 Bolt —— `FLOAT_VECTOR` 不能用 Cypher `SET` 写入("Function not implemented yet"),
`BLOB` 不能经 Bolt 返回("ToBolt meet unsupported data type")。如需此类列请在上游序列化为 STRING。

### 一致性与容错

- **语义**：at-least-once。每次 checkpoint 调用 `flush`，barrier 完成前缓冲数据已落库。
- **幂等**：全部走 `MERGE`（点按主键、边按端点 + 边 label），重放只更新不重复 —— 结果图**业务可见
  exactly-once**。
- **属性语义**：属性为**全量覆盖**（`SET = 最新值`），不支持累加 / 计数语义，累加场景请在上游预聚合。

### 测试

```bash
./gradlew test                 # 仅单元测试，无需 Docker
$env:TUGRAPH_IT=1; ./gradlew test   # 集成测试（需 Docker + 可用 TuGraph 镜像）
```

集成测试（`*IT` / `*ITCase`）用 Testcontainers 启动 TuGraph 容器，未设置 `TUGRAPH_IT=1` 时自动跳过；
可用 `TUGRAPH_IMAGE` 覆盖镜像，`TUGRAPH_USERNAME` / `TUGRAPH_PASSWORD` 覆盖凭据。

### 路线图

- **v0.2**：有界 `ScanTableSource`（点 ✅ + 边 ✅）、带缓存的维表 `LookupTableSource` ✅、projection ✅
  与 filter ✅（点）下推。TuGraph 仅存标量属性，不支持嵌套 ARRAY/MAP/ROW。
- **v0.3（暂不做）**：TuGraph 无原生变更订阅 / binlog，Bolt 上无法实现无界 / CDC Source；如需 CDC，在
  上游用外部 CDC 源（Flink CDC / Debezium）捕获变更，再经本 Sink 的 changelog 能力入图。

### 贡献与许可

欢迎贡献，详见 [CONTRIBUTING.md](CONTRIBUTING.md)；提交信息使用英文的
[Conventional Commits](https://www.conventionalcommits.org/) 规范。本项目以
[Apache License 2.0](LICENSE) 开源。
