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
| Direction | Source (bounded scan) | 🟡 v0.2 (planned) |
| Dim. table | `LookupTableSource` + cache | 🟡 v0.2 (planned) |
| Consistency | at-least-once + idempotent `MERGE` | ✅ v0.1 |
| Batching | size / time / checkpoint flush | ✅ v0.1 |
| Fault tolerance | retry + idempotent replay | ✅ v0.1 |
| Type mapping | scalar types (§ below) | ✅ v0.1 |
| Type mapping | nested ARRAY/MAP/ROW | 🟡 v0.2 (planned) |
| Pushdown | projection / filter | 🟡 v0.2 (planned) |

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
| `edge.on-missing-endpoint` | | `skip` | `skip` (record metric) or `fail` |
| `sink.batch.size` | | `500` | Flush threshold (rows) |
| `sink.batch.interval.ms` | | `1000` | Flush threshold (time); `0` disables |
| `sink.max.retries` | | `3` | Transient-failure retries |
| `connection.timeout.ms` | | `15000` | Bolt connection timeout |
| `max.connection.pool.size` | | `10` | Bolt pool size per subtask |

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
- **Property semantics:** properties are **fully overwritten** (`SET = latest value`); accumulative
  ("counter") semantics are not supported — pre-aggregate upstream if you need them.

## Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `numRecordsSend` | counter | elements written to TuGraph |
| `tugraph.flushCount` | counter | number of flush operations |
| `tugraph.flushLatencyMs` | gauge | last flush latency (ms) |
| `tugraph.edgeSkipped` | counter | edges skipped due to missing endpoints |

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

## Roadmap

- **v0.2** — bounded `ScanTableSource`, `LookupTableSource` with `LookupCache`, projection/filter
  pushdown, nested type mapping.
- **v0.3** — unbounded / CDC source (pending TuGraph change-subscription support).

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

### Flink SQL 用法

点表与边表的建表语句见上文英文部分。要点：
- `element.type` 取 `vertex` / `edge`；
- 边的 `edge.src.col` 是**表中承载端点键值的列**，`edge.src.key` 是该值要匹配的**顶点属性名**
  （省略时默认等于 `edge.src.col`）；
- `edge.on-missing-endpoint` 取 `skip`（默认，记录指标）或 `fail`（抛异常重启）。

### 配置项、类型映射、指标

详见上文英文表格（配置项 / 类型映射 / 指标含义一致）。

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

- **v0.2**：有界 `ScanTableSource`、带缓存的维表 `LookupTableSource`、projection/filter 下推、复杂类型。
- **v0.3**：无界 / CDC Source（取决于 TuGraph 变更订阅能力）。

### 贡献与许可

欢迎贡献，详见 [CONTRIBUTING.md](CONTRIBUTING.md)；提交信息使用英文的
[Conventional Commits](https://www.conventionalcommits.org/) 规范。本项目以
[Apache License 2.0](LICENSE) 开源。
