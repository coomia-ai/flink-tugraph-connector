# Packaging & Deployment

This guide covers building the connector, the shaded-jar layout, deployment onto a Flink cluster,
and how to run the performance benchmark.

## Build artifacts

```bash
./gradlew build
```

produces in `build/libs/`:

| Artifact | Purpose |
|----------|---------|
| `flink-tugraph-connector-<version>.jar` | **Shaded** jar with the relocated Bolt driver — this is what you deploy. |
| `flink-tugraph-connector-<version>-sources.jar` | Sources, for IDE navigation / Maven Central. |
| `flink-tugraph-connector-<version>-javadoc.jar` | Javadoc, for Maven Central. |

### What is shaded

Flink is **provided** (`compileOnly`) and never bundled. The shaded jar contains only the runtime
dependencies, relocated to avoid classpath conflicts on the cluster:

| Original package | Relocated to |
|------------------|--------------|
| `org.neo4j.driver` | `com.coomia.flink.tugraph.shaded.neo4j.driver` |
| `io.netty` | `com.coomia.flink.tugraph.shaded.netty` |
| `org.reactivestreams` | `com.coomia.flink.tugraph.shaded.reactivestreams` |

The `META-INF/services/org.apache.flink.table.factories.Factory` SPI entry is preserved so Flink SQL
discovers the `tugraph` connector automatically.

## Deploying onto a Flink cluster

Choose one of:

1. **Cluster lib** — copy the shaded jar into `$FLINK_HOME/lib/` on every node and restart the
   cluster. The connector is then available to all jobs (including the SQL client / gateway).
2. **Job uber-jar** — depend on the connector and shade it into your job jar (recommended for
   per-job isolation).

For the SQL client, also `ADD JAR '/path/to/flink-tugraph-connector-<version>.jar';` or place it in
`lib/`.

## Compatibility

| Connector | Flink | TuGraph-DB | Bolt driver | Java |
|-----------|-------|------------|-------------|------|
| 0.1.x | 1.20.x | 4.x (Bolt 7687) | neo4j-java-driver 4.4.x | 21 |

> The Bolt driver line is pinned to 4.4.x because it is the version officially verified against
> TuGraph-DB 4.x. The default `MergeCypherStatementBuilder` is tuned for TuGraph's openCypher subset
> (plain identifiers, one idempotent parameterized `MERGE` per element, per-property `SET`,
> auto-commit); the `CypherStatementBuilder` interface lets you swap it for another dialect if needed.
>
> **Schema is not created by the connector.** TuGraph is not schema-less — create the target vertex
> and edge labels (with their primary key and properties) before running the job (see the README).

## Performance benchmark

Because TuGraph rejects `UNWIND`-batched and multi-statement writes, the connector issues **one
auto-commit `MERGE` per element**. Throughput is bound by per-write round-trips, each a disk-synced
commit on the server. The sink mitigates this by writing **order-independent flushes concurrently**
over the Bolt connection pool (a vertices-only or edges-only upsert flush; mixed/delete flushes stay
sequential — see `TuGraphSinkWriter`).

### Measured baseline

| Path | Throughput (single subtask) |
|------|-----------------------------|
| Sequential (1 connection) | ≈ 80 vertices/s |
| Concurrent (pool 16) | ≈ 305 vertices/s |

*2000 vertices (3 props) into TuGraph-DB 4.x over Bolt on a development LAN instance.*

The concurrent path is ~3.8× faster, not 16×, because TuGraph serializes commits server-side (one
disk-synced write at a time). All of this is still well below the original ≥ 5k rows/s aspiration
(NFR-1), which **is not reachable** given TuGraph's write model — a property of TuGraph, not of the
connector.

**To scale further, increase sink parallelism** (each subtask has its own pool) and raise
`max.connection.pool.size`; a low-latency network helps since per-write latency dominates.

Reproduce on your own hardware (gated on `TUGRAPH_LIVE`):

```bash
TUGRAPH_BENCH=1 ./gradlew test --tests *TuGraphBenchmarkIT   # override with TUGRAPH_BENCH_ROWS / _CONCURRENCY
```

> Record the measured throughput and the environment (TuGraph version, hardware, network) alongside
> your results — numbers vary widely with deployment.
