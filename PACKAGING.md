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
auto-commit `MERGE` per element** (sharing a Bolt session per flush). Throughput is therefore bound
by per-write round-trips, each of which is a disk-synced commit on the server.

### Measured baseline

| Metric | Value |
|--------|-------|
| Workload | 2000 vertices (3 props), `batchSize` 500, single subtask |
| Environment | TuGraph-DB 4.x over Bolt, development LAN instance |
| **Throughput** | **≈ 80 vertices/s per subtask** (≈ 12 ms / write) |

This is well below the original ≥ 5k rows/s aspiration (NFR-1), which **is not reachable on a single
connection given TuGraph's write model** (no `UNWIND` batching, no multi-statement transactions, a
disk-synced commit per `MERGE`). It is a property of TuGraph, not of the connector.

**To scale, increase sink parallelism** — each subtask uses its own connection, so throughput grows
roughly linearly (N subtasks ≈ N × per-subtask rate). A low-latency network to TuGraph also helps,
since per-write latency dominates.

Reproduce on your own hardware (gated on `TUGRAPH_LIVE`):

```bash
TUGRAPH_LIVE=1 ./gradlew test --tests *TuGraphBenchmarkIT   # override with TUGRAPH_BENCH_ROWS / _BATCH
```

> Record the measured throughput and the environment (TuGraph version, hardware, network) alongside
> your results — numbers vary widely with deployment.
