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
> TuGraph-DB 4.x. The `CypherStatementBuilder` interface lets you swap the generated dialect if a
> specific TuGraph build does not support `UNWIND` / `SET x += $map`.

## Performance benchmark

A single sink subtask is expected to sustain **≥ 5k rows/s** writing vertices (batch size 500),
network and TuGraph write contention permitting (NFR-1).

To reproduce on your own hardware:

1. Start a TuGraph-DB 4.x instance with Bolt enabled.
2. Run a DataStream job that generates synthetic vertices (e.g. a `DataGeneratorSource`) into
   `TuGraphSink` with `batchSize=500`, parallelism 1.
3. Read `numRecordsSend` and `tugraph.flushLatencyMs` from the Flink metrics / web UI.
4. Tune `sink.batch.size`, `sink.batch.interval.ms`, `max.connection.pool.size` and job parallelism
   for your workload.

> Record the measured throughput and the environment (TuGraph version, hardware, network) alongside
> your results. Numbers vary widely with deployment, so this connector ships the knobs rather than a
> single headline figure.
