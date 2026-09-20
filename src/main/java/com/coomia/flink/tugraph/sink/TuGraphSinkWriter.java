/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.coomia.flink.tugraph.sink;

import com.coomia.flink.tugraph.TuGraphSinkOptions;
import com.coomia.flink.tugraph.TuGraphSinkOptions.OnMissingEndpoint;
import com.coomia.flink.tugraph.TuGraphSinkOptions.OnMissingLabel;
import com.coomia.flink.tugraph.client.TuGraphConnection;
import com.coomia.flink.tugraph.client.TuGraphConnection.BatchWriteResult;
import com.coomia.flink.tugraph.cypher.CypherStatement;
import com.coomia.flink.tugraph.cypher.CypherStatementBuilder;
import com.coomia.flink.tugraph.cypher.NativeBulkStatementBuilder;
import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.GraphElement;
import com.coomia.flink.tugraph.element.Vertex;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Buffers graph change operations and flushes them to TuGraph as idempotent {@code MERGE} (upsert)
 * or {@code DELETE} statements. Eligible homogeneous upsert batches use TuGraph 4.5.2 native
 * {@code db.upsertVertex}/{@code db.upsertEdge} procedures; batches that require ordering,
 * endpoint creation, custom edge identity, or record-level label skipping retain the MERGE path.
 *
 * <p><b>Changelog.</b> Each buffered record carries whether it is an upsert or a delete (derived
 * from the Flink {@code RowKind} on the Table path, or always upsert for the DataStream identity
 * path). The buffer is flushed <em>in arrival order</em> so an insert-then-delete (or vice versa)
 * of the same key reaches TuGraph in the correct order. Native procedures are used only when this
 * ordering contract is provably safe; the legacy MERGE builder remains the compatibility path.
 *
 * <p><b>Concurrency optimization.</b> When a flush contains only upserts of a single kind
 * (vertices-only or edges-only) the statements are order-independent (idempotent {@code MERGE}, and
 * edge endpoints come from earlier flushes), so they are written concurrently over the Bolt
 * connection pool — TuGraph's per-statement, disk-synced commits make this the main throughput lever
 * within a subtask. Mixed (vertex+edge) or delete-containing flushes stay strictly sequential to
 * preserve ordering / endpoint dependencies.
 *
 * <p><b>Threading.</b> {@link #write}, {@link #flush} and the processing-time callback all run on
 * the single task thread (Flink's mailbox); the buffer needs no synchronization. Parallel writes use
 * a private pool and the flush blocks until they complete (so flushing still back-pressures).
 *
 * <p><b>Failure recovery.</b> A timer-triggered flush keeps the buffer on failure. Transient
 * connection failures are retried by the next timer or synchronously before the next
 * {@link #write}/{@link #flush}; a successful replay clears the recorded failure. Deterministic
 * failures are surfaced on the task thread so they go through Flink's regular failure handling.
 *
 * @param <InputT> upstream record type
 */
public class TuGraphSinkWriter<InputT> implements SinkWriter<InputT> {

    private static final Logger LOG = LoggerFactory.getLogger(TuGraphSinkWriter.class);

    private static final int MAX_WRITE_THREADS = 16;

    /** A buffered change operation: a graph element plus whether it is a delete. */
    private static final class Op {
        final GraphElement element;
        final boolean delete;

        Op(GraphElement element, boolean delete) {
            this.element = element;
            this.delete = delete;
        }
    }

    private final TuGraphSinkOptions options;
    private final ElementConverter<InputT> converter;
    private final CypherStatementBuilder cypherBuilder;
    private final TuGraphConnection connection;
    private final ProcessingTimeService timeService;
    private final ExecutorService writeExecutor;

    private final List<Op> buffer;

    // ---- Metrics ----
    private final Counter numRecordsSend;
    private final Counter flushCounter;
    private final Counter edgeSkippedCounter;
    private final Counter vertexSkippedCounter;
    private final Counter deletedCounter;
    private final Counter retryAttemptsCounter;
    private final Counter asyncFlushFailuresCounter;
    private volatile long lastFlushLatencyMs;

    /** Failure from a timer-triggered flush, rethrown on the task thread by write/flush. */
    private volatile Exception asyncFlushException;
    private volatile long asyncFlushFailureStartedNanos;

    private boolean closed;

    public TuGraphSinkWriter(TuGraphSinkOptions options,
                             ElementConverter<InputT> converter,
                             CypherStatementBuilder cypherBuilder,
                             SinkWriterMetricGroup metricGroup,
                             ProcessingTimeService timeService) {
        this(options, converter, cypherBuilder, metricGroup, timeService, null);
    }

    /** Test seam for supplying a deterministic connection implementation. */
    TuGraphSinkWriter(TuGraphSinkOptions options,
                      ElementConverter<InputT> converter,
                      CypherStatementBuilder cypherBuilder,
                      SinkWriterMetricGroup metricGroup,
                      ProcessingTimeService timeService,
                      TuGraphConnection suppliedConnection) {
        this.options = options;
        this.converter = converter;
        this.cypherBuilder = cypherBuilder;
        this.timeService = timeService;
        this.buffer = new ArrayList<>(options.batchSize());

        int threads = Math.min(Math.max(1, options.maxConnectionPoolSize()), MAX_WRITE_THREADS);
        this.writeExecutor = Executors.newFixedThreadPool(threads);

        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        MetricGroup tg = metricGroup.addGroup("tugraph");
        this.flushCounter = tg.counter("flushCount");
        this.edgeSkippedCounter = tg.counter("edgeSkipped");
        this.vertexSkippedCounter = tg.counter("vertexSkipped");
        this.deletedCounter = tg.counter("deleted");
        this.retryAttemptsCounter = tg.counter("retryAttempts");
        this.asyncFlushFailuresCounter = tg.counter("asyncFlushFailures");
        tg.gauge("flushLatencyMs", () -> lastFlushLatencyMs);

        this.connection = suppliedConnection != null
                ? suppliedConnection
                : new TuGraphConnection(options, retryAttemptsCounter::inc);
        this.connection.open();

        scheduleNextTimer();
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        checkAsyncFlushException();
        GraphElement converted = converter.convert(element);
        if (converted == null) {
            return; // dropped (e.g. the UPDATE_BEFORE half of a changelog update)
        }
        buffer.add(new Op(converted, converter.isDelete(element)));
        if (buffer.size() >= options.batchSize()) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncFlushException();
        flushBuffer();
    }

    /** Recover a transient timer failure, or surface a deterministic failure on the task thread. */
    private void checkAsyncFlushException() throws IOException {
        Exception e = asyncFlushException;
        if (e == null) {
            return;
        }
        if (!TuGraphConnection.isRetryableFailure(e)) {
            throw new IOException("A TuGraph flush triggered by the batch-interval timer failed", e);
        }
        // The failed timer left its records in the buffer. Replay those records before consuming
        // the caller's new element or acknowledging a checkpoint flush.
        flushBuffer();
    }

    /** Turn the buffer into native or ordered MERGE statements and write them. */
    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            clearAsyncFlushFailure();
            return;
        }
        long startNanos = System.nanoTime();

        List<CypherStatement> statements = buildNativeStatements();
        BitSet vertexStatements = new BitSet(buffer.size());
        int vertexUpserts = 0;
        int edgeUpserts = 0;
        int deletes = 0;
        boolean nativeBulk = statements != null;
        if (nativeBulk) {
            for (Op op : buffer) {
                if (op.element instanceof Vertex) {
                    vertexUpserts++;
                } else if (op.element instanceof Edge) {
                    edgeUpserts++;
                } else {
                    throw new IOException("Unsupported graph element type: " + op.element.getClass().getName());
                }
            }
        } else {
            statements = new ArrayList<>(buffer.size());
            for (Op op : buffer) {
                GraphElement e = op.element;
                if (e instanceof Vertex) {
                    Vertex v = (Vertex) e;
                    int from = statements.size();
                    if (op.delete) {
                        statements.addAll(cypherBuilder.buildVertexDelete(v.label(), v.primaryKey(), List.of(v)));
                        deletes++;
                    } else {
                        statements.addAll(cypherBuilder.buildVertexUpsert(v.label(), v.primaryKey(), List.of(v)));
                        vertexUpserts++;
                    }
                    vertexStatements.set(from, statements.size());
                } else if (e instanceof Edge) {
                    Edge ed = (Edge) e;
                    if (op.delete) {
                        statements.addAll(cypherBuilder.buildEdgeDelete(ed.label(), ed.srcLabel(), ed.srcKey(),
                                ed.dstLabel(), ed.dstKey(), List.of(ed)));
                        deletes++;
                    } else {
                        statements.addAll(cypherBuilder.buildEdgeUpsert(ed.label(), ed.srcLabel(), ed.srcKey(),
                                ed.dstLabel(), ed.dstKey(), List.of(ed)));
                        edgeUpserts++;
                    }
                } else {
                    throw new IOException("Unsupported graph element type: " + e.getClass().getName());
                }
            }
        }

        // With vertex.on-missing-label=skip, vertex statements (upserts and deletes) hitting a
        // missing-label schema error are skipped record-level instead of failing the flush. Edge
        // statements are never label-skipped; their endpoint handling stays with on-missing-endpoint.
        boolean[] skippableOnMissingLabel = null;
        if (options.onMissingLabel() == OnMissingLabel.SKIP && !vertexStatements.isEmpty()) {
            skippableOnMissingLabel = new boolean[statements.size()];
            for (int i = vertexStatements.nextSetBit(0); i >= 0; i = vertexStatements.nextSetBit(i + 1)) {
                skippableOnMissingLabel[i] = true;
            }
        }

        // Safe to parallelize only when the flush is a single upsert kind with no ordering / endpoint
        // dependencies: vertices-only or edges-only, and no deletes.
        // Parallelize only pure vertex-upsert flushes - avoids edge / endpoint write races
        // (e.g. on-missing-endpoint=create MERGEing the same endpoint from two threads).
        boolean parallel = !nativeBulk && deletes == 0 && edgeUpserts == 0 && statements.size() > 1;
        BatchWriteResult result = parallel
                ? connection.writeBatchConcurrently(
                        statements, skippableOnMissingLabel, writeExecutor)
                : connection.writeBatch(statements, skippableOnMissingLabel);
        long writtenEdges = result.written();

        long labelSkipped = result.skippedMissingLabel();
        if (labelSkipped > 0) {
            vertexSkippedCounter.inc(labelSkipped);
            LOG.warn("Skipped {} vertex op(s) whose label is missing from the graph schema"
                    + " (vertex.on-missing-label=skip)", labelSkipped);
        }

        long endpointSkipped = 0;
        if (edgeUpserts > 0 && writtenEdges != TuGraphConnection.NO_WRITTEN_COUNT) {
            endpointSkipped = edgeUpserts - writtenEdges;
            if (endpointSkipped > 0) {
                if (options.onMissingEndpoint() == OnMissingEndpoint.FAIL) {
                    throw new IOException(endpointSkipped + " edge(s) could not be written because an endpoint"
                            + " vertex was missing (edge.on-missing-endpoint=fail)");
                }
                edgeSkippedCounter.inc(endpointSkipped);
                LOG.warn("Skipped {} edge(s) due to missing endpoint vertices", endpointSkipped);
            }
        }

        int total = buffer.size();
        buffer.clear();
        clearAsyncFlushFailure();

        numRecordsSend.inc(total - endpointSkipped - labelSkipped);
        if (deletes > 0) {
            deletedCounter.inc(deletes);
        }
        flushCounter.inc();
        lastFlushLatencyMs = (System.nanoTime() - startNanos) / 1_000_000L;
        LOG.debug("Flushed {} ops ({} deletes, {} edges skipped, {} vertices skipped, parallel={})"
                + " to TuGraph in {} ms",
                total, deletes, endpointSkipped, labelSkipped, parallel, lastFlushLatencyMs);
    }

    /**
     * Native procedure fast path. It is deliberately conservative: any delete, repeated target,
     * custom edge identity, or record-level missing-label policy keeps the established MERGE path.
     */
    private List<CypherStatement> buildNativeStatements() {
        if (options.onMissingLabel() != OnMissingLabel.FAIL
                || (options.edgeMergeKeys() != null && !options.edgeMergeKeys().isEmpty())) {
            return null;
        }
        boolean vertices = true;
        boolean edges = true;
        Set<String> targets = new HashSet<>();
        for (Op op : buffer) {
            if (op.delete || !targets.add(targetKey(op.element))) {
                return null;
            }
            vertices &= op.element instanceof Vertex;
            edges &= op.element instanceof Edge;
        }
        if (!vertices && !edges) {
            return null;
        }
        if (edges && options.onMissingEndpoint() != OnMissingEndpoint.SKIP) {
            return null;
        }

        Map<String, List<Vertex>> vertexGroups = new LinkedHashMap<>();
        Map<String, List<Edge>> edgeGroups = new LinkedHashMap<>();
        for (Op op : buffer) {
            if (vertices) {
                Vertex v = (Vertex) op.element;
                String key = v.label() + '\u0000' + v.primaryKey();
                vertexGroups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(v);
            } else {
                Edge e = (Edge) op.element;
                String key = e.label() + '\u0000' + e.srcLabel() + '\u0000' + e.srcKey()
                        + '\u0000' + e.dstLabel() + '\u0000' + e.dstKey();
                edgeGroups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(e);
            }
        }
        List<CypherStatement> statements = new ArrayList<>(vertices ? vertexGroups.size() : edgeGroups.size());
        if (vertices) {
            for (List<Vertex> group : vertexGroups.values()) {
                Vertex first = group.get(0);
                statements.add(NativeBulkStatementBuilder.vertex(first.label(), first.primaryKey(), group));
            }
        } else {
            for (List<Edge> group : edgeGroups.values()) {
                Edge first = group.get(0);
                statements.add(NativeBulkStatementBuilder.edge(first.label(), first.srcLabel(), first.srcKey(),
                        first.dstLabel(), first.dstKey(), group));
            }
        }
        return statements;
    }

    private static String targetKey(GraphElement element) {
        if (element instanceof Vertex) {
            Vertex v = (Vertex) element;
            return "V\u0000" + v.label() + '\u0000' + v.primaryKey() + '\u0000' + v.primaryKeyValue();
        }
        if (element instanceof Edge) {
            Edge e = (Edge) element;
            return "E\u0000" + e.label() + '\u0000' + e.srcLabel() + '\u0000' + e.srcKey()
                    + '\u0000' + e.srcValue() + '\u0000' + e.dstLabel() + '\u0000' + e.dstKey()
                    + '\u0000' + e.dstValue();
        }
        return element.getClass().getName();
    }

    /** Register the next processing-time flush timer if time-based flushing is enabled. */
    private void scheduleNextTimer() {
        if (options.batchIntervalMs() <= 0 || closed) {
            return;
        }
        long triggerAt = timeService.getCurrentProcessingTime() + options.batchIntervalMs();
        timeService.registerTimer(triggerAt, timestamp -> {
            if (closed) {
                return;
            }
            // Never leak an exception from the timer callback. A transient failure remains
            // recoverable by this timer or the next task-thread write/flush; a deterministic error
            // is surfaced by the next task-thread write/flush.
            try {
                Exception pending = asyncFlushException;
                if (pending == null || TuGraphConnection.isRetryableFailure(pending)) {
                    flushBuffer();
                }
            } catch (Exception e) {
                recordAsyncFlushFailure(e);
            }
            scheduleNextTimer();
        });
    }

    private void recordAsyncFlushFailure(Exception failure) {
        if (asyncFlushException == null) {
            asyncFlushFailureStartedNanos = System.nanoTime();
        }
        asyncFlushException = failure;
        asyncFlushFailuresCounter.inc();
        if (TuGraphConnection.isRetryableFailure(failure)) {
            LOG.error("Timer-triggered TuGraph flush exhausted its retry policy; retaining {}"
                    + " buffered op(s) for recovery", buffer.size(), failure);
        } else {
            LOG.error("Timer-triggered TuGraph flush failed with a non-retryable error; failing"
                    + " the task on the next write/flush", failure);
        }
    }

    private void clearAsyncFlushFailure() {
        if (asyncFlushException == null) {
            return;
        }
        long failedForMs = Math.max(0L,
                (System.nanoTime() - asyncFlushFailureStartedNanos) / 1_000_000L);
        asyncFlushException = null;
        asyncFlushFailureStartedNanos = 0L;
        LOG.info("TuGraph timer flush recovered after {} ms", failedForMs);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        try {
            checkAsyncFlushException();
            flushBuffer();
        } finally {
            writeExecutor.shutdown();
            connection.close();
        }
    }
}
