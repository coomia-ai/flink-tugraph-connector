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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Buffers graph change operations and flushes them to TuGraph as idempotent {@code MERGE} (upsert)
 * or {@code DELETE} statements.
 *
 * <p><b>Changelog.</b> Each buffered record carries whether it is an upsert or a delete (derived
 * from the Flink {@code RowKind} on the Table path, or always upsert for the DataStream identity
 * path). The buffer is flushed <em>in arrival order</em> so an insert-then-delete (or vice versa)
 * of the same key reaches TuGraph in the correct order. TuGraph requires one statement per element
 * (it rejects {@code UNWIND}-batched writes), which makes in-order processing the natural choice.
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
 * <p><b>Failure surfacing.</b> A flush triggered by the batch-interval timer never lets its
 * exception escape the timer callback (which would fail the task as an uncaught
 * {@code AsynchronousException}); the failure is recorded and rethrown from the next {@link #write}
 * or {@link #flush} call on the task thread, so it goes through Flink's regular failure handling.
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
    private volatile long lastFlushLatencyMs;

    /** Failure from a timer-triggered flush, rethrown on the task thread by write/flush. */
    private volatile Exception asyncFlushException;

    private boolean closed;

    public TuGraphSinkWriter(TuGraphSinkOptions options,
                             ElementConverter<InputT> converter,
                             CypherStatementBuilder cypherBuilder,
                             SinkWriterMetricGroup metricGroup,
                             ProcessingTimeService timeService) {
        this.options = options;
        this.converter = converter;
        this.cypherBuilder = cypherBuilder;
        this.timeService = timeService;
        this.buffer = new ArrayList<>(options.batchSize());

        this.connection = new TuGraphConnection(options);
        this.connection.open();

        int threads = Math.min(Math.max(1, options.maxConnectionPoolSize()), MAX_WRITE_THREADS);
        this.writeExecutor = Executors.newFixedThreadPool(threads);

        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        MetricGroup tg = metricGroup.addGroup("tugraph");
        this.flushCounter = tg.counter("flushCount");
        this.edgeSkippedCounter = tg.counter("edgeSkipped");
        this.vertexSkippedCounter = tg.counter("vertexSkipped");
        this.deletedCounter = tg.counter("deleted");
        tg.gauge("flushLatencyMs", () -> lastFlushLatencyMs);

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

    /** Rethrow a recorded timer-flush failure on the task thread (Flink's regular failure path). */
    private void checkAsyncFlushException() throws IOException {
        Exception e = asyncFlushException;
        if (e != null) {
            throw new IOException("A TuGraph flush triggered by the batch-interval timer failed", e);
        }
    }

    /** Turn the buffer into ordered upsert/delete statements and write them. */
    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        long startNanos = System.nanoTime();

        List<CypherStatement> statements = new ArrayList<>(buffer.size());
        BitSet vertexStatements = new BitSet(buffer.size());
        int vertexUpserts = 0;
        int edgeUpserts = 0;
        int deletes = 0;
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
        boolean parallel = deletes == 0 && edgeUpserts == 0 && statements.size() > 1;
        BatchWriteResult result = parallel
                ? writeConcurrently(statements, skippableOnMissingLabel)
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
     * Write order-independent statements concurrently over the connection pool. Each statement is an
     * independent auto-commit query; the connection is thread-safe across sessions.
     *
     * @return the aggregated written / label-skipped counts across all statements
     */
    private BatchWriteResult writeConcurrently(List<CypherStatement> statements,
                                               boolean[] skippableOnMissingLabel) throws IOException {
        List<Future<BatchWriteResult>> futures = new ArrayList<>(statements.size());
        for (int i = 0; i < statements.size(); i++) {
            List<CypherStatement> single = List.of(statements.get(i));
            boolean[] singleSkippable = skippableOnMissingLabel == null
                    ? null
                    : new boolean[] {skippableOnMissingLabel[i]};
            futures.add(writeExecutor.submit(() -> connection.writeBatch(single, singleSkippable)));
        }
        long written = TuGraphConnection.NO_WRITTEN_COUNT;
        int skipped = 0;
        try {
            for (Future<BatchWriteResult> future : futures) {
                BatchWriteResult r = future.get();
                long w = r.written();
                if (w != TuGraphConnection.NO_WRITTEN_COUNT) {
                    written = (written == TuGraphConnection.NO_WRITTEN_COUNT ? 0L : written) + w;
                }
                skipped += r.skippedMissingLabel();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing a TuGraph batch concurrently", ie);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause; // propagate driver exception to trigger Flink restart
            }
            throw new IOException("Concurrent TuGraph write failed", cause);
        }
        return new BatchWriteResult(written, skipped);
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
            // Never let the flush fail the timer callback (an uncaught AsynchronousException that
            // bypasses regular error handling); record it and rethrow from write()/flush() on the
            // task thread. The buffer is only cleared on success, so no data is lost.
            try {
                if (asyncFlushException == null) {
                    flushBuffer();
                }
            } catch (Exception e) {
                asyncFlushException = e;
                LOG.error("Timer-triggered TuGraph flush failed; failing the task on the next"
                        + " write/flush", e);
            }
            scheduleNextTimer();
        });
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        try {
            flushBuffer();
        } finally {
            writeExecutor.shutdown();
            connection.close();
        }
    }
}
