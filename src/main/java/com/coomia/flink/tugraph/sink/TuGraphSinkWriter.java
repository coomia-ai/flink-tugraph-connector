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
import com.coomia.flink.tugraph.client.TuGraphConnection;
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
import java.util.List;

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
 * <p><b>Threading.</b> {@link #write}, {@link #flush} and the processing-time callback all run on
 * the single task thread (Flink's mailbox), so the buffer needs no synchronization.
 *
 * <p><b>Flush triggers.</b> (1) the buffer reaching {@link TuGraphSinkOptions#batchSize()};
 * (2) a processing-time timer every {@link TuGraphSinkOptions#batchIntervalMs()} (when &gt; 0);
 * (3) Flink calling {@link #flush(boolean)} on every checkpoint and at end of input — giving
 * at-least-once delivery.
 *
 * @param <InputT> upstream record type
 */
public class TuGraphSinkWriter<InputT> implements SinkWriter<InputT> {

    private static final Logger LOG = LoggerFactory.getLogger(TuGraphSinkWriter.class);

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

    private final List<Op> buffer;

    // ---- Metrics ----
    private final Counter numRecordsSend;
    private final Counter flushCounter;
    private final Counter edgeSkippedCounter;
    private final Counter deletedCounter;
    private volatile long lastFlushLatencyMs;

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

        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        MetricGroup tg = metricGroup.addGroup("tugraph");
        this.flushCounter = tg.counter("flushCount");
        this.edgeSkippedCounter = tg.counter("edgeSkipped");
        this.deletedCounter = tg.counter("deleted");
        tg.gauge("flushLatencyMs", () -> lastFlushLatencyMs);

        scheduleNextTimer();
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
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
        flushBuffer();
    }

    /** Turn the buffer into ordered upsert/delete statements and write them in one Bolt session. */
    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        long startNanos = System.nanoTime();

        List<CypherStatement> statements = new ArrayList<>(buffer.size());
        int edgeUpserts = 0;
        int deletes = 0;
        for (Op op : buffer) {
            GraphElement e = op.element;
            if (e instanceof Vertex) {
                Vertex v = (Vertex) e;
                if (op.delete) {
                    statements.addAll(cypherBuilder.buildVertexDelete(v.label(), v.primaryKey(), List.of(v)));
                    deletes++;
                } else {
                    statements.addAll(cypherBuilder.buildVertexUpsert(v.label(), v.primaryKey(), List.of(v)));
                }
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

        long writtenEdges = connection.writeBatch(statements);

        long skipped = 0;
        if (edgeUpserts > 0 && writtenEdges != TuGraphConnection.NO_WRITTEN_COUNT) {
            skipped = edgeUpserts - writtenEdges;
            if (skipped > 0) {
                if (options.onMissingEndpoint() == OnMissingEndpoint.FAIL) {
                    throw new IOException(skipped + " edge(s) could not be written because an endpoint"
                            + " vertex was missing (edge.on-missing-endpoint=fail)");
                }
                edgeSkippedCounter.inc(skipped);
                LOG.warn("Skipped {} edge(s) due to missing endpoint vertices", skipped);
            }
        }

        int total = buffer.size();
        buffer.clear();

        numRecordsSend.inc(total - skipped);
        if (deletes > 0) {
            deletedCounter.inc(deletes);
        }
        flushCounter.inc();
        lastFlushLatencyMs = (System.nanoTime() - startNanos) / 1_000_000L;
        LOG.debug("Flushed {} ops ({} deletes, {} edges skipped) to TuGraph in {} ms",
                total, deletes, skipped, lastFlushLatencyMs);
    }

    /** Register the next processing-time flush timer if time-based flushing is enabled. */
    private void scheduleNextTimer() {
        if (options.batchIntervalMs() <= 0 || closed) {
            return;
        }
        long triggerAt = timeService.getCurrentProcessingTime() + options.batchIntervalMs();
        timeService.registerTimer(triggerAt, timestamp -> {
            if (!closed) {
                flushBuffer();
                scheduleNextTimer();
            }
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
            connection.close();
        }
    }
}
