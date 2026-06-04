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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Buffers graph elements and flushes them to TuGraph in idempotent {@code MERGE} batches.
 *
 * <p><b>Threading.</b> {@link #write}, {@link #flush} and the processing-time callback all run on
 * the single task thread (Flink's mailbox), so the buffer needs no synchronization.
 *
 * <p><b>Flush triggers.</b> (1) the buffer reaching {@link TuGraphSinkOptions#batchSize()};
 * (2) a processing-time timer every {@link TuGraphSinkOptions#batchIntervalMs()} (when &gt; 0);
 * (3) Flink calling {@link #flush(boolean)} on every checkpoint and at end of input. Because a
 * checkpoint barrier triggers {@code flush}, every record buffered before the barrier is durably
 * written before the checkpoint completes — giving at-least-once delivery.
 *
 * @param <InputT> upstream record type
 */
public class TuGraphSinkWriter<InputT> implements SinkWriter<InputT> {

    private static final Logger LOG = LoggerFactory.getLogger(TuGraphSinkWriter.class);

    private static final char KEY_SEP = '\u0001';

    private final TuGraphSinkOptions options;
    private final ElementConverter<InputT> converter;
    private final CypherStatementBuilder cypherBuilder;
    private final TuGraphConnection connection;
    private final ProcessingTimeService timeService;

    private final List<GraphElement> buffer;

    // ---- Metrics ----
    private final Counter numRecordsSend;
    private final Counter flushCounter;
    private final Counter edgeSkippedCounter;
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
        tg.gauge("flushLatencyMs", () -> lastFlushLatencyMs);

        scheduleNextTimer();
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        GraphElement converted = converter.convert(element);
        if (converted == null) {
            return;
        }
        buffer.add(converted);
        if (buffer.size() >= options.batchSize()) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        flushBuffer();
    }

    /** Group the buffer by element kind and label, emit one batch statement per group. */
    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        long startNanos = System.nanoTime();

        // Preserve first-seen order so output is deterministic and easy to reason about.
        Map<String, List<Vertex>> vertexGroups = new LinkedHashMap<>();
        Map<String, List<Edge>> edgeGroups = new LinkedHashMap<>();
        for (GraphElement e : buffer) {
            if (e instanceof Vertex) {
                Vertex v = (Vertex) e;
                vertexGroups.computeIfAbsent(v.label() + KEY_SEP + v.primaryKey(),
                        k -> new ArrayList<>()).add(v);
            } else if (e instanceof Edge) {
                Edge edge = (Edge) e;
                String key = String.join(String.valueOf(KEY_SEP),
                        edge.label(), edge.srcLabel(), edge.srcKey(), edge.dstLabel(), edge.dstKey());
                edgeGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(edge);
            } else {
                throw new IOException("Unsupported graph element type: " + e.getClass().getName());
            }
        }

        long written = 0L;
        for (List<Vertex> group : vertexGroups.values()) {
            Vertex sample = group.get(0);
            List<CypherStatement> statements = cypherBuilder.buildVertexUpsert(
                    sample.label(), sample.primaryKey(), group);
            connection.writeBatch(statements);
            written += group.size();
        }
        for (List<Edge> group : edgeGroups.values()) {
            written += flushEdgeGroup(group);
        }

        int flushed = buffer.size();
        buffer.clear();

        numRecordsSend.inc(written);
        flushCounter.inc();
        lastFlushLatencyMs = (System.nanoTime() - startNanos) / 1_000_000L;
        LOG.debug("Flushed {} buffered elements ({} written) to TuGraph in {} ms",
                flushed, written, lastFlushLatencyMs);
    }

    /** Write one edge group and apply the missing-endpoint policy. @return edges actually written. */
    private long flushEdgeGroup(List<Edge> group) throws IOException {
        Edge sample = group.get(0);
        List<CypherStatement> statements = cypherBuilder.buildEdgeUpsert(
                sample.label(), sample.srcLabel(), sample.srcKey(),
                sample.dstLabel(), sample.dstKey(), group);
        long writtenCount = connection.writeBatch(statements);
        if (writtenCount == TuGraphConnection.NO_WRITTEN_COUNT) {
            // The statement did not report a count; assume all were written.
            return group.size();
        }
        long skipped = group.size() - writtenCount;
        if (skipped > 0) {
            if (options.onMissingEndpoint() == OnMissingEndpoint.FAIL) {
                throw new IOException(skipped + " edge(s) of label '" + sample.label()
                        + "' could not be written because an endpoint vertex was missing"
                        + " (edge.on-missing-endpoint=fail)");
            }
            edgeSkippedCounter.inc(skipped);
            LOG.warn("Skipped {} edge(s) of label '{}' due to missing endpoint vertices",
                    skipped, sample.label());
        }
        return writtenCount;
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
