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
import com.coomia.flink.tugraph.cypher.CypherStatementBuilder;
import com.coomia.flink.tugraph.cypher.MergeCypherStatementBuilder;
import com.coomia.flink.tugraph.element.GraphElement;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.Objects;

/**
 * Flink {@link Sink} (Sink V2) that writes graph elements to TuGraph-DB over Bolt.
 *
 * <p>The same writer backs both entry points: the DataStream API (via {@link #builder()} with an
 * identity {@link ElementConverter}) and the Table/SQL API (with a {@code RowDataToElementConverter}).
 *
 * <p>Delivery is at-least-once; idempotent {@code MERGE} writes make replays after a restart
 * effectively exactly-once at the business level (no duplicate vertices or edges).
 *
 * @param <InputT> upstream record type
 */
public class TuGraphSink<InputT> implements Sink<InputT> {

    private static final long serialVersionUID = 1L;

    private final TuGraphSinkOptions options;
    private final ElementConverter<InputT> converter;
    private final CypherStatementBuilder cypherBuilder;

    public TuGraphSink(TuGraphSinkOptions options,
                       ElementConverter<InputT> converter,
                       CypherStatementBuilder cypherBuilder) {
        this.options = Objects.requireNonNull(options, "options");
        this.converter = Objects.requireNonNull(converter, "converter");
        this.cypherBuilder = Objects.requireNonNull(cypherBuilder, "cypherBuilder");
    }

    /** Convenience constructor using the default {@link MergeCypherStatementBuilder}. */
    public TuGraphSink(TuGraphSinkOptions options, ElementConverter<InputT> converter) {
        this(options, converter, new MergeCypherStatementBuilder());
    }

    /** Modern entry point invoked by Flink 1.20 at runtime. */
    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return new TuGraphSinkWriter<>(
                options, converter, cypherBuilder,
                context.metricGroup(), context.getProcessingTimeService());
    }

    /**
     * Legacy entry point kept for binary compatibility with Flink &lt; 1.20. Flink 1.20 calls the
     * {@link WriterInitContext} overload above; this one is retained because the interface still
     * declares it abstract.
     */
    @Deprecated
    @Override
    @SuppressWarnings("deprecation")
    public SinkWriter<InputT> createWriter(Sink.InitContext context) throws IOException {
        return new TuGraphSinkWriter<>(
                options, converter, cypherBuilder,
                context.metricGroup(), context.getProcessingTimeService());
    }

    /** @return a DataStream builder for streams of {@link GraphElement} (e.g. {@code Vertex}/{@code Edge}). */
    public static <T extends GraphElement> TuGraphSinkBuilder<T> builder() {
        return new TuGraphSinkBuilder<>();
    }
}
