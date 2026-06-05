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
import com.coomia.flink.tugraph.cypher.CypherStatementBuilder;
import com.coomia.flink.tugraph.cypher.MergeCypherStatementBuilder;
import com.coomia.flink.tugraph.element.GraphElement;

/**
 * Fluent builder for the DataStream {@link TuGraphSink}. Mirrors {@link TuGraphSinkOptions.Builder}
 * and additionally allows overriding the {@link CypherStatementBuilder} (e.g. for a TuGraph dialect
 * that does not support {@code SET x += $map}).
 *
 * <pre>{@code
 * stream.sinkTo(TuGraphSink.<Vertex>builder()
 *         .uri("bolt://host:7687")
 *         .auth("admin", "password")
 *         .graph("default")
 *         .batchSize(500)
 *         .build());
 * }</pre>
 *
 * @param <T> graph element type written by the stream ({@code Vertex} or {@code Edge})
 */
public class TuGraphSinkBuilder<T extends GraphElement> {

    private final TuGraphSinkOptions.Builder options = TuGraphSinkOptions.builder();
    private CypherStatementBuilder cypherBuilder; // null -> derived from options at build()

    TuGraphSinkBuilder() {
    }

    public TuGraphSinkBuilder<T> uri(String uri) {
        options.uri(uri);
        return this;
    }

    public TuGraphSinkBuilder<T> username(String username) {
        options.username(username);
        return this;
    }

    public TuGraphSinkBuilder<T> password(String password) {
        options.password(password);
        return this;
    }

    public TuGraphSinkBuilder<T> auth(String username, String password) {
        options.auth(username, password);
        return this;
    }

    public TuGraphSinkBuilder<T> graph(String graph) {
        options.graph(graph);
        return this;
    }

    public TuGraphSinkBuilder<T> batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    public TuGraphSinkBuilder<T> batchIntervalMs(long batchIntervalMs) {
        options.batchIntervalMs(batchIntervalMs);
        return this;
    }

    public TuGraphSinkBuilder<T> maxRetries(int maxRetries) {
        options.maxRetries(maxRetries);
        return this;
    }

    public TuGraphSinkBuilder<T> connectionTimeoutMs(long connectionTimeoutMs) {
        options.connectionTimeoutMs(connectionTimeoutMs);
        return this;
    }

    public TuGraphSinkBuilder<T> maxConnectionPoolSize(int maxConnectionPoolSize) {
        options.maxConnectionPoolSize(maxConnectionPoolSize);
        return this;
    }

    public TuGraphSinkBuilder<T> onMissingEndpoint(OnMissingEndpoint onMissingEndpoint) {
        options.onMissingEndpoint(onMissingEndpoint);
        return this;
    }

    /** Edge property columns folded into the edge MERGE match key (e.g. {@code rel_type}). */
    public TuGraphSinkBuilder<T> edgeMergeKeys(java.util.List<String> edgeMergeKeys) {
        options.edgeMergeKeys(edgeMergeKeys);
        return this;
    }

    /** Override the Cypher generation strategy (defaults to {@link MergeCypherStatementBuilder}). */
    public TuGraphSinkBuilder<T> cypherStatementBuilder(CypherStatementBuilder cypherBuilder) {
        this.cypherBuilder = cypherBuilder;
        return this;
    }

    public TuGraphSink<T> build() {
        TuGraphSinkOptions opts = options.build();
        CypherStatementBuilder builder = cypherBuilder != null
                ? cypherBuilder
                : new MergeCypherStatementBuilder(opts.edgeMergeKeys(),
                        opts.onMissingEndpoint() == TuGraphSinkOptions.OnMissingEndpoint.CREATE);
        return new TuGraphSink<>(opts, ElementConverter.identity(), builder);
    }
}
