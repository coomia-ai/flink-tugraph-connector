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

package com.coomia.flink.tugraph.table;

import com.coomia.flink.tugraph.TuGraphSinkOptions.OnMissingEndpoint;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * {@link ConfigOption}s for the {@code 'connector' = 'tugraph'} table connector.
 *
 * <p>Keys mirror the connector requirements document (§8). See
 * {@link TuGraphDynamicTableSinkFactory} for how they are validated and consumed.
 */
public final class TuGraphConnectorOptions {

    private TuGraphConnectorOptions() {
    }

    /** Element kind written by the table. */
    public enum ElementType {
        VERTEX,
        EDGE
    }

    // ---- Connection ----
    public static final ConfigOption<String> URI = ConfigOptions.key("uri")
            .stringType().noDefaultValue()
            .withDescription("Bolt URI of the TuGraph server, e.g. bolt://host:7687.");

    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType().noDefaultValue()
            .withDescription("Bolt basic-auth username.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType().noDefaultValue()
            .withDescription("Bolt basic-auth password.");

    public static final ConfigOption<String> GRAPH = ConfigOptions.key("graph")
            .stringType().defaultValue("default")
            .withDescription("Target sub-graph / database name.");

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT = ConfigOptions.key("connection.timeout.ms")
            .durationType().defaultValue(Duration.ofMillis(15_000))
            .withDescription("Bolt connection timeout.");

    public static final ConfigOption<Integer> MAX_CONNECTION_POOL_SIZE =
            ConfigOptions.key("max.connection.pool.size")
                    .intType().defaultValue(10)
                    .withDescription("Maximum size of the Bolt connection pool per sink subtask.");

    // ---- Element selection ----
    public static final ConfigOption<ElementType> ELEMENT_TYPE = ConfigOptions.key("element.type")
            .enumType(ElementType.class).noDefaultValue()
            .withDescription("Whether the table maps to graph 'vertex' or 'edge' elements.");

    // ---- Vertex ----
    public static final ConfigOption<String> VERTEX_LABEL = ConfigOptions.key("vertex.label")
            .stringType().noDefaultValue()
            .withDescription("Vertex label (required when element.type = vertex).");

    public static final ConfigOption<String> VERTEX_PRIMARY_KEY = ConfigOptions.key("vertex.primary-key")
            .stringType().noDefaultValue()
            .withDescription("Primary-key column; defaults to the table's PRIMARY KEY constraint.");

    // ---- Edge ----
    public static final ConfigOption<String> EDGE_LABEL = ConfigOptions.key("edge.label")
            .stringType().noDefaultValue()
            .withDescription("Edge label (required when element.type = edge).");

    public static final ConfigOption<String> EDGE_SRC_LABEL = ConfigOptions.key("edge.src.label")
            .stringType().noDefaultValue()
            .withDescription("Source vertex label.");

    public static final ConfigOption<String> EDGE_SRC_COL = ConfigOptions.key("edge.src.col")
            .stringType().noDefaultValue()
            .withDescription("Table column carrying the source vertex key value.");

    public static final ConfigOption<String> EDGE_SRC_KEY = ConfigOptions.key("edge.src.key")
            .stringType().noDefaultValue()
            .withDescription("Source vertex primary-key property to match; defaults to edge.src.col.");

    public static final ConfigOption<String> EDGE_DST_LABEL = ConfigOptions.key("edge.dst.label")
            .stringType().noDefaultValue()
            .withDescription("Destination vertex label.");

    public static final ConfigOption<String> EDGE_DST_COL = ConfigOptions.key("edge.dst.col")
            .stringType().noDefaultValue()
            .withDescription("Table column carrying the destination vertex key value.");

    public static final ConfigOption<String> EDGE_DST_KEY = ConfigOptions.key("edge.dst.key")
            .stringType().noDefaultValue()
            .withDescription("Destination vertex primary-key property to match; defaults to edge.dst.col.");

    public static final ConfigOption<OnMissingEndpoint> EDGE_ON_MISSING_ENDPOINT =
            ConfigOptions.key("edge.on-missing-endpoint")
                    .enumType(OnMissingEndpoint.class).defaultValue(OnMissingEndpoint.SKIP)
                    .withDescription("Behaviour when an edge endpoint vertex is missing: SKIP, FAIL, "
                            + "or CREATE (MERGE a bare endpoint vertex).");

    public static final ConfigOption<java.util.List<String>> EDGE_MERGE_KEYS =
            ConfigOptions.key("edge.merge.keys")
                    .stringType().asList().noDefaultValue()
                    .withDescription("Edge property columns folded into the edge MERGE match key "
                            + "(e.g. rel_type), so distinct relation types between the same vertex "
                            + "pair are kept as separate edges instead of collapsing.");

    // ---- Batching / retry ----
    public static final ConfigOption<Integer> SINK_BATCH_SIZE = ConfigOptions.key("sink.batch.size")
            .intType().defaultValue(500)
            .withDescription("Flush when this many buffered elements accumulate.");

    public static final ConfigOption<Duration> SINK_BATCH_INTERVAL = ConfigOptions.key("sink.batch.interval.ms")
            .durationType().defaultValue(Duration.ofMillis(1_000))
            .withDescription("Maximum time before a non-empty buffer is flushed; 0 disables time-based flush.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions.key("sink.max.retries")
            .intType().defaultValue(3)
            .withDescription("Number of retries for transient write failures.");

    // ---- Source / Lookup (v0.2) ----
    public static final ConfigOption<Integer> SCAN_FETCH_SIZE = ConfigOptions.key("scan.fetch-size")
            .intType().defaultValue(1000)
            .withDescription("Page size for the bounded vertex scan (SKIP/LIMIT paging).");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions.key("lookup.cache.max-rows")
            .longType().defaultValue(0L)
            .withDescription("Maximum rows held in the lookup cache; 0 disables caching.");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions.key("lookup.cache.ttl")
            .durationType().noDefaultValue()
            .withDescription("Lookup cache entry time-to-live (expire after write).");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions.key("lookup.max-retries")
            .intType().defaultValue(3)
            .withDescription("Number of retries for a failed lookup query.");
}
