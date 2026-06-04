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

package com.coomia.flink.tugraph;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Immutable, serializable configuration for the TuGraph sink.
 *
 * <p>The sink is serialized and shipped to every task manager, so this object and everything it
 * references must be {@link Serializable}. Build instances through {@link #builder()}; the
 * {@link Builder#build()} call validates required fields and value ranges.
 *
 * <p>Defaults follow the connector requirements document (§8).
 */
public final class TuGraphSinkOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Strategy applied when an edge endpoint vertex is missing at write time. */
    public enum OnMissingEndpoint {
        /** Silently skip the edge and record a metric (default). */
        SKIP,
        /** Fail the write, triggering a Flink restart. */
        FAIL
    }

    // ---- Connection ----
    private final String uri;
    private final String username;
    private final String password;
    private final String graph;
    private final long connectionTimeoutMs;
    private final int maxConnectionPoolSize;

    // ---- Batching / retry ----
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;

    // ---- Edge behaviour ----
    private final OnMissingEndpoint onMissingEndpoint;

    private TuGraphSinkOptions(Builder b) {
        this.uri = b.uri;
        this.username = b.username;
        this.password = b.password;
        this.graph = b.graph;
        this.connectionTimeoutMs = b.connectionTimeoutMs;
        this.maxConnectionPoolSize = b.maxConnectionPoolSize;
        this.batchSize = b.batchSize;
        this.batchIntervalMs = b.batchIntervalMs;
        this.maxRetries = b.maxRetries;
        this.onMissingEndpoint = b.onMissingEndpoint;
    }

    public String uri() {
        return uri;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public String graph() {
        return graph;
    }

    public long connectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public int maxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    public int batchSize() {
        return batchSize;
    }

    public long batchIntervalMs() {
        return batchIntervalMs;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public OnMissingEndpoint onMissingEndpoint() {
        return onMissingEndpoint;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        // Password intentionally masked.
        return "TuGraphSinkOptions{uri=" + uri
                + ", username=" + username
                + ", graph=" + graph
                + ", batchSize=" + batchSize
                + ", batchIntervalMs=" + batchIntervalMs
                + ", maxRetries=" + maxRetries
                + ", connectionTimeoutMs=" + connectionTimeoutMs
                + ", maxConnectionPoolSize=" + maxConnectionPoolSize
                + ", onMissingEndpoint=" + onMissingEndpoint
                + '}';
    }

    /** Fluent builder. All setters return {@code this}; {@link #build()} validates. */
    public static final class Builder {
        private String uri;
        private String username;
        private String password;
        private String graph = "default";
        private long connectionTimeoutMs = 15_000L;
        private int maxConnectionPoolSize = 10;
        private int batchSize = 500;
        private long batchIntervalMs = 1_000L;
        private int maxRetries = 3;
        private OnMissingEndpoint onMissingEndpoint = OnMissingEndpoint.SKIP;

        private Builder() {
        }

        /** Bolt URI, e.g. {@code bolt://host:7687}. Required. */
        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }

        /** Bolt basic-auth username. Required. */
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        /** Bolt basic-auth password. Required. */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /** Convenience for {@link #username(String)} + {@link #password(String)}. */
        public Builder auth(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        /** Target sub-graph / database. Defaults to {@code default}. */
        public Builder graph(String graph) {
            this.graph = graph;
            return this;
        }

        public Builder connectionTimeoutMs(long connectionTimeoutMs) {
            this.connectionTimeoutMs = connectionTimeoutMs;
            return this;
        }

        public Builder connectionTimeout(Duration timeout) {
            this.connectionTimeoutMs = timeout.toMillis();
            return this;
        }

        public Builder maxConnectionPoolSize(int maxConnectionPoolSize) {
            this.maxConnectionPoolSize = maxConnectionPoolSize;
            return this;
        }

        /** Flush when this many buffered elements accumulate. Defaults to 500. */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /** Flush at least this often, in milliseconds. {@code <= 0} disables time-based flush. */
        public Builder batchIntervalMs(long batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        /** Number of retries for transient write failures. Defaults to 3. */
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder onMissingEndpoint(OnMissingEndpoint onMissingEndpoint) {
            this.onMissingEndpoint = onMissingEndpoint;
            return this;
        }

        public TuGraphSinkOptions build() {
            requireNonBlank(uri, "uri");
            requireNonBlank(username, "username");
            // Password may legitimately be empty on some deployments, but null is a misconfiguration.
            Objects.requireNonNull(password, "password must not be null");
            requireNonBlank(graph, "graph");
            checkArgument(batchSize > 0, "batchSize must be > 0");
            checkArgument(maxRetries >= 0, "maxRetries must be >= 0");
            checkArgument(connectionTimeoutMs > 0, "connectionTimeoutMs must be > 0");
            checkArgument(maxConnectionPoolSize > 0, "maxConnectionPoolSize must be > 0");
            return new TuGraphSinkOptions(this);
        }

        private static void requireNonBlank(String value, String name) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(name + " must not be null or blank");
            }
        }

        private static void checkArgument(boolean condition, String message) {
            if (!condition) {
                throw new IllegalArgumentException(message);
            }
        }
    }
}
