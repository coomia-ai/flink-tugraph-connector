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

package com.coomia.flink.tugraph.client;

import com.coomia.flink.tugraph.TuGraphSinkOptions;
import com.coomia.flink.tugraph.cypher.CypherStatement;
import com.coomia.flink.tugraph.cypher.CypherStatementBuilder;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Thin wrapper around the Neo4j Java driver speaking the Bolt protocol to TuGraph-DB.
 *
 * <p>Owns a single {@link Driver} (and its internal connection pool). The driver is expensive, so
 * create exactly one per sink subtask in {@code SinkWriter} construction and {@link #close()} it on
 * teardown — never per batch.
 *
 * <p>Each {@link #writeBatch(CypherStatement)} runs in its own explicit (unmanaged) transaction so
 * this class fully controls retry behaviour: transient failures are retried with exponential
 * backoff up to {@link TuGraphSinkOptions#maxRetries()}, after which the exception propagates and
 * Flink restarts the job from the last checkpoint (idempotent {@code MERGE} absorbs the replay).
 *
 * <p>This type is reused by the future Source / Lookup paths and therefore exposes a generic
 * read helper as well.
 */
public class TuGraphConnection implements AutoCloseable, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(TuGraphConnection.class);

    /** Sentinel returned by {@link #writeBatch} when the statement carries no written-count field. */
    public static final long NO_WRITTEN_COUNT = -1L;

    private final TuGraphSinkOptions options;

    /** Lazily built, then reused; {@code transient} because driver is not serializable. */
    private transient volatile Driver driver;

    public TuGraphConnection(TuGraphSinkOptions options) {
        this.options = options;
    }

    /** Open the driver (idempotent). Safe to call once during writer initialization. */
    public synchronized void open() {
        if (driver == null) {
            Config config = Config.builder()
                    .withMaxConnectionPoolSize(options.maxConnectionPoolSize())
                    .withConnectionTimeout(options.connectionTimeoutMs(), TimeUnit.MILLISECONDS)
                    .withConnectionAcquisitionTimeout(
                            Math.max(options.connectionTimeoutMs(), 60_000L), TimeUnit.MILLISECONDS)
                    .build();
            driver = GraphDatabase.driver(
                    options.uri(), AuthTokens.basic(options.username(), options.password()), config);
            LOG.info("Opened TuGraph Bolt driver to {} (graph={})", options.uri(), options.graph());
        }
    }

    /** Fail fast if the server is unreachable or credentials are wrong. */
    public void verifyConnectivity() {
        ensureOpen();
        driver.verifyConnectivity();
    }

    /**
     * Execute a write batch in a single transaction, retrying transient errors with exponential
     * backoff.
     *
     * @param stmt the parameterized batch statement
     * @return for edge upserts, the number of edges actually written (see
     *         {@link CypherStatementBuilder#WRITTEN_COUNT_FIELD}); {@link #NO_WRITTEN_COUNT} when the
     *         statement returns no such field (e.g. vertex upserts)
     */
    public long writeBatch(CypherStatement stmt) {
        ensureOpen();
        int attempt = 0;
        while (true) {
            try (Session session = driver.session(SessionConfig.forDatabase(options.graph()));
                    Transaction tx = session.beginTransaction()) {
                Result result = tx.run(stmt.cypher(), stmt.parameters());
                long written = readWrittenCount(result);
                tx.commit();
                return written;
            } catch (TransientException | ServiceUnavailableException | SessionExpiredException ex) {
                if (attempt >= options.maxRetries()) {
                    LOG.error("TuGraph write failed after {} retries; propagating to trigger restart",
                            options.maxRetries(), ex);
                    throw ex;
                }
                long backoffMs = backoffMillis(attempt);
                LOG.warn("Transient TuGraph write failure (attempt {}/{}), retrying in {} ms: {}",
                        attempt + 1, options.maxRetries(), backoffMs, ex.getMessage());
                sleep(backoffMs);
                attempt++;
            }
        }
    }

    /** Read the single {@code written} count produced by an edge upsert, if present. */
    private static long readWrittenCount(Result result) {
        List<Record> records = result.list();
        if (records.isEmpty()) {
            return NO_WRITTEN_COUNT;
        }
        Record first = records.get(0);
        if (first.containsKey(CypherStatementBuilder.WRITTEN_COUNT_FIELD)) {
            return first.get(CypherStatementBuilder.WRITTEN_COUNT_FIELD).asLong(NO_WRITTEN_COUNT);
        }
        return NO_WRITTEN_COUNT;
    }

    /** Exponential backoff: 200ms, 400ms, 800ms, ... capped at 10s. */
    private static long backoffMillis(int attempt) {
        long base = 200L << Math.min(attempt, 6);
        return Math.min(base, 10_000L);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while backing off before a TuGraph write retry", ie);
        }
    }

    private void ensureOpen() {
        if (driver == null) {
            open();
        }
    }

    @Override
    public synchronized void close() {
        if (driver != null) {
            try {
                driver.close();
                LOG.info("Closed TuGraph Bolt driver to {}", options.uri());
            } finally {
                driver = null;
            }
        }
    }
}
