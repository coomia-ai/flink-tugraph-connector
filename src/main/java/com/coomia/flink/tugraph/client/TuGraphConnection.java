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
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Thin wrapper around the Neo4j Java driver speaking the Bolt protocol to TuGraph-DB.
 *
 * <p>Owns a single {@link Driver} (and its internal connection pool). The driver is expensive, so
 * create exactly one per sink subtask in {@code SinkWriter} construction and {@link #close()} it on
 * teardown — never per batch.
 *
 * <p>A whole flush group is written by {@link #writeBatch(List)} in one explicit (unmanaged)
 * operation. This class fully controls retry behaviour: transient failures retry the whole group
 * with exponential backoff, either within {@link TuGraphSinkOptions#retryBudgetMs()} or (for
 * compatibility) up to {@link TuGraphSinkOptions#maxRetries()}. The last driver exception is
 * propagated after exhaustion so Flink can restart from the last checkpoint; idempotent
 * {@code MERGE} absorbs the replay.
 */
public class TuGraphConnection implements AutoCloseable, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(TuGraphConnection.class);

    /** Sentinel meaning no statement in the batch reported a written count (e.g. vertex upserts). */
    public static final long NO_WRITTEN_COUNT = -1L;

    /**
     * Outcome of {@link #writeBatch(List, boolean[])}: the summed edge written-count plus the number
     * of statements skipped because their vertex label is missing from the graph schema.
     */
    public static final class BatchWriteResult {
        private final long written;
        private final int skippedMissingLabel;

        public BatchWriteResult(long written, int skippedMissingLabel) {
            this.written = written;
            this.skippedMissingLabel = skippedMissingLabel;
        }

        /** @return summed edge written-count, or {@link #NO_WRITTEN_COUNT} if none was reported. */
        public long written() {
            return written;
        }

        /** @return statements skipped because their vertex label does not exist in the schema. */
        public int skippedMissingLabel() {
            return skippedMissingLabel;
        }
    }

    private final TuGraphSinkOptions options;

    /** Runtime-only listener used by the sink writer to expose retry attempts as a metric. */
    private transient Runnable retryListener;

    private transient RetryPolicy retryPolicy;

    /** Lazily built, then reused; {@code transient} because driver is not serializable. */
    private transient volatile Driver driver;

    public TuGraphConnection(TuGraphSinkOptions options) {
        this(options, null);
    }

    public TuGraphConnection(TuGraphSinkOptions options, Runnable retryListener) {
        this.options = options;
        this.retryListener = retryListener;
        this.retryPolicy = new RetryPolicy(options, retryListener);
    }

    /** Open the driver (idempotent). Safe to call once during writer initialization. */
    public synchronized void open() {
        if (driver == null) {
            if (options.retryBudgetMs() > 0) {
                retryPolicy().execute("connection open", () -> {
                    Driver candidate = newDriver();
                    try {
                        candidate.verifyConnectivity();
                        driver = candidate;
                        return null;
                    } catch (RuntimeException failure) {
                        try {
                            candidate.close();
                        } catch (RuntimeException closeFailure) {
                            failure.addSuppressed(closeFailure);
                        }
                        throw failure;
                    }
                });
            } else {
                // Preserve 0.2 behaviour when budget mode is disabled: driver creation is lazy and
                // the first query performs the actual network connection.
                driver = newDriver();
            }
            LOG.info("Opened TuGraph Bolt driver to {} (graph={})", options.uri(), options.graph());
        }
    }

    /** Fail fast if the server is unreachable or credentials are wrong. */
    public void verifyConnectivity() {
        ensureOpen();
        if (options.retryBudgetMs() > 0) {
            retryPolicy().execute("connectivity verification", () -> {
                driver.verifyConnectivity();
                return null;
            });
        } else {
            driver.verifyConnectivity();
        }
    }

    /** Convenience for a single statement; see {@link #writeBatch(List)}. */
    public long writeBatch(CypherStatement statement) {
        return writeBatch(Collections.singletonList(statement));
    }

    /**
     * Executes a group of statements sequentially using auto-commit sessions.
     *
     * <p>The retry policy applies to the complete batch. A retry therefore replays the batch from
     * its first statement; callers should use idempotent Cypher (for example, {@code MERGE}) when
     * transient-failure recovery is enabled.
     *
     * @param statements the parameterized statements, run in order
     * @return the total number of edges written across statements that report it (see
     *         {@link CypherStatementBuilder#WRITTEN_COUNT_FIELD}); {@link #NO_WRITTEN_COUNT} when no
     *         statement returns such a field (e.g. vertex upserts)
     */
    public long writeBatch(List<CypherStatement> statements) {
        return writeBatch(statements, null).written();
    }

    /**
     * Like {@link #writeBatch(List)}, but statements flagged in {@code skippableOnMissingLabel} that
     * fail because their vertex label does not exist in the graph schema are skipped (and counted)
     * instead of failing the whole batch — the record-level behaviour behind
     * {@code vertex.on-missing-label = skip}.
     *
     * @param statements               the parameterized statements (run in order)
     * @param skippableOnMissingLabel  per-statement flags, aligned by index with {@code statements};
     *                                 {@code null} means no statement may be skipped
     * @return the summed edge written-count and the number of skipped statements
     */
    public BatchWriteResult writeBatch(List<CypherStatement> statements,
                                       boolean[] skippableOnMissingLabel) {
        if (statements == null || statements.isEmpty()) {
            return new BatchWriteResult(NO_WRITTEN_COUNT, 0);
        }
        ensureOpen();
        return retryPolicy().execute("write", () -> writeBatchOnce(statements, skippableOnMissingLabel));
    }

    /**
     * Executes independent statements concurrently. Budget mode applies one shared deadline to the
     * complete flush and waits for all tasks in an attempt before replay; compatibility mode keeps
     * the 0.2 per-statement {@link TuGraphSinkOptions#maxRetries()} behaviour.
     */
    public BatchWriteResult writeBatchConcurrently(List<CypherStatement> statements,
                                                    boolean[] skippableOnMissingLabel,
                                                    ExecutorService executor) {
        if (statements == null || statements.isEmpty()) {
            return new BatchWriteResult(NO_WRITTEN_COUNT, 0);
        }
        ensureOpen();
        if (options.retryBudgetMs() == 0) {
            return writeBatchConcurrentlyLegacy(
                    statements, skippableOnMissingLabel, executor);
        }
        return retryPolicy().execute("parallel write",
                () -> writeBatchConcurrentlyOnce(statements, skippableOnMissingLabel, executor));
    }

    /**
     * Execute a read query, returning each row as a map of result alias to value (raw Bolt Java
     * types: String / Long / Double / Boolean / List / Map / null). Retries transient failures.
     *
     * @param stmt the parameterized read query
     * @return the result rows
     */
    public List<Map<String, Object>> read(CypherStatement stmt) {
        ensureOpen();
        return retryPolicy().execute("read", () -> {
            try (Session session = driver.session(SessionConfig.forDatabase(options.graph()))) {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (Record record : session.run(stmt.cypher(), stmt.parameters()).list()) {
                    rows.add(record.asMap());
                }
                return rows;
            }
        });
    }
    /**
     * Whether the failure is TuGraph rejecting a statement because its vertex label is not defined
     * in the graph schema (e.g. {@code DatabaseException: No such vertex label: orders}). Such
     * errors are deterministic per record — retrying cannot fix them, but other records in the
     * batch are unaffected.
     */
    static boolean isMissingVertexLabel(Neo4jException ex) {
        if (ex instanceof TransientException || ex instanceof ServiceUnavailableException
                || ex instanceof SessionExpiredException) {
            return false;
        }
        String message = ex.getMessage();
        return message != null && message.contains("No such vertex label");
    }

    /** Read a connector MERGE count or native procedure insert/update count, if present. */
    private static long readWrittenCount(Result result) {
        List<Record> records = result.list();
        if (records.isEmpty()) {
            return NO_WRITTEN_COUNT;
        }
        Record first = records.get(0);
        if (first.containsKey(CypherStatementBuilder.WRITTEN_COUNT_FIELD)) {
            return first.get(CypherStatementBuilder.WRITTEN_COUNT_FIELD).asLong(NO_WRITTEN_COUNT);
        }
        if (first.containsKey("insert") || first.containsKey("update")) {
            long insert = first.containsKey("insert")
                    ? first.get("insert").asLong(0L) : 0L;
            long update = first.containsKey("update")
                    ? first.get("update").asLong(0L) : 0L;
            return insert + update;
        }
        return NO_WRITTEN_COUNT;
    }

    /** Whether {@code failure} or any cause is safe to retry after waiting for TuGraph to recover. */
    public static boolean isRetryableFailure(Throwable failure) {
        return RetryPolicy.isRetryable(failure);
    }

    private Driver newDriver() {
        long connectionTimeoutMs = options.connectionTimeoutMs();
        long acquisitionTimeoutMs = Math.max(connectionTimeoutMs, 60_000L);
        if (options.retryBudgetMs() > 0) {
            connectionTimeoutMs = Math.min(connectionTimeoutMs, options.retryBudgetMs());
            acquisitionTimeoutMs = Math.min(acquisitionTimeoutMs, options.retryBudgetMs());
        }
        Config config = Config.builder()
                .withMaxConnectionPoolSize(options.maxConnectionPoolSize())
                .withConnectionTimeout(connectionTimeoutMs, TimeUnit.MILLISECONDS)
                .withConnectionAcquisitionTimeout(acquisitionTimeoutMs, TimeUnit.MILLISECONDS)
                .build();
        return GraphDatabase.driver(
                options.uri(), AuthTokens.basic(options.username(), options.password()), config);
    }

    private BatchWriteResult writeBatchOnce(List<CypherStatement> statements,
                                            boolean[] skippableOnMissingLabel) {
        try (Session session = driver.session(SessionConfig.forDatabase(options.graph()))) {
            long written = NO_WRITTEN_COUNT;
            int skipped = 0;
            for (int i = 0; i < statements.size(); i++) {
                CypherStatement stmt = statements.get(i);
                long count;
                try {
                    // TuGraph supports auto-commit only. A retry replays the ordered group;
                    // idempotent MERGE/delete statements make partial auto-commits safe.
                    count = readWrittenCount(session.run(stmt.cypher(), stmt.parameters()));
                } catch (Neo4jException failure) {
                    if (skippableOnMissingLabel != null && skippableOnMissingLabel[i]
                            && isMissingVertexLabel(failure)) {
                        skipped++;
                        LOG.debug("Skipping statement for a missing vertex label: {}",
                                failure.getMessage());
                        continue;
                    }
                    throw failure;
                }
                if (count != NO_WRITTEN_COUNT) {
                    written = (written == NO_WRITTEN_COUNT ? 0L : written) + count;
                }
            }
            return new BatchWriteResult(written, skipped);
        }
    }

    private BatchWriteResult writeBatchConcurrentlyOnce(List<CypherStatement> statements,
                                                         boolean[] skippableOnMissingLabel,
                                                         ExecutorService executor) {
        List<Callable<BatchWriteResult>> tasks = new ArrayList<>(statements.size());
        for (int i = 0; i < statements.size(); i++) {
            List<CypherStatement> single = Collections.singletonList(statements.get(i));
            boolean[] singleSkippable = skippableOnMissingLabel == null
                    ? null
                    : new boolean[] {skippableOnMissingLabel[i]};
            tasks.add(() -> writeBatchOnce(single, singleSkippable));
        }

        try {
            List<Future<BatchWriteResult>> futures = executor.invokeAll(tasks);
            long written = NO_WRITTEN_COUNT;
            int skipped = 0;
            for (Future<BatchWriteResult> future : futures) {
                BatchWriteResult result = future.get();
                if (result.written() != NO_WRITTEN_COUNT) {
                    written = (written == NO_WRITTEN_COUNT ? 0L : written) + result.written();
                }
                skipped += result.skippedMissingLabel();
            }
            return new BatchWriteResult(written, skipped);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while writing a TuGraph batch concurrently",
                    interrupted);
        } catch (ExecutionException failedTask) {
            Throwable cause = failedTask.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException("Concurrent TuGraph write failed", cause);
        }
    }

    /** Preserve 0.2 semantics: every parallel statement owns its own max-retry counter. */
    private BatchWriteResult writeBatchConcurrentlyLegacy(List<CypherStatement> statements,
                                                           boolean[] skippableOnMissingLabel,
                                                           ExecutorService executor) {
        List<Future<BatchWriteResult>> futures = new ArrayList<>(statements.size());
        for (int i = 0; i < statements.size(); i++) {
            List<CypherStatement> single = Collections.singletonList(statements.get(i));
            boolean[] singleSkippable = skippableOnMissingLabel == null
                    ? null
                    : new boolean[] {skippableOnMissingLabel[i]};
            futures.add(executor.submit(() -> writeBatch(single, singleSkippable)));
        }
        return collectBatchResults(futures);
    }

    private static BatchWriteResult collectBatchResults(List<Future<BatchWriteResult>> futures) {
        long written = NO_WRITTEN_COUNT;
        int skipped = 0;
        try {
            for (Future<BatchWriteResult> future : futures) {
                BatchWriteResult result = future.get();
                if (result.written() != NO_WRITTEN_COUNT) {
                    written = (written == NO_WRITTEN_COUNT ? 0L : written) + result.written();
                }
                skipped += result.skippedMissingLabel();
            }
            return new BatchWriteResult(written, skipped);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while writing a TuGraph batch concurrently",
                    interrupted);
        } catch (ExecutionException failedTask) {
            Throwable cause = failedTask.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException("Concurrent TuGraph write failed", cause);
        }
    }

    private RetryPolicy retryPolicy() {
        if (retryPolicy == null) {
            retryPolicy = new RetryPolicy(options, retryListener);
        }
        return retryPolicy;
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
