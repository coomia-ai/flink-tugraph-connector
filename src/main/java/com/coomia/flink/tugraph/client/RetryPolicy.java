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
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Executes transient-failure retries using either the legacy count or a wall-clock budget. */
final class RetryPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(RetryPolicy.class);
    private static final long LEGACY_INITIAL_BACKOFF_MS = 200L;
    private static final long LEGACY_MAX_BACKOFF_MS = 10_000L;

    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final TuGraphSinkOptions options;
    private final Runnable retryListener;
    private final LongSupplier nanoTime;
    private final Sleeper sleeper;

    RetryPolicy(TuGraphSinkOptions options, Runnable retryListener) {
        this(options, retryListener, System::nanoTime, Thread::sleep);
    }

    RetryPolicy(TuGraphSinkOptions options,
                Runnable retryListener,
                LongSupplier nanoTime,
                Sleeper sleeper) {
        this.options = Objects.requireNonNull(options, "options");
        this.retryListener = retryListener == null ? () -> { } : retryListener;
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    <T> T execute(String operation, Supplier<T> action) {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(action, "action");
        return options.retryBudgetMs() > 0
                ? executeWithBudget(operation, action)
                : executeWithLegacyCount(operation, action);
    }

    private <T> T executeWithLegacyCount(String operation, Supplier<T> action) {
        int retries = 0;
        while (true) {
            try {
                return action.get();
            } catch (RuntimeException failure) {
                // Compatibility mode intentionally mirrors 0.2: only a directly thrown driver
                // transient is retried. Cause-chain classification belongs to budget mode.
                if (!isDirectlyRetryable(failure) || retries >= options.maxRetries()) {
                    throw failure;
                }
                long backoffMs = exponentialBackoff(
                        LEGACY_INITIAL_BACKOFF_MS, LEGACY_MAX_BACKOFF_MS, retries);
                notifyRetry();
                LOG.warn("Transient TuGraph {} failure (attempt {}/{}), retrying in {} ms: {}",
                        operation, retries + 1, options.maxRetries(), backoffMs,
                        failure.getMessage());
                sleep(backoffMs, operation);
                retries++;
            }
        }
    }

    private <T> T executeWithBudget(String operation, Supplier<T> action) {
        long budgetNanos = TimeUnit.MILLISECONDS.toNanos(options.retryBudgetMs());
        long startedAt = nanoTime.getAsLong();
        long deadline = saturatingAdd(startedAt, budgetNanos);
        int retries = 0;

        while (true) {
            try {
                return action.get();
            } catch (RuntimeException failure) {
                if (!isRetryable(failure)) {
                    throw failure;
                }

                long remainingNanos = deadline - nanoTime.getAsLong();
                if (remainingNanos <= 0) {
                    throw failure;
                }

                long desiredBackoffMs = exponentialBackoff(
                        options.retryInitialBackoffMs(), options.retryMaxBackoffMs(), retries);
                long remainingMs = Math.max(1L,
                        TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                long backoffMs = Math.min(desiredBackoffMs, remainingMs);

                notifyRetry();
                LOG.warn("Transient TuGraph {} failure (retry {}), retrying in {} ms"
                                + " (remaining budget {} ms): {}",
                        operation, retries + 1, backoffMs, remainingMs, failure.getMessage());
                sleep(backoffMs, operation);
                retries++;

                if (deadline - nanoTime.getAsLong() <= 0) {
                    // Preserve the driver's last exception as the public failure, rather than
                    // replacing it with a connector-specific timeout wrapper.
                    throw failure;
                }
            }
        }
    }

    private void notifyRetry() {
        retryListener.run();
    }

    private void sleep(long millis, String operation) {
        try {
            sleeper.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while backing off before a TuGraph " + operation + " retry",
                    interrupted);
        }
    }

    static boolean isRetryable(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (isDirectlyRetryable(current)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDirectlyRetryable(Throwable failure) {
        return failure instanceof ServiceUnavailableException
                || failure instanceof SessionExpiredException
                || failure instanceof TransientException;
    }

    private static long exponentialBackoff(long initialMs, long maxMs, int retry) {
        long value = initialMs;
        for (int i = 0; i < retry && value < maxMs; i++) {
            value = value > maxMs / 2 ? maxMs : value * 2;
        }
        return Math.min(value, maxMs);
    }

    private static long saturatingAdd(long left, long right) {
        long result = left + right;
        if (((left ^ result) & (right ^ result)) < 0) {
            return Long.MAX_VALUE;
        }
        return result;
    }
}
