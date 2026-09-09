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
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    @Test
    void budgetedRetry_recoversWithExponentialBackoffWithoutSleepingForReal() {
        TuGraphSinkOptions options = optionsBuilder()
                .retryBudgetMs(90_000)
                .retryInitialBackoffMs(1_000)
                .retryMaxBackoffMs(10_000)
                .build();
        AtomicLong nowNanos = new AtomicLong();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();
        List<Long> sleeps = new ArrayList<>();
        RetryPolicy policy = new RetryPolicy(options, retries::incrementAndGet,
                nowNanos::get, millis -> {
                    sleeps.add(millis);
                    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
                });

        String result = policy.execute("write", () -> {
            if (attempts.incrementAndGet() <= 6) {
                throw new ServiceUnavailableException("leader unavailable");
            }
            return "written";
        });

        assertThat(result).isEqualTo("written");
        assertThat(attempts).hasValue(7);
        assertThat(retries).hasValue(6);
        assertThat(sleeps).containsExactly(1_000L, 2_000L, 4_000L, 8_000L, 10_000L, 10_000L);
    }

    @Test
    void budgetedRetry_stopsAtDeadlineAndRethrowsLastDriverFailure() {
        TuGraphSinkOptions options = optionsBuilder()
                .retryBudgetMs(2_500)
                .retryInitialBackoffMs(1_000)
                .retryMaxBackoffMs(2_000)
                .build();
        AtomicLong nowNanos = new AtomicLong();
        AtomicInteger attempts = new AtomicInteger();
        List<Long> sleeps = new ArrayList<>();
        ServiceUnavailableException failure = new ServiceUnavailableException("still unavailable");
        RetryPolicy policy = new RetryPolicy(options, null, nowNanos::get, millis -> {
            sleeps.add(millis);
            nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        });

        assertThatThrownBy(() -> policy.execute("write", () -> {
            attempts.incrementAndGet();
            throw failure;
        })).isSameAs(failure);

        assertThat(attempts).hasValue(2);
        assertThat(sleeps).containsExactly(1_000L, 1_500L);
        assertThat(TimeUnit.NANOSECONDS.toMillis(nowNanos.get())).isEqualTo(2_500L);
    }

    @Test
    void retryClassification_walksCauseChainButRejectsClientErrors() {
        assertThat(RetryPolicy.isRetryable(
                new RuntimeException("wrapped", new ServiceUnavailableException("down"))))
                .isTrue();
        assertThat(RetryPolicy.isRetryable(new SessionExpiredException("expired"))).isTrue();
        assertThat(RetryPolicy.isRetryable(
                new TransientException("Neo.TransientError.General.DatabaseUnavailable", "down")))
                .isTrue();
        assertThat(RetryPolicy.isRetryable(
                new ClientException("Neo.ClientError.Statement.SyntaxError", "bad cypher")))
                .isFalse();
        assertThat(RetryPolicy.isRetryable(
                new SecurityException("Neo.ClientError.Security.Unauthorized", "bad auth")))
                .isFalse();
    }

    @Test
    void budgetedRetry_doesNotRetryDeterministicFailure() {
        TuGraphSinkOptions options = optionsBuilder().retryBudgetMs(90_000).build();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();
        ClientException failure = new ClientException(
                "Neo.ClientError.Statement.SyntaxError", "bad cypher");
        RetryPolicy policy = new RetryPolicy(options, retries::incrementAndGet,
                System::nanoTime, millis -> { });

        assertThatThrownBy(() -> policy.execute("write", () -> {
            attempts.incrementAndGet();
            throw failure;
        })).isSameAs(failure);

        assertThat(attempts).hasValue(1);
        assertThat(retries).hasValue(0);
    }

    @Test
    void disabledBudget_preservesLegacyRetryCountAndBackoff() {
        TuGraphSinkOptions options = optionsBuilder().maxRetries(3).build();
        AtomicLong nowNanos = new AtomicLong();
        AtomicInteger attempts = new AtomicInteger();
        List<Long> sleeps = new ArrayList<>();
        RetryPolicy policy = new RetryPolicy(options, null, nowNanos::get, millis -> {
            sleeps.add(millis);
            nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        });

        assertThatThrownBy(() -> policy.execute("write", () -> {
            attempts.incrementAndGet();
            throw new ServiceUnavailableException("down");
        })).isInstanceOf(ServiceUnavailableException.class);

        assertThat(attempts).hasValue(4);
        assertThat(sleeps).containsExactly(200L, 400L, 800L);
    }

    @Test
    void disabledBudget_preservesLegacyDirectExceptionClassification() {
        TuGraphSinkOptions options = optionsBuilder().maxRetries(3).build();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();
        RuntimeException wrapped = new RuntimeException(
                "wrapped", new ServiceUnavailableException("down"));
        RetryPolicy policy = new RetryPolicy(options, retries::incrementAndGet,
                System::nanoTime, millis -> { });

        assertThatThrownBy(() -> policy.execute("write", () -> {
            attempts.incrementAndGet();
            throw wrapped;
        })).isSameAs(wrapped);

        assertThat(attempts).hasValue(1);
        assertThat(retries).hasValue(0);
    }

    private static TuGraphSinkOptions.Builder optionsBuilder() {
        return TuGraphSinkOptions.builder()
                .uri("bolt://localhost:7687")
                .auth("admin", "secret");
    }
}
