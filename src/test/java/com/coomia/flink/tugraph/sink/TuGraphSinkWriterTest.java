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
import com.coomia.flink.tugraph.client.TuGraphConnection;
import com.coomia.flink.tugraph.cypher.CypherStatement;
import com.coomia.flink.tugraph.cypher.MergeCypherStatementBuilder;
import com.coomia.flink.tugraph.element.Vertex;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TuGraphSinkWriterTest {

    @Test
    void nextWriteRecoversRetainedBufferAfterTransientTimerFailure() throws Exception {
        TuGraphSinkOptions options = options();
        FakeConnection connection = new FakeConnection(options)
                .thenFail(new ServiceUnavailableException("leader switching"))
                .thenSucceed()
                .thenSucceed();
        TestProcessingTimeService time = new TestProcessingTimeService();
        TuGraphSinkWriter<Vertex> writer = writer(options, connection, time);

        writer.write(vertex(1), null);
        time.setCurrentTime(1_000L);
        assertThat(connection.calls).isEqualTo(1);
        assertThat(asyncFailure(writer)).isInstanceOf(ServiceUnavailableException.class);

        assertThatCode(() -> writer.write(vertex(2), null)).doesNotThrowAnyException();
        assertThat(asyncFailure(writer)).isNull();
        assertThat(connection.calls).isEqualTo(2);

        writer.flush(true);
        assertThat(connection.calls).isEqualTo(3);
        assertThat(connection.successfulStatements).isEqualTo(2);
        writer.close();
    }

    @Test
    void laterTimersKeepRetryingTransientFailureAndRecoverWithoutNewInput() throws Exception {
        TuGraphSinkOptions options = options();
        FakeConnection connection = new FakeConnection(options)
                .thenFail(new ServiceUnavailableException("down-1"))
                .thenFail(new ServiceUnavailableException("down-2"))
                .thenSucceed();
        TestProcessingTimeService time = new TestProcessingTimeService();
        TuGraphSinkWriter<Vertex> writer = writer(options, connection, time);

        writer.write(vertex(1), null);
        time.setCurrentTime(1_000L);
        time.setCurrentTime(2_000L);
        time.setCurrentTime(3_000L);

        assertThat(connection.calls).isEqualTo(3);
        assertThat(connection.successfulStatements).isEqualTo(1);
        assertThat(asyncFailure(writer)).isNull();

        // Recovery flushed the retained record. The next write must not replay it again.
        writer.write(vertex(2), null);
        assertThat(connection.calls).isEqualTo(3);
        writer.close();
        assertThat(connection.successfulStatements).isEqualTo(2);
    }

    @Test
    void nonRetryableTimerFailureStillFailsOnTaskThread() throws Exception {
        TuGraphSinkOptions options = options();
        ClientException syntaxFailure = new ClientException(
                "Neo.ClientError.Statement.SyntaxError", "bad cypher");
        FakeConnection connection = new FakeConnection(options).thenFail(syntaxFailure);
        TestProcessingTimeService time = new TestProcessingTimeService();
        TuGraphSinkWriter<Vertex> writer = writer(options, connection, time);

        writer.write(vertex(1), null);
        time.setCurrentTime(1_000L);

        assertThatThrownBy(() -> writer.write(vertex(2), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("batch-interval timer failed")
                .hasCause(syntaxFailure);
        assertThat(connection.calls).isEqualTo(1);
        assertThatThrownBy(writer::close).isInstanceOf(IOException.class);
    }

    private static TuGraphSinkWriter<Vertex> writer(TuGraphSinkOptions options,
                                                     TuGraphConnection connection,
                                                     TestProcessingTimeService time) {
        return new TuGraphSinkWriter<>(options, ElementConverter.identity(),
                new MergeCypherStatementBuilder(),
                UnregisteredMetricsGroup.createSinkWriterMetricGroup(), time, connection);
    }

    private static TuGraphSinkOptions options() {
        return TuGraphSinkOptions.builder()
                .uri("bolt://localhost:7687")
                .auth("admin", "secret")
                .batchSize(10)
                .batchIntervalMs(1_000L)
                .build();
    }

    private static Vertex vertex(int id) {
        return Vertex.of("person", "id", id, Map.of("id", id));
    }

    private static Exception asyncFailure(TuGraphSinkWriter<?> writer) throws Exception {
        Field field = TuGraphSinkWriter.class.getDeclaredField("asyncFlushException");
        field.setAccessible(true);
        return (Exception) field.get(writer);
    }

    private static final class FakeConnection extends TuGraphConnection {
        private static final Object SUCCESS = new Object();

        private final Deque<Object> outcomes = new ArrayDeque<>();
        private int calls;
        private int successfulStatements;

        FakeConnection(TuGraphSinkOptions options) {
            super(options);
        }

        FakeConnection thenFail(RuntimeException failure) {
            outcomes.addLast(failure);
            return this;
        }

        FakeConnection thenSucceed() {
            outcomes.addLast(SUCCESS);
            return this;
        }

        @Override
        public synchronized void open() {
            // No external driver in this deterministic unit test.
        }

        @Override
        public BatchWriteResult writeBatch(List<CypherStatement> statements,
                                           boolean[] skippableOnMissingLabel) {
            return completeAttempt(statements);
        }

        @Override
        public BatchWriteResult writeBatchConcurrently(List<CypherStatement> statements,
                                                       boolean[] skippableOnMissingLabel,
                                                       ExecutorService executor) {
            return completeAttempt(statements);
        }

        private BatchWriteResult completeAttempt(List<CypherStatement> statements) {
            calls++;
            Object outcome = outcomes.isEmpty() ? SUCCESS : outcomes.removeFirst();
            if (outcome instanceof RuntimeException) {
                throw (RuntimeException) outcome;
            }
            successfulStatements += statements.size();
            return new BatchWriteResult(NO_WRITTEN_COUNT, 0);
        }

        @Override
        public synchronized void close() {
            // No-op.
        }
    }
}
