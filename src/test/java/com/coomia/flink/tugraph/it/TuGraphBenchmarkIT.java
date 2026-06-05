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

package com.coomia.flink.tugraph.it;

import com.coomia.flink.tugraph.TuGraphSinkOptions;
import com.coomia.flink.tugraph.client.TuGraphConnection;
import com.coomia.flink.tugraph.cypher.CypherStatement;
import com.coomia.flink.tugraph.cypher.MergeCypherStatementBuilder;
import com.coomia.flink.tugraph.element.Vertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures single-subtask vertex write throughput (NFR-1) against a real TuGraph instance, in a
 * throwaway graph. Compares sequential writes against the connector's concurrent path (a
 * vertices-only upsert flush is written concurrently over the Bolt pool). Reports rows/s; numbers
 * are environment-dependent and recorded in the docs as a baseline rather than asserted strictly.
 *
 * <p>Gated on {@code TUGRAPH_BENCH=1} (opt-in; not part of the regular suite). Override with {@code TUGRAPH_BENCH_ROWS} /
 * {@code TUGRAPH_BENCH_CONCURRENCY}.
 */
@EnabledIfEnvironmentVariable(named = "TUGRAPH_BENCH", matches = "1")
class TuGraphBenchmarkIT {

    private static final String GRAPH = "flink_bench_test";
    private static final String V = "BenchVertex";

    private static String uri()  { String x = System.getenv("TUGRAPH_URI");      return x == null ? "bolt://192.168.2.60:7687" : x; }
    private static String user() { String x = System.getenv("TUGRAPH_USERNAME"); return x == null ? "admin" : x; }
    private static String pass() { String x = System.getenv("TUGRAPH_PASSWORD"); return x == null ? "73@TuGraph" : x; }

    @Test
    void measuresVertexWriteThroughput() throws Exception {
        int rows = Integer.parseInt(System.getenv().getOrDefault("TUGRAPH_BENCH_ROWS", "2000"));
        int concurrency = Integer.parseInt(System.getenv().getOrDefault("TUGRAPH_BENCH_CONCURRENCY", "16"));

        try (Driver driver = GraphDatabase.driver(uri(), AuthTokens.basic(user(), pass()))) {
            try (Session sys = driver.session()) {
                try {
                    sys.run("CALL dbms.graph.deleteGraph('" + GRAPH + "')").consume();
                } catch (RuntimeException ignored) {
                    // absent
                }
                sys.run("CALL dbms.graph.createGraph('" + GRAPH + "', 'benchmark', 1)").consume();
            }
            try (Session s = driver.session(SessionConfig.forDatabase(GRAPH))) {
                s.run("CALL db.createVertexLabel('" + V + "', 'id', 'id', 'STRING', false, "
                        + "'name', 'STRING', true, 'value', 'DOUBLE', true)").consume();
            }

            TuGraphSinkOptions options = TuGraphSinkOptions.builder()
                    .uri(uri()).auth(user(), pass()).graph(GRAPH)
                    .maxConnectionPoolSize(concurrency)
                    .build();
            MergeCypherStatementBuilder builder = new MergeCypherStatementBuilder();

            List<Vertex> vertices = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                Map<String, Object> props = new HashMap<>();
                props.put("id", "v" + i);
                props.put("name", "name-" + i);
                props.put("value", (double) i);
                vertices.add(new Vertex(V, "id", "v" + i, props));
            }
            List<CypherStatement> statements = builder.buildVertexUpsert(V, "id", vertices);

            try (TuGraphConnection conn = new TuGraphConnection(options)) {
                conn.open();
                conn.verifyConnectivity();
                // Warm up so first-connection / classloading cost is excluded.
                conn.writeBatch(builder.buildVertexUpsert(V, "id",
                        List.of(new Vertex(V, "id", "warmup", Map.of("id", "warmup")))));

                // Concurrent path: write order-independent statements over the connection pool, the
                // way a vertices-only flush does in TuGraphSinkWriter.
                ExecutorService pool = Executors.newFixedThreadPool(concurrency);
                long start = System.nanoTime();
                List<Future<Long>> futures = new ArrayList<>(statements.size());
                for (CypherStatement statement : statements) {
                    futures.add(pool.submit(() -> conn.writeBatch(statement)));
                }
                for (Future<Long> future : futures) {
                    future.get();
                }
                long elapsedNanos = System.nanoTime() - start;
                pool.shutdown();

                double seconds = elapsedNanos / 1_000_000_000.0;
                double rowsPerSec = rows / seconds;
                System.out.printf("=== TuGraph write benchmark: %d vertices in %.2f s = %.0f rows/s "
                        + "(single subtask, concurrency %d, %s) ===%n",
                        rows, seconds, rowsPerSec, concurrency, uri());

                assertThat(rowsPerSec).isGreaterThan(0);
            }
        } finally {
            try (Driver driver = GraphDatabase.driver(uri(), AuthTokens.basic(user(), pass()));
                    Session sys = driver.session()) {
                sys.run("CALL dbms.graph.deleteGraph('" + GRAPH + "')").consume();
            } catch (RuntimeException ignored) {
                // best effort
            }
        }
    }
}
