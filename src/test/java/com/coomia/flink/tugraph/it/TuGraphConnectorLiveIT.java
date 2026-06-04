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
import com.coomia.flink.tugraph.cypher.MergeCypherStatementBuilder;
import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.Vertex;
import com.coomia.flink.tugraph.sink.TuGraphSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end validation of the connector against a real TuGraph instance, run inside a dedicated
 * throwaway graph that is created and dropped by the test (no production data touched).
 *
 * <p>Covers both the low-level write path ({@link TuGraphConnection} + {@link MergeCypherStatementBuilder})
 * and a full Flink DataStream job through {@link TuGraphSink} on a local MiniCluster, asserting
 * idempotent writes of vertices and edges.
 *
 * <p>Gated on {@code TUGRAPH_LIVE=1}; endpoint/credentials from {@code TUGRAPH_URI},
 * {@code TUGRAPH_USERNAME}, {@code TUGRAPH_PASSWORD}. Defaults target the development instance.
 */
@EnabledIfEnvironmentVariable(named = "TUGRAPH_LIVE", matches = "1")
class TuGraphConnectorLiveIT {

    private static final String GRAPH = "flink_conn_test";
    private static final String V = "ProbeCompany";
    private static final String E = "PROBE_INVEST";

    private static String uri()  { return env("TUGRAPH_URI", "bolt://192.168.2.60:7687"); }
    private static String user() { return env("TUGRAPH_USERNAME", "admin"); }
    private static String pass() { return env("TUGRAPH_PASSWORD", "73@TuGraph"); }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? d : v;
    }

    private static Driver driver;

    @BeforeAll
    static void setUpGraphAndSchema() {
        driver = GraphDatabase.driver(uri(), AuthTokens.basic(user(), pass()));
        try (Session sys = driver.session()) {
            try {
                sys.run("CALL dbms.graph.deleteGraph('" + GRAPH + "')").consume();
            } catch (RuntimeException ignored) {
                // graph did not exist yet
            }
            sys.run("CALL dbms.graph.createGraph('" + GRAPH + "', 'flink connector IT', 1)").consume();
        }
        try (Session s = driver.session(SessionConfig.forDatabase(GRAPH))) {
            s.run("CALL db.createVertexLabel('" + V + "', 'company_id', "
                    + "'company_id', 'STRING', false, 'name', 'STRING', true)").consume();
            s.run("CALL db.createEdgeLabel('" + E + "', '[]', 'ratio', 'DOUBLE', true)").consume();
        }
    }

    @AfterAll
    static void dropGraph() {
        if (driver != null) {
            try (Session sys = driver.session()) {
                sys.run("CALL dbms.graph.deleteGraph('" + GRAPH + "')").consume();
            } catch (RuntimeException ignored) {
                // best-effort cleanup
            }
            driver.close();
        }
    }

    @BeforeEach
    void clearData() {
        try (Session s = driver.session(SessionConfig.forDatabase(GRAPH))) {
            s.run("MATCH (n:" + V + ") DETACH DELETE n").consume();
        }
    }

    private TuGraphSinkOptions options() {
        return TuGraphSinkOptions.builder().uri(uri()).auth(user(), pass()).graph(GRAPH).build();
    }

    private List<Vertex> vertices() {
        return List.of(
                new Vertex(V, "company_id", "p1", mutableMap("company_id", "p1", "name", "Acme")),
                new Vertex(V, "company_id", "p2", mutableMap("company_id", "p2", "name", "Beta")));
    }

    private List<Edge> edges() {
        return List.of(new Edge(E, V, "company_id", "p1", V, "company_id", "p2", mutableMap("ratio", 0.3d)));
    }

    @Test
    void writesThroughConnectionLayerIdempotently() {
        MergeCypherStatementBuilder builder = new MergeCypherStatementBuilder();
        try (TuGraphConnection conn = new TuGraphConnection(options())) {
            conn.open();
            conn.verifyConnectivity();

            conn.writeBatch(builder.buildVertexUpsert(V, "company_id", vertices()));
            assertThat(count("MATCH (n:" + V + ") RETURN count(n) AS c")).isEqualTo(2L);
            // The previously-fixed per-row binding bug: each vertex keeps its own property value.
            assertThat(single("MATCH (n:" + V + " {company_id:'p1'}) RETURN n.name AS v")).isEqualTo("Acme");
            assertThat(single("MATCH (n:" + V + " {company_id:'p2'}) RETURN n.name AS v")).isEqualTo("Beta");

            conn.writeBatch(builder.buildVertexUpsert(V, "company_id", vertices()));
            assertThat(count("MATCH (n:" + V + ") RETURN count(n) AS c")).isEqualTo(2L);

            long written = conn.writeBatch(builder.buildEdgeUpsert(E, V, "company_id", V, "company_id", edges()));
            assertThat(written).isEqualTo(1L);
            assertThat(count("MATCH ()-[e:" + E + "]->() RETURN count(e) AS c")).isEqualTo(1L);

            conn.writeBatch(builder.buildEdgeUpsert(E, V, "company_id", V, "company_id", edges()));
            assertThat(count("MATCH ()-[e:" + E + "]->() RETURN count(e) AS c")).isEqualTo(1L);
        }
    }

    @Test
    void writesThroughDataStreamSinkJob() throws Exception {
        // Vertex job.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData(vertices().get(0), vertices().get(1))
                .sinkTo(TuGraphSink.<Vertex>builder()
                        .uri(uri()).auth(user(), pass()).graph(GRAPH).batchSize(2).build());
        env.execute("tugraph-live-vertex-job");

        assertThat(count("MATCH (n:" + V + ") RETURN count(n) AS c")).isEqualTo(2L);
        assertThat(single("MATCH (n:" + V + " {company_id:'p1'}) RETURN n.name AS v")).isEqualTo("Acme");

        // Edge job (endpoints now exist).
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.setParallelism(1);
        env2.fromData(edges().get(0))
                .sinkTo(TuGraphSink.<Edge>builder()
                        .uri(uri()).auth(user(), pass()).graph(GRAPH).batchSize(1).build());
        env2.execute("tugraph-live-edge-job");

        assertThat(count("MATCH ()-[e:" + E + "]->() RETURN count(e) AS c")).isEqualTo(1L);
    }

    private static java.util.Map<String, Object> mutableMap(Object... kv) {
        java.util.Map<String, Object> m = new java.util.HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static long count(String cypher) {
        try (Session s = driver.session(SessionConfig.forDatabase(GRAPH))) {
            return s.run(cypher).single().get("c").asLong();
        }
    }

    private static String single(String cypher) {
        try (Session s = driver.session(SessionConfig.forDatabase(GRAPH))) {
            return s.run(cypher).single().get("v").asString();
        }
    }
}
