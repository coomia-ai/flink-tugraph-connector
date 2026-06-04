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

import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.Vertex;
import com.coomia.flink.tugraph.sink.TuGraphSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs DataStream jobs against a real TuGraph container via a local MiniCluster: writes vertices,
 * then edges, and asserts the resulting graph. Re-running the jobs must not change the counts
 * (idempotent {@code MERGE}).
 */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "TUGRAPH_IT", matches = "1")
class TuGraphSinkITCase {

    @Container
    private static final TuGraphTestContainer TUGRAPH = new TuGraphTestContainer();

    @BeforeAll
    static void createSchema() {
        try (Driver d = GraphDatabase.driver(TUGRAPH.boltUri(),
                AuthTokens.basic(TuGraphTestContainer.username(), TuGraphTestContainer.password()));
                Session s = d.session()) {
            s.run("CALL db.createVertexLabel('Company', 'company_id', 'company_id', 'STRING', false, 'name', 'STRING', true)").consume();
            s.run("CALL db.createEdgeLabel('INVEST', '[]', 'ratio', 'DOUBLE', true)").consume();
        }
    }


    @Test
    void writesVerticesAndEdgesIdempotently() throws Exception {
        runVertexJob();
        assertThat(GraphAssertions.countVertices(TUGRAPH, "Company")).isEqualTo(2L);

        runEdgeJob();
        assertThat(GraphAssertions.countEdges(TUGRAPH, "INVEST")).isEqualTo(1L);

        // Replay both jobs: idempotent MERGE means counts stay the same.
        runVertexJob();
        runEdgeJob();
        assertThat(GraphAssertions.countVertices(TUGRAPH, "Company")).isEqualTo(2L);
        assertThat(GraphAssertions.countEdges(TUGRAPH, "INVEST")).isEqualTo(1L);
    }

    private void runVertexJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Vertex c1 = new Vertex("Company", "company_id", "c1", Map.of("company_id", "c1", "name", "Acme"));
        Vertex c2 = new Vertex("Company", "company_id", "c2", Map.of("company_id", "c2", "name", "Beta"));
        env.fromData(c1, c2).sinkTo(TuGraphSink.<Vertex>builder()
                .uri(TUGRAPH.boltUri())
                .auth(TuGraphTestContainer.username(), TuGraphTestContainer.password())
                .batchSize(2)
                .build());
        env.execute("tugraph-vertex-job");
    }

    private void runEdgeJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Edge invest = new Edge("INVEST",
                "Company", "company_id", "c1",
                "Company", "company_id", "c2",
                Map.of("ratio", 0.3d));
        env.fromData(invest).sinkTo(TuGraphSink.<Edge>builder()
                .uri(TUGRAPH.boltUri())
                .auth(TuGraphTestContainer.username(), TuGraphTestContainer.password())
                .batchSize(1)
                .build());
        env.execute("tugraph-edge-job");
    }
}
