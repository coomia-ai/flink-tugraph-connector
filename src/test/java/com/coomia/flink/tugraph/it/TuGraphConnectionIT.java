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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end connectivity test: open a Bolt connection to a real TuGraph container, run an
 * idempotent vertex {@code MERGE}, and verify the write through a read-back. Verifies that
 * re-running the same batch does not create duplicates.
 *
 * <p>Gated on {@code TUGRAPH_IT=1} so it only runs where Docker and a TuGraph image are available.
 */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "TUGRAPH_IT", matches = "1")
class TuGraphConnectionIT {

    @Container
    private static final TuGraphTestContainer TUGRAPH = new TuGraphTestContainer();

    private TuGraphSinkOptions options() {
        return TuGraphSinkOptions.builder()
                .uri(TUGRAPH.boltUri())
                .auth(TuGraphTestContainer.username(), TuGraphTestContainer.password())
                .build();
    }

    @Test
    void writesAndIdempotentlyReplaysVertex() {
        MergeCypherStatementBuilder builder = new MergeCypherStatementBuilder();
        Vertex p1 = new Vertex("Person", "id", "p1", Map.of("id", "p1", "name", "Alice"));
        List<CypherStatement> upsert = builder.buildVertexUpsert("Person", "id", List.of(p1));

        try (TuGraphConnection conn = new TuGraphConnection(options())) {
            conn.open();
            conn.verifyConnectivity();

            conn.writeBatch(upsert);
            assertThat(GraphAssertions.countVertices(TUGRAPH, "Person")).isEqualTo(1L);

            // Idempotent replay: count must not grow.
            conn.writeBatch(upsert);
            assertThat(GraphAssertions.countVertices(TUGRAPH, "Person")).isEqualTo(1L);
        }
    }
}
