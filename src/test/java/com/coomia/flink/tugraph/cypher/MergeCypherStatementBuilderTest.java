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

package com.coomia.flink.tugraph.cypher;

import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.Vertex;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure-logic unit tests for {@link MergeCypherStatementBuilder} (no Flink / TuGraph required). */
class MergeCypherStatementBuilderTest {

    private final MergeCypherStatementBuilder builder = new MergeCypherStatementBuilder();

    @Test
    void vertexUpsert_buildsBackquotedMergeTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put("company_id", "c1");
        props.put("name", "Acme");
        props.put("reg_capital", 100.0d);

        CypherStatement stmt = builder.buildVertexUpsert(
                "Company", "company_id", List.of(new Vertex("Company", "company_id", "c1", props)));

        assertThat(stmt.cypher())
                .isEqualTo("UNWIND $batch AS row\n"
                        + "MERGE (n:`Company` {`company_id`: row.id})\n"
                        + "SET n += row.props");
    }

    @Test
    @SuppressWarnings("unchecked")
    void vertexUpsert_buildsBatchParamWithIdAndProps() {
        Map<String, Object> props = new HashMap<>();
        props.put("company_id", "c1");
        props.put("name", "Acme");

        CypherStatement stmt = builder.buildVertexUpsert(
                "Company", "company_id", List.of(new Vertex("Company", "company_id", "c1", props)));

        List<Map<String, Object>> batch = (List<Map<String, Object>>) stmt.parameters().get("batch");
        assertThat(batch).hasSize(1);
        Map<String, Object> row = batch.get(0);
        assertThat(row).containsKeys("id", "props");
        assertThat(row.get("id")).isEqualTo("c1");
        assertThat((Map<String, Object>) row.get("props")).containsEntry("name", "Acme");
    }

    @Test
    @SuppressWarnings("unchecked")
    void vertexUpsert_dropsNullProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Acme");
        props.put("note", null); // must not be written, so it cannot overwrite an existing value

        CypherStatement stmt = builder.buildVertexUpsert(
                "Company", "id", List.of(new Vertex("Company", "id", "c1", props)));

        List<Map<String, Object>> batch = (List<Map<String, Object>>) stmt.parameters().get("batch");
        Map<String, Object> rowProps = (Map<String, Object>) batch.get(0).get("props");
        assertThat(rowProps).containsKey("name").doesNotContainKey("note");
    }

    @Test
    void edgeUpsert_buildsMatchMergeTemplateWithWrittenCount() {
        Edge edge = new Edge("INVEST",
                "Company", "company_id", "c1",
                "Company", "company_id", "c2",
                Map.of("ratio", 0.3d));

        CypherStatement stmt = builder.buildEdgeUpsert(
                "INVEST", "Company", "company_id", "Company", "company_id", List.of(edge));

        assertThat(stmt.cypher())
                .isEqualTo("UNWIND $batch AS row\n"
                        + "MATCH (a:`Company` {`company_id`: row.src}), (b:`Company` {`company_id`: row.dst})\n"
                        + "MERGE (a)-[e:`INVEST`]->(b)\n"
                        + "SET e += row.props\n"
                        + "RETURN count(e) AS written");
    }

    @Test
    @SuppressWarnings("unchecked")
    void edgeUpsert_buildsBatchRowsWithSrcDstProps() {
        Edge edge = new Edge("INVEST",
                "Company", "company_id", "c1",
                "Company", "company_id", "c2",
                Map.of("ratio", 0.3d));

        CypherStatement stmt = builder.buildEdgeUpsert(
                "INVEST", "Company", "company_id", "Company", "company_id", List.of(edge));

        List<Map<String, Object>> batch = (List<Map<String, Object>>) stmt.parameters().get("batch");
        assertThat(batch).hasSize(1);
        Map<String, Object> row = batch.get(0);
        assertThat(row.get("src")).isEqualTo("c1");
        assertThat(row.get("dst")).isEqualTo("c2");
        assertThat((Map<String, Object>) row.get("props")).containsEntry("ratio", 0.3d);
    }

    @Test
    void sanitizeIdentifier_escapesEmbeddedBackticks() {
        // A label trying to break out of the back-quoted identifier must be escaped, not interpolated.
        CypherStatement stmt = builder.buildVertexUpsert(
                "Comp`any", "id", List.of(new Vertex("Comp`any", "id", "c1", Map.of())));

        assertThat(stmt.cypher()).contains("MERGE (n:`Comp``any` {`id`: row.id})");
    }

    @Test
    void emptyBatch_isRejected() {
        assertThatThrownBy(() -> builder.buildVertexUpsert("Company", "id", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.buildEdgeUpsert(
                "E", "A", "id", "B", "id", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void blankIdentifier_isRejected() {
        assertThatThrownBy(() -> builder.buildVertexUpsert(
                "", "id", List.of(new Vertex("x", "id", "c1", Map.of()))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
