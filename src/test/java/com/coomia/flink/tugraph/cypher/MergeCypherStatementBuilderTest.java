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

/** Pure-logic unit tests for the TuGraph-tuned {@link MergeCypherStatementBuilder}. */
class MergeCypherStatementBuilderTest {

    private final MergeCypherStatementBuilder builder = new MergeCypherStatementBuilder();

    @Test
    void vertexUpsert_buildsOneParameterizedStatementPerVertexWithPerPropertySet() {
        Map<String, Object> props = new HashMap<>();
        props.put("company_id", "c1");
        props.put("name", "Acme");
        props.put("reg_capital", 100.0d);

        List<CypherStatement> stmts = builder.buildVertexUpsert(
                "Company", "company_id", List.of(new Vertex("Company", "company_id", "c1", props)));

        assertThat(stmts).hasSize(1);
        // Plain identifiers; primary key not in SET; non-PK keys sorted for determinism.
        assertThat(stmts.get(0).cypher()).isEqualTo(
                "MERGE (n:Company {company_id: $pk})\n"
                        + "SET n.name = $p_name, n.reg_capital = $p_reg_capital");
        assertThat(stmts.get(0).parameters())
                .containsEntry("pk", "c1")
                .containsEntry("p_name", "Acme")
                .containsEntry("p_reg_capital", 100.0d);
    }

    @Test
    void vertexUpsert_oneStatementPerVertex() {
        List<CypherStatement> stmts = builder.buildVertexUpsert("Company", "id", List.of(
                new Vertex("Company", "id", "a", Map.of("id", "a", "name", "A")),
                new Vertex("Company", "id", "b", Map.of("id", "b", "name", "B"))));
        assertThat(stmts).hasSize(2);
        assertThat(stmts.get(0).parameters()).containsEntry("pk", "a").containsEntry("p_name", "A");
        assertThat(stmts.get(1).parameters()).containsEntry("pk", "b").containsEntry("p_name", "B");
    }

    @Test
    void vertexUpsert_doesNotPutPrimaryKeyInSet() {
        List<CypherStatement> stmts = builder.buildVertexUpsert(
                "Company", "company_id",
                List.of(new Vertex("Company", "company_id", "c1", Map.of("company_id", "c1", "name", "Acme"))));
        assertThat(stmts.get(0).cypher()).doesNotContain("n.company_id");
        assertThat(stmts.get(0).parameters()).doesNotContainKey("p_company_id");
    }

    @Test
    void vertexUpsert_withOnlyPrimaryKey_omitsSet() {
        List<CypherStatement> stmts = builder.buildVertexUpsert(
                "Company", "company_id",
                List.of(new Vertex("Company", "company_id", "c1", Map.of("company_id", "c1"))));
        assertThat(stmts.get(0).cypher()).isEqualTo("MERGE (n:Company {company_id: $pk})");
    }

    @Test
    void vertexUpsert_dropsNullProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("id", "c1");
        props.put("name", "Acme");
        props.put("note", null);
        List<CypherStatement> stmts = builder.buildVertexUpsert(
                "Company", "id", List.of(new Vertex("Company", "id", "c1", props)));
        assertThat(stmts.get(0).cypher()).contains("n.name").doesNotContain("note");
        assertThat(stmts.get(0).parameters()).doesNotContainKey("p_note");
    }

    @Test
    void edgeUpsert_buildsOneParameterizedStatementPerEdge() {
        Edge edge = new Edge("INVEST",
                "Company", "company_id", "c1",
                "Company", "company_id", "c2",
                Map.of("ratio", 0.3d));

        List<CypherStatement> stmts = builder.buildEdgeUpsert(
                "INVEST", "Company", "company_id", "Company", "company_id", List.of(edge));

        assertThat(stmts).hasSize(1);
        assertThat(stmts.get(0).cypher()).isEqualTo(
                "MATCH (a:Company {company_id: $_src}), (b:Company {company_id: $_dst})\n"
                        + "MERGE (a)-[e:INVEST]->(b)\n"
                        + "SET e.ratio = $p_ratio\n"
                        + "RETURN count(e) AS written");
        assertThat(stmts.get(0).parameters())
                .containsEntry("_src", "c1")
                .containsEntry("_dst", "c2")
                .containsEntry("p_ratio", 0.3d);
    }

    @Test
    void edgeUpsert_withoutProperties_omitsSet() {
        Edge edge = new Edge("LINKS", "A", "id", "a", "B", "id", "b", Map.of());
        List<CypherStatement> stmts = builder.buildEdgeUpsert("LINKS", "A", "id", "B", "id", List.of(edge));
        assertThat(stmts.get(0).cypher()).isEqualTo(
                "MATCH (a:A {id: $_src}), (b:B {id: $_dst})\n"
                        + "MERGE (a)-[e:LINKS]->(b)\n"
                        + "RETURN count(e) AS written");
    }

    @Test
    void emptyBatch_isRejected() {
        assertThatThrownBy(() -> builder.buildVertexUpsert("Company", "id", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.buildEdgeUpsert("E", "A", "id", "B", "id", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void vertexDelete_buildsDetachDeletePerVertex() {
        List<CypherStatement> stmts = builder.buildVertexDelete(
                "Company", "company_id",
                List.of(new Vertex("Company", "company_id", "c1", Map.of("company_id", "c1"))));
        assertThat(stmts).hasSize(1);
        assertThat(stmts.get(0).cypher())
                .isEqualTo("MATCH (n:Company {company_id: $pk}) DETACH DELETE n");
        assertThat(stmts.get(0).parameters()).containsEntry("pk", "c1");
    }

    @Test
    void edgeDelete_buildsMatchDeletePerEdge() {
        Edge edge = new Edge("INVEST",
                "Company", "company_id", "c1", "Company", "company_id", "c2", Map.of());
        List<CypherStatement> stmts = builder.buildEdgeDelete(
                "INVEST", "Company", "company_id", "Company", "company_id", List.of(edge));
        assertThat(stmts).hasSize(1);
        assertThat(stmts.get(0).cypher()).isEqualTo(
                "MATCH (a:Company {company_id: $_src})-[e:INVEST]->(b:Company {company_id: $_dst}) DELETE e");
        assertThat(stmts.get(0).parameters()).containsEntry("_src", "c1").containsEntry("_dst", "c2");
    }
    @Test
    void edgeUpsert_withMergeKeys_foldsKeyIntoMergePatternNotSet() {
        MergeCypherStatementBuilder mk = new MergeCypherStatementBuilder(List.of("rel_type"), false);
        Edge edge = new Edge("REL", "Company", "id", "c1", "Company", "id", "c2",
                Map.of("rel_type", "placed_by", "weight", 1.0d));
        List<CypherStatement> stmts = mk.buildEdgeUpsert("REL", "Company", "id", "Company", "id", List.of(edge));
        assertThat(stmts.get(0).cypher()).isEqualTo(
                "MATCH (a:Company {id: $_src}), (b:Company {id: $_dst})\n"
                        + "MERGE (a)-[e:REL {rel_type: $mk_rel_type}]->(b)\n"
                        + "SET e.weight = $p_weight\n"
                        + "RETURN count(e) AS written");
        assertThat(stmts.get(0).parameters())
                .containsEntry("mk_rel_type", "placed_by")
                .containsEntry("p_weight", 1.0d)
                .doesNotContainKey("p_rel_type");
    }

    @Test
    void edgeUpsert_createMode_mergesEndpoints() {
        MergeCypherStatementBuilder create = new MergeCypherStatementBuilder(List.of(), true);
        Edge edge = new Edge("REL", "Company", "id", "c1", "Company", "id", "c2", Map.of("w", 1.0d));
        List<CypherStatement> stmts = create.buildEdgeUpsert("REL", "Company", "id", "Company", "id", List.of(edge));
        assertThat(stmts.get(0).cypher()).isEqualTo(
                "MERGE (a:Company {id: $_src})\n"
                        + "MERGE (b:Company {id: $_dst})\n"
                        + "MERGE (a)-[e:REL]->(b)\n"
                        + "SET e.w = $p_w\n"
                        + "RETURN count(e) AS written");
    }

    @Test
    void edgeDelete_withMergeKeys_narrowsToSpecificEdge() {
        MergeCypherStatementBuilder mk = new MergeCypherStatementBuilder(List.of("rel_type"), false);
        Edge edge = new Edge("REL", "Company", "id", "c1", "Company", "id", "c2", Map.of("rel_type", "placed_by"));
        List<CypherStatement> stmts = mk.buildEdgeDelete("REL", "Company", "id", "Company", "id", List.of(edge));
        assertThat(stmts.get(0).cypher()).isEqualTo(
                "MATCH (a:Company {id: $_src})-[e:REL {rel_type: $mk_rel_type}]->(b:Company {id: $_dst}) DELETE e");
        assertThat(stmts.get(0).parameters()).containsEntry("mk_rel_type", "placed_by");
    }
    @Test
    void invalidIdentifier_isRejected() {
        // TuGraph has no identifier quoting, so unsafe names (e.g. with a space or backtick) are refused.
        assertThatThrownBy(() -> builder.buildVertexUpsert(
                "Comp any", "id", List.of(new Vertex("Comp any", "id", "c1", Map.of("id", "c1")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("valid TuGraph identifier");
    }
}
