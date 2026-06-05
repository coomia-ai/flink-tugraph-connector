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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure-logic unit tests for {@link CypherQueryBuilder} (scan / lookup read queries). */
class CypherQueryBuilderTest {

    private final CypherQueryBuilder builder = new CypherQueryBuilder();

    @Test
    void lookup_buildsPointMatchWithProjection() {
        CypherStatement stmt = builder.buildVertexLookup(
                "Company", List.of("company_id"), List.of("c1"), List.of("company_id", "name"));

        assertThat(stmt.cypher()).isEqualTo(
                "MATCH (n:Company {company_id: $_k0})\n"
                        + "RETURN n.company_id AS company_id, n.name AS name");
        assertThat(stmt.parameters()).containsEntry("_k0", "c1");
    }

    @Test
    void lookup_supportsCompositeKey() {
        CypherStatement stmt = builder.buildVertexLookup(
                "E", List.of("a", "b"), List.of("x", "y"), List.of("a"));
        assertThat(stmt.cypher()).startsWith("MATCH (n:E {a: $_k0, b: $_k1})");
        assertThat(stmt.parameters()).containsEntry("_k0", "x").containsEntry("_k1", "y");
    }

    @Test
    void scan_buildsOrderedPaginatedProjection() {
        CypherStatement stmt = builder.buildVertexScan(
                "Company", List.of("company_id", "name"), "company_id", 10, 100);
        assertThat(stmt.cypher()).isEqualTo(
                "MATCH (n:Company)\n"
                        + "RETURN n.company_id AS company_id, n.name AS name\n"
                        + "ORDER BY n.company_id\n"
                        + "SKIP 10\n"
                        + "LIMIT 100");
    }

    @Test
    void scan_omitsOrderSkipLimitWhenNotNeeded() {
        CypherStatement stmt = builder.buildVertexScan("Company", List.of("id"), null, 0, -1);
        assertThat(stmt.cypher()).isEqualTo("MATCH (n:Company)\nRETURN n.id AS id");
    }

    @Test
    void invalidIdentifier_isRejected() {
        assertThatThrownBy(() -> builder.buildVertexScan("Comp any", List.of("id"), null, 0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("valid TuGraph identifier");
    }
}
