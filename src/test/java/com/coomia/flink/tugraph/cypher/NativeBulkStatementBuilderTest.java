/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package com.coomia.flink.tugraph.cypher;

import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.Vertex;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeBulkStatementBuilderTest {

    @Test
    void vertexCallUsesOneRowsParameterAndSkipsNullProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("id", "v-1");
        properties.put("name", "Alice");
        properties.put("optional", null);

        CypherStatement statement = NativeBulkStatementBuilder.vertex(
                "Customer", "id", List.of(new Vertex("Customer", "id", "v-1", properties)));

        assertThat(statement.cypher()).isEqualTo(
                "CALL db.upsertVertex('Customer', $__onto_rows)");
        assertThat(statement.parameters()).containsKey(NativeBulkStatementBuilder.ROWS_PARAM);
        assertThat(statement.parameters().get(NativeBulkStatementBuilder.ROWS_PARAM))
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
                .singleElement()
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .containsEntry("id", "v-1")
                .containsEntry("name", "Alice")
                .doesNotContainKey("optional");
    }

    @Test
    void edgeCallCarriesEndpointDescriptorsAndNullSafeProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("weight", 2);
        properties.put("ignored", null);
        Edge edge = new Edge("OWNS", "Customer", "id", "c-1",
                "Account", "id", "a-1", properties);

        CypherStatement statement = NativeBulkStatementBuilder.edge(
                "OWNS", "Customer", "id", "Account", "id", List.of(edge));

        assertThat(statement.cypher())
                .isEqualTo("CALL db.upsertEdge('OWNS', {type:'Customer', key:'_src'}, "
                        + "{type:'Account', key:'_dst'}, $__onto_rows)");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = ((List<Map<String, Object>>) statement.parameters()
                .get(NativeBulkStatementBuilder.ROWS_PARAM)).get(0);
        assertThat(row).containsEntry("_src", "c-1")
                .containsEntry("_dst", "a-1")
                .containsEntry("weight", 2)
                .doesNotContainKey("ignored");
    }

    @Test
    void rejectsUnsafeLabelsAndEndpointFieldCollisions() {
        assertThatThrownBy(() -> NativeBulkStatementBuilder.vertex(
                "Customer' CALL db.dropDB()", "id", List.of(
                        new Vertex("Customer", "id", "v-1", Map.of()))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeBulkStatementBuilder.edge(
                "OWNS", "Customer", "id", "Account", "id",
                List.of(new Edge("OWNS", "Customer", "id", "c-1", "Account", "id", "a-1",
                        Map.of("_src", "bad")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint field");
    }
}
