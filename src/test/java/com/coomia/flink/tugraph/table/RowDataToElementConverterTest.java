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

package com.coomia.flink.tugraph.table;

import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.GraphElement;
import com.coomia.flink.tugraph.element.Vertex;
import com.coomia.flink.tugraph.table.TuGraphConnectorOptions.ElementType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RowDataToElementConverter} type mapping (§7.2), no Flink runtime needed. */
class RowDataToElementConverterTest {

    @Test
    void convertsVertexWithTypeWidening() {
        String[] names = {"company_id", "name", "reg_capital", "employees", "active"};
        LogicalType[] types = {
                DataTypes.STRING().getLogicalType(),
                DataTypes.STRING().getLogicalType(),
                DataTypes.DOUBLE().getLogicalType(),
                DataTypes.INT().getLogicalType(),
                DataTypes.BOOLEAN().getLogicalType()
        };
        RowDataToElementConverter converter = RowDataToElementConverter.builder()
                .elementType(ElementType.VERTEX)
                .schema(names, types)
                .vertex("Company", 0)
                .build();

        GenericRowData row = GenericRowData.of(
                StringData.fromString("c1"), StringData.fromString("Acme"), 100.5d, 42, true);

        GraphElement e = converter.convert(row);
        assertThat(e).isInstanceOf(Vertex.class);
        Vertex v = (Vertex) e;
        assertThat(v.label()).isEqualTo("Company");
        assertThat(v.primaryKey()).isEqualTo("company_id");
        assertThat(v.primaryKeyValue()).isEqualTo("c1");
        assertThat(v.properties())
                .containsEntry("company_id", "c1")
                .containsEntry("name", "Acme")
                .containsEntry("reg_capital", 100.5d)
                .containsEntry("employees", 42L)   // INT widened to Long
                .containsEntry("active", true);
    }

    @Test
    void dropsNullColumns() {
        String[] names = {"id", "note"};
        LogicalType[] types = {DataTypes.STRING().getLogicalType(), DataTypes.STRING().getLogicalType()};
        RowDataToElementConverter converter = RowDataToElementConverter.builder()
                .elementType(ElementType.VERTEX)
                .schema(names, types)
                .vertex("V", 0)
                .build();

        GenericRowData row = GenericRowData.of(StringData.fromString("v1"), null);

        Vertex v = (Vertex) converter.convert(row);
        assertThat(v.properties()).containsKey("id").doesNotContainKey("note");
    }

    @Test
    void convertsEdgeAndExcludesEndpointColumnsFromProps() {
        String[] names = {"src_company", "dst_company", "ratio"};
        LogicalType[] types = {
                DataTypes.STRING().getLogicalType(),
                DataTypes.STRING().getLogicalType(),
                DataTypes.DOUBLE().getLogicalType()
        };
        RowDataToElementConverter converter = RowDataToElementConverter.builder()
                .elementType(ElementType.EDGE)
                .schema(names, types)
                .edge("INVEST",
                        "Company", "company_id", 0,
                        "Company", "company_id", 1)
                .build();

        GenericRowData row = GenericRowData.of(
                StringData.fromString("c1"), StringData.fromString("c2"), 0.3d);

        GraphElement e = converter.convert(row);
        assertThat(e).isInstanceOf(Edge.class);
        Edge edge = (Edge) e;
        assertThat(edge.label()).isEqualTo("INVEST");
        assertThat(edge.srcLabel()).isEqualTo("Company");
        assertThat(edge.srcKey()).isEqualTo("company_id");
        assertThat(edge.srcValue()).isEqualTo("c1");
        assertThat(edge.dstValue()).isEqualTo("c2");
        // Endpoint key columns must not leak into edge properties.
        assertThat(edge.properties())
                .containsEntry("ratio", 0.3d)
                .doesNotContainKey("src_company")
                .doesNotContainKey("dst_company");
    }

    @Test
    void mapsTemporalTypesToIsoStrings() {
        String[] names = {"id", "d"};
        LogicalType[] types = {DataTypes.STRING().getLogicalType(), DataTypes.DATE().getLogicalType()};
        RowDataToElementConverter converter = RowDataToElementConverter.builder()
                .elementType(ElementType.VERTEX)
                .schema(names, types)
                .vertex("V", 0)
                .build();

        // 2026-06-04 is epoch day 20608.
        int epochDay = (int) java.time.LocalDate.of(2026, 6, 4).toEpochDay();
        GenericRowData row = GenericRowData.of(StringData.fromString("v1"), epochDay);

        Vertex v = (Vertex) converter.convert(row);
        assertThat(v.properties().get("d")).isEqualTo("2026-06-04");
    }

    @Test
    void convertSkipsUpdateBefore() {
        RowDataToElementConverter c = vertexConverter();
        GenericRowData row = GenericRowData.ofKind(
                RowKind.UPDATE_BEFORE, StringData.fromString("v1"), StringData.fromString("A"));
        assertThat(c.convert(row)).isNull();
    }

    @Test
    void isDeleteReflectsRowKind() {
        RowDataToElementConverter c = vertexConverter();
        GenericRowData del = GenericRowData.ofKind(
                RowKind.DELETE, StringData.fromString("v1"), StringData.fromString("A"));
        GenericRowData ins = GenericRowData.of(StringData.fromString("v1"), StringData.fromString("A"));
        assertThat(c.isDelete(del)).isTrue();
        assertThat(c.isDelete(ins)).isFalse();
    }

    private static RowDataToElementConverter vertexConverter() {
        String[] names = {"id", "name"};
        LogicalType[] types = {DataTypes.STRING().getLogicalType(), DataTypes.STRING().getLogicalType()};
        return RowDataToElementConverter.builder()
                .elementType(ElementType.VERTEX).schema(names, types).vertex("V", 0).build();
    }
}
