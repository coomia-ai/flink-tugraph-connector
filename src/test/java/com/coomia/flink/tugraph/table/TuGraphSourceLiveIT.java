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

import com.coomia.flink.tugraph.TuGraphSinkOptions;
import com.coomia.flink.tugraph.cypher.CypherQueryBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live validation of the v0.2 TuGraph source against a real instance, in a throwaway graph: a
 * bounded SQL scan (with projection pushdown) and the dimension-table lookup function.
 *
 * <p>Gated on {@code TUGRAPH_LIVE=1}.
 */
@EnabledIfEnvironmentVariable(named = "TUGRAPH_LIVE", matches = "1")
class TuGraphSourceLiveIT {

    private static final String GRAPH = "flink_src_test";
    private static final String V = "ProbeCompany";

    private static String uri()  { String x = System.getenv("TUGRAPH_URI");      return x == null ? "bolt://192.168.2.60:7687" : x; }
    private static String user() { String x = System.getenv("TUGRAPH_USERNAME"); return x == null ? "admin" : x; }
    private static String pass() { String x = System.getenv("TUGRAPH_PASSWORD"); return x == null ? "73@TuGraph" : x; }

    private static Driver driver;

    @BeforeAll
    static void setUp() {
        driver = GraphDatabase.driver(uri(), AuthTokens.basic(user(), pass()));
        try (Session sys = driver.session()) {
            try {
                sys.run("CALL dbms.graph.deleteGraph('" + GRAPH + "')").consume();
            } catch (RuntimeException ignored) {
                // absent
            }
            sys.run("CALL dbms.graph.createGraph('" + GRAPH + "', 'source IT', 1)").consume();
        }
        try (Session s = driver.session(SessionConfig.forDatabase(GRAPH))) {
            s.run("CALL db.createVertexLabel('" + V + "', 'company_id', "
                    + "'company_id', 'STRING', false, 'name', 'STRING', true)").consume();
            for (String[] v : new String[][] {{"p1", "Acme"}, {"p2", "Beta"}, {"p3", "Gamma"}}) {
                s.run("MERGE (n:" + V + " {company_id:$pk}) SET n.name=$name",
                        Map.of("pk", v[0], "name", v[1])).consume();
            }
        }
    }

    @AfterAll
    static void tearDown() {
        if (driver != null) {
            try (Session sys = driver.session()) {
                sys.run("CALL dbms.graph.deleteGraph('" + GRAPH + "')").consume();
            } catch (RuntimeException ignored) {
                // best effort
            }
            driver.close();
        }
    }

    private void createSourceTable(TableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TABLE company_src (\n"
                        + "  company_id STRING,\n"
                        + "  name STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'tugraph',\n"
                        + "  'uri' = '" + uri() + "',\n"
                        + "  'username' = '" + user() + "',\n"
                        + "  'password' = '" + pass() + "',\n"
                        + "  'graph' = '" + GRAPH + "',\n"
                        + "  'element.type' = 'vertex',\n"
                        + "  'vertex.label' = '" + V + "',\n"
                        + "  'vertex.primary-key' = 'company_id'\n"
                        + ")");
    }

    @Test
    void boundedScanReadsAllVertices() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        createSourceTable(tEnv);

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql("SELECT company_id, name FROM company_src").collect()) {
            it.forEachRemaining(rows::add);
        }
        rows.sort(Comparator.comparing(r -> String.valueOf(r.getField(0))));

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getField(0)).isEqualTo("p1");
        assertThat(rows.get(0).getField(1)).isEqualTo("Acme");
        assertThat(rows.get(2).getField(1)).isEqualTo("Gamma");
    }

    @Test
    void projectionPushDownReturnsOnlyRequestedColumn() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        createSourceTable(tEnv);

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql("SELECT company_id FROM company_src").collect()) {
            it.forEachRemaining(rows::add);
        }
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getArity()).isEqualTo(1); // only the projected column
    }

    @Test
    void lookupFunctionPointQueriesByKey() throws Exception {
        TuGraphSinkOptions options = TuGraphSinkOptions.builder()
                .uri(uri()).auth(user(), pass()).graph(GRAPH).build();
        LogicalType str = DataTypes.STRING().getLogicalType();

        TuGraphRowDataLookupFunction fn = new TuGraphRowDataLookupFunction(
                options, new CypherQueryBuilder(), V,
                new String[] {"company_id"}, new LogicalType[] {str},
                new String[] {"company_id", "name"}, new LogicalType[] {str, str});
        fn.open(null);
        try {
            Collection<RowData> hit = fn.lookup(GenericRowData.of(StringData.fromString("p2")));
            assertThat(hit).hasSize(1);
            RowData row = hit.iterator().next();
            assertThat(row.getString(0).toString()).isEqualTo("p2");
            assertThat(row.getString(1).toString()).isEqualTo("Beta");

            Collection<RowData> miss = fn.lookup(GenericRowData.of(StringData.fromString("p9")));
            assertThat(miss).isEmpty();
        } finally {
            fn.close();
        }
    }
}
