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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs Flink SQL {@code INSERT INTO ... VALUES} jobs through the {@code 'connector' = 'tugraph'}
 * table sink against a real TuGraph container, covering both vertex and edge tables.
 */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "TUGRAPH_IT", matches = "1")
class TuGraphSqlSinkITCase {

    @Container
    private static final TuGraphTestContainer TUGRAPH = new TuGraphTestContainer();

    @Test
    void insertsVerticesAndEdgesViaSql() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                "CREATE TABLE company_vertex (\n"
                        + "  company_id STRING,\n"
                        + "  name STRING,\n"
                        + "  reg_capital DOUBLE,\n"
                        + "  PRIMARY KEY (company_id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'tugraph',\n"
                        + "  'uri' = '" + TUGRAPH.boltUri() + "',\n"
                        + "  'username' = '" + TuGraphTestContainer.username() + "',\n"
                        + "  'password' = '" + TuGraphTestContainer.password() + "',\n"
                        + "  'element.type' = 'vertex',\n"
                        + "  'vertex.label' = 'Company',\n"
                        + "  'sink.batch.size' = '2'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE invest_edge (\n"
                        + "  src_company STRING,\n"
                        + "  dst_company STRING,\n"
                        + "  ratio DOUBLE\n"
                        + ") WITH (\n"
                        + "  'connector' = 'tugraph',\n"
                        + "  'uri' = '" + TUGRAPH.boltUri() + "',\n"
                        + "  'username' = '" + TuGraphTestContainer.username() + "',\n"
                        + "  'password' = '" + TuGraphTestContainer.password() + "',\n"
                        + "  'element.type' = 'edge',\n"
                        + "  'edge.label' = 'INVEST',\n"
                        + "  'edge.src.label' = 'Company', 'edge.src.col' = 'src_company', 'edge.src.key' = 'company_id',\n"
                        + "  'edge.dst.label' = 'Company', 'edge.dst.col' = 'dst_company', 'edge.dst.key' = 'company_id'\n"
                        + ")");

        tEnv.executeSql(
                "INSERT INTO company_vertex VALUES ('c1','Acme',100.0),('c2','Beta',200.0)").await();
        assertThat(GraphAssertions.countVertices(TUGRAPH, "Company")).isEqualTo(2L);

        tEnv.executeSql("INSERT INTO invest_edge VALUES ('c1','c2',0.3)").await();
        assertThat(GraphAssertions.countEdges(TUGRAPH, "INVEST")).isEqualTo(1L);
    }
}
