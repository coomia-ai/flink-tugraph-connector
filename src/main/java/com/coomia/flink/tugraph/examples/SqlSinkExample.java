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

package com.coomia.flink.tugraph.examples;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL example: declare TuGraph vertex and edge tables with {@code 'connector' = 'tugraph'}
 * and write rows with {@code INSERT INTO ... VALUES}. In production the {@code VALUES} would be a
 * {@code SELECT} from a Kafka/CDC source table.
 */
public final class SqlSinkExample {

    private SqlSinkExample() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                "CREATE TABLE company_vertex (\n"
                        + "  company_id  STRING,\n"
                        + "  name        STRING,\n"
                        + "  reg_capital DOUBLE,\n"
                        + "  PRIMARY KEY (company_id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector'       = 'tugraph',\n"
                        + "  'uri'             = 'bolt://127.0.0.1:7687',\n"
                        + "  'username'        = 'admin',\n"
                        + "  'password'        = '73@TuGraph',\n"
                        + "  'graph'           = 'default',\n"
                        + "  'element.type'    = 'vertex',\n"
                        + "  'vertex.label'    = 'Company',\n"
                        + "  'sink.batch.size' = '500'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE invest_edge (\n"
                        + "  src_company STRING,\n"
                        + "  dst_company STRING,\n"
                        + "  ratio       DOUBLE\n"
                        + ") WITH (\n"
                        + "  'connector'                = 'tugraph',\n"
                        + "  'uri'                      = 'bolt://127.0.0.1:7687',\n"
                        + "  'username'                 = 'admin',\n"
                        + "  'password'                 = '73@TuGraph',\n"
                        + "  'element.type'             = 'edge',\n"
                        + "  'edge.label'               = 'INVEST',\n"
                        + "  'edge.src.label'           = 'Company',\n"
                        + "  'edge.src.col'             = 'src_company',\n"
                        + "  'edge.src.key'             = 'company_id',\n"
                        + "  'edge.dst.label'           = 'Company',\n"
                        + "  'edge.dst.col'             = 'dst_company',\n"
                        + "  'edge.dst.key'             = 'company_id',\n"
                        + "  'edge.on-missing-endpoint' = 'skip'\n"
                        + ")");

        tEnv.executeSql(
                "INSERT INTO company_vertex VALUES ('c1','Acme',1000000.0), ('c2','Beta',500000.0)")
                .await();
        tEnv.executeSql(
                "INSERT INTO invest_edge VALUES ('c1','c2',0.30), ('c2','c1',0.15)")
                .await();
    }
}
