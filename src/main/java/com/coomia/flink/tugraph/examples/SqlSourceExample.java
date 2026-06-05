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
 * Flink SQL example: read vertices from TuGraph with the {@code 'connector' = 'tugraph'} source.
 *
 * <ul>
 *   <li><b>Bounded scan</b> with projection + filter push-down ({@code SELECT … WHERE …}).</li>
 *   <li><b>Dimension-table lookup</b> for streaming enrichment (the join SQL is shown below; run it
 *       against a stream that carries a processing-time attribute).</li>
 * </ul>
 *
 * <p>The {@code Company} vertex label must already exist in TuGraph.
 */
public final class SqlSourceExample {

    private SqlSourceExample() {
    }

    public static void main(String[] args) {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                "CREATE TABLE company_src (\n"
                        + "  company_id  STRING,\n"
                        + "  name        STRING,\n"
                        + "  reg_capital DOUBLE\n"
                        + ") WITH (\n"
                        + "  'connector'          = 'tugraph',\n"
                        + "  'uri'                = 'bolt://127.0.0.1:7687',\n"
                        + "  'username'           = 'admin',\n"
                        + "  'password'           = '73@TuGraph',\n"
                        + "  'element.type'       = 'vertex',\n"
                        + "  'vertex.label'       = 'Company',\n"
                        + "  'vertex.primary-key' = 'company_id',\n"
                        + "  'scan.fetch-size'    = '1000'\n"
                        + ")");

        // Projection (only company_id, name) and filter (reg_capital > 1e6) are pushed into TuGraph.
        tEnv.executeSql("SELECT company_id, name FROM company_src WHERE reg_capital > 1000000").print();

        // --- Dimension-table lookup (streaming enrichment) ---
        // CREATE TABLE company_dim ( company_id STRING, name STRING,
        //   PRIMARY KEY (company_id) NOT ENFORCED
        // ) WITH ( 'connector'='tugraph', 'uri'=..., 'element.type'='vertex', 'vertex.label'='Company',
        //   'lookup.cache.max-rows'='10000', 'lookup.cache.ttl'='10 min' );
        //
        // SELECT e.event_id, e.company_id, c.name
        // FROM events AS e
        // JOIN company_dim FOR SYSTEM_TIME AS OF e.proc_time AS c
        //   ON e.company_id = c.company_id;
    }
}
