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

import com.coomia.flink.tugraph.element.Vertex;
import com.coomia.flink.tugraph.sink.TuGraphSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * DataStream example: write {@link Vertex} records to TuGraph with the idempotent {@code MERGE} sink.
 *
 * <p>Run against a TuGraph instance reachable at {@code bolt://127.0.0.1:7687}. Adjust the URI and
 * credentials to match your deployment.
 */
public final class VertexSinkExample {

    private VertexSinkExample() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Vertex> vertices = env.fromData(
                new Vertex("Company", "company_id", "c1", company("c1", "Acme", 1_000_000.0)),
                new Vertex("Company", "company_id", "c2", company("c2", "Beta", 500_000.0)));

        vertices.sinkTo(TuGraphSink.<Vertex>builder()
                .uri("bolt://127.0.0.1:7687")
                .auth("admin", "73@TuGraph")
                .graph("default")
                .batchSize(500)
                .batchIntervalMs(1_000)
                .maxRetries(3)
                .build());

        env.execute("tugraph-vertex-sink-example");
    }

    private static Map<String, Object> company(String id, String name, double regCapital) {
        Map<String, Object> props = new HashMap<>();
        props.put("company_id", id);
        props.put("name", name);
        props.put("reg_capital", regCapital);
        return props;
    }
}
