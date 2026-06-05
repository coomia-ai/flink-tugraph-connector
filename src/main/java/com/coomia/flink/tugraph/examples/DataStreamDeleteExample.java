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
 * DataStream example: upsert vertices and then <b>delete</b> one with {@code Vertex.asDelete()} —
 * the Java-API mirror of an SQL changelog DELETE. The sink applies operations in arrival order, so
 * the final graph reflects the last change to each key.
 */
public final class DataStreamDeleteExample {

    private DataStreamDeleteExample() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Vertex> changes = env.fromData(
                new Vertex("Company", "company_id", "c1", company("c1", "Acme")),   // upsert c1
                new Vertex("Company", "company_id", "c2", company("c2", "Beta")),   // upsert c2
                new Vertex("Company", "company_id", "c1", company("c1", "Acme")).asDelete()); // delete c1

        changes.sinkTo(TuGraphSink.<Vertex>builder()
                .uri("bolt://127.0.0.1:7687")
                .auth("admin", "73@TuGraph")
                .graph("default")
                .batchSize(1) // flush per record so insert-then-delete ordering is exact
                .build());

        // Result in TuGraph: only :Company c2 remains.
        env.execute("tugraph-datastream-delete-example");
    }

    private static Map<String, Object> company(String id, String name) {
        Map<String, Object> props = new HashMap<>();
        props.put("company_id", id);
        props.put("name", name);
        return props;
    }
}
