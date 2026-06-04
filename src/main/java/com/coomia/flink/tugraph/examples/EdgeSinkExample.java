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

import com.coomia.flink.tugraph.TuGraphSinkOptions.OnMissingEndpoint;
import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.sink.TuGraphSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * DataStream example: write {@link Edge} records to TuGraph. The source and destination vertices
 * are matched by their primary key; if an endpoint is missing the edge is skipped (the default,
 * recorded via the {@code tugraph.edgeSkipped} metric) — set {@link OnMissingEndpoint#FAIL} to fail
 * instead. Make sure the endpoint vertices are written first (see {@link VertexSinkExample}).
 */
public final class EdgeSinkExample {

    private EdgeSinkExample() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Edge> edges = env.fromData(
                new Edge("INVEST",
                        "Company", "company_id", "c1",
                        "Company", "company_id", "c2",
                        Map.of("ratio", 0.30)),
                new Edge("INVEST",
                        "Company", "company_id", "c2",
                        "Company", "company_id", "c1",
                        Map.of("ratio", 0.15)));

        edges.sinkTo(TuGraphSink.<Edge>builder()
                .uri("bolt://127.0.0.1:7687")
                .auth("admin", "73@TuGraph")
                .graph("default")
                .batchSize(500)
                .onMissingEndpoint(OnMissingEndpoint.SKIP)
                .build());

        env.execute("tugraph-edge-sink-example");
    }
}
