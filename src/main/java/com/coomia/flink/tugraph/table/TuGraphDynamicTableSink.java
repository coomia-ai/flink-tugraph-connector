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
import com.coomia.flink.tugraph.sink.TuGraphSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;

import java.util.Objects;

/**
 * {@link DynamicTableSink} for {@code 'connector' = 'tugraph'}. Wraps the shared
 * {@link TuGraphSink} via {@link SinkV2Provider} so the Table/SQL path reuses the exact same
 * buffering, batching and retry runtime as the DataStream path.
 *
 * <p>Reports {@link ChangelogMode#insertOnly()}: rows are upserted idempotently with {@code MERGE},
 * so an append stream of (possibly repeated) keys is written without duplicates. Retract/upsert
 * changelog handling (deletes) is out of scope for v0.1.
 */
public class TuGraphDynamicTableSink implements DynamicTableSink {

    private final TuGraphSinkOptions options;
    private final RowDataToElementConverter converter;

    public TuGraphDynamicTableSink(TuGraphSinkOptions options, RowDataToElementConverter converter) {
        this.options = Objects.requireNonNull(options, "options");
        this.converter = Objects.requireNonNull(converter, "converter");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new TuGraphSink<>(options, converter));
    }

    @Override
    public DynamicTableSink copy() {
        // options and converter are immutable, so sharing references is safe.
        return new TuGraphDynamicTableSink(options, converter);
    }

    @Override
    public String asSummaryString() {
        return "TuGraph[" + options.uri() + ", graph=" + options.graph() + "]";
    }
}
