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
import com.coomia.flink.tugraph.table.TuGraphConnectorOptions.ElementType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.CONNECTION_TIMEOUT;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_DST_COL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_DST_KEY;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_DST_LABEL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_LABEL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_ON_MISSING_ENDPOINT;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_SRC_COL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_SRC_KEY;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_SRC_LABEL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.ELEMENT_TYPE;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.GRAPH;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.MAX_CONNECTION_POOL_SIZE;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.PASSWORD;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.SINK_BATCH_INTERVAL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.SINK_BATCH_SIZE;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.SINK_MAX_RETRIES;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.URI;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.USERNAME;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.VERTEX_LABEL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.VERTEX_PRIMARY_KEY;

/**
 * Factory for the {@code 'connector' = 'tugraph'} Table/SQL sink, discovered via Java SPI
 * ({@code META-INF/services/org.apache.flink.table.factories.Factory}).
 */
public class TuGraphDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "tugraph";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URI);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(ELEMENT_TYPE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GRAPH);
        options.add(CONNECTION_TIMEOUT);
        options.add(MAX_CONNECTION_POOL_SIZE);
        options.add(VERTEX_LABEL);
        options.add(VERTEX_PRIMARY_KEY);
        options.add(EDGE_LABEL);
        options.add(EDGE_SRC_LABEL);
        options.add(EDGE_SRC_COL);
        options.add(EDGE_SRC_KEY);
        options.add(EDGE_DST_LABEL);
        options.add(EDGE_DST_COL);
        options.add(EDGE_DST_KEY);
        options.add(EDGE_ON_MISSING_ENDPOINT);
        options.add(SINK_BATCH_SIZE);
        options.add(SINK_BATCH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        TuGraphSinkOptions sinkOptions = TuGraphSinkOptions.builder()
                .uri(config.get(URI))
                .auth(config.get(USERNAME), config.get(PASSWORD))
                .graph(config.get(GRAPH))
                .batchSize(config.get(SINK_BATCH_SIZE))
                .batchIntervalMs(config.get(SINK_BATCH_INTERVAL).toMillis())
                .maxRetries(config.get(SINK_MAX_RETRIES))
                .connectionTimeoutMs(config.get(CONNECTION_TIMEOUT).toMillis())
                .maxConnectionPoolSize(config.get(MAX_CONNECTION_POOL_SIZE))
                .onMissingEndpoint(config.get(EDGE_ON_MISSING_ENDPOINT))
                .build();

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        List<String> names = schema.getColumnNames();
        String[] fieldNames = names.toArray(new String[0]);
        LogicalType[] fieldTypes = schema.getColumnDataTypes().stream()
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new);

        ElementType elementType = config.get(ELEMENT_TYPE);
        RowDataToElementConverter.Builder converter = RowDataToElementConverter.builder()
                .elementType(elementType)
                .schema(fieldNames, fieldTypes);

        if (elementType == ElementType.VERTEX) {
            String label = require(config, VERTEX_LABEL, elementType);
            String pkCol = config.getOptional(VERTEX_PRIMARY_KEY).orElseGet(() ->
                    schema.getPrimaryKey()
                            .map(uc -> uc.getColumns().get(0))
                            .orElseThrow(() -> new ValidationException(
                                    "Option '" + VERTEX_PRIMARY_KEY.key() + "' is not set and the table"
                                            + " declares no PRIMARY KEY to infer the vertex primary key from.")));
            converter.vertex(label, columnIndex(names, pkCol, VERTEX_PRIMARY_KEY.key()));
        } else {
            String edgeLabel = require(config, EDGE_LABEL, elementType);
            String srcLabel = require(config, EDGE_SRC_LABEL, elementType);
            String srcCol = require(config, EDGE_SRC_COL, elementType);
            String srcKey = config.getOptional(EDGE_SRC_KEY).orElse(srcCol);
            String dstLabel = require(config, EDGE_DST_LABEL, elementType);
            String dstCol = require(config, EDGE_DST_COL, elementType);
            String dstKey = config.getOptional(EDGE_DST_KEY).orElse(dstCol);
            converter.edge(edgeLabel,
                    srcLabel, srcKey, columnIndex(names, srcCol, EDGE_SRC_COL.key()),
                    dstLabel, dstKey, columnIndex(names, dstCol, EDGE_DST_COL.key()));
        }

        return new TuGraphDynamicTableSink(sinkOptions, converter.build());
    }

    private static String require(ReadableConfig config, ConfigOption<String> option, ElementType type) {
        return config.getOptional(option).orElseThrow(() -> new ValidationException(
                "Option '" + option.key() + "' is required when '" + ELEMENT_TYPE.key()
                        + "' = " + type.name().toLowerCase() + "."));
    }

    private static int columnIndex(List<String> names, String column, String optionKey) {
        int idx = names.indexOf(column);
        if (idx < 0) {
            throw new ValidationException("Column '" + column + "' referenced by option '" + optionKey
                    + "' does not exist in the table schema " + names + ".");
        }
        return idx;
    }
}
