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
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
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
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_SRC_COL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_SRC_KEY;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.EDGE_SRC_LABEL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.ELEMENT_TYPE;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.GRAPH;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.LOOKUP_CACHE_TTL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.LOOKUP_MAX_RETRIES;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.MAX_CONNECTION_POOL_SIZE;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.PASSWORD;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.SCAN_FETCH_SIZE;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.URI;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.USERNAME;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.VERTEX_LABEL;
import static com.coomia.flink.tugraph.table.TuGraphConnectorOptions.VERTEX_PRIMARY_KEY;

/**
 * Factory for the {@code 'connector' = 'tugraph'} Table/SQL <em>source</em>, discovered via Java
 * SPI. Shares the {@code tugraph} identifier with the sink factory; Flink selects this one when a
 * source is requested. Supports a bounded vertex scan + dimension-table lookup
 * ({@code element.type = vertex}) and a bounded edge scan ({@code element.type = edge}).
 */
public class TuGraphDynamicTableSourceFactory implements DynamicTableSourceFactory {

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
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GRAPH);
        options.add(CONNECTION_TIMEOUT);
        options.add(MAX_CONNECTION_POOL_SIZE);
        options.add(ELEMENT_TYPE);
        options.add(VERTEX_LABEL);
        options.add(VERTEX_PRIMARY_KEY);
        options.add(EDGE_LABEL);
        options.add(EDGE_SRC_LABEL);
        options.add(EDGE_SRC_COL);
        options.add(EDGE_SRC_KEY);
        options.add(EDGE_DST_LABEL);
        options.add(EDGE_DST_COL);
        options.add(EDGE_DST_KEY);
        options.add(SCAN_FETCH_SIZE);
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL);
        options.add(LOOKUP_MAX_RETRIES);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        ElementType elementType = config.getOptional(ELEMENT_TYPE).orElse(ElementType.VERTEX);

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        List<String> names = schema.getColumnNames();
        String[] fieldNames = names.toArray(new String[0]);
        LogicalType[] fieldTypes = schema.getColumnDataTypes().stream()
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new);
        DataType fullDataType = schema.toPhysicalRowDataType();

        TuGraphSinkOptions connectionOptions = TuGraphSinkOptions.builder()
                .uri(config.get(URI))
                .auth(config.get(USERNAME), config.get(PASSWORD))
                .graph(config.get(GRAPH))
                .maxRetries(config.get(LOOKUP_MAX_RETRIES))
                .connectionTimeoutMs(config.get(CONNECTION_TIMEOUT).toMillis())
                .maxConnectionPoolSize(config.get(MAX_CONNECTION_POOL_SIZE))
                .build();

        if (elementType == ElementType.EDGE) {
            String edgeLabel = require(config, EDGE_LABEL);
            String srcLabel = require(config, EDGE_SRC_LABEL);
            String srcCol = require(config, EDGE_SRC_COL);
            String srcKey = config.getOptional(EDGE_SRC_KEY).orElse(srcCol);
            String dstLabel = require(config, EDGE_DST_LABEL);
            String dstCol = require(config, EDGE_DST_COL);
            String dstKey = config.getOptional(EDGE_DST_KEY).orElse(dstCol);
            return new TuGraphEdgeDynamicTableSource(connectionOptions, edgeLabel,
                    srcLabel, srcKey, srcCol, dstLabel, dstKey, dstCol,
                    config.get(SCAN_FETCH_SIZE), fieldNames, fieldTypes, fullDataType);
        }

        String label = config.getOptional(VERTEX_LABEL).orElseThrow(() -> new ValidationException(
                "Option '" + VERTEX_LABEL.key() + "' is required for a TuGraph vertex source."));
        String primaryKey = config.getOptional(VERTEX_PRIMARY_KEY).orElseGet(() ->
                schema.getPrimaryKey().map(pk -> pk.getColumns().get(0)).orElse(null));

        LookupCache cache = null;
        long maxRows = config.get(LOOKUP_CACHE_MAX_ROWS);
        if (maxRows > 0) {
            DefaultLookupCache.Builder builder = DefaultLookupCache.newBuilder().maximumSize(maxRows);
            config.getOptional(LOOKUP_CACHE_TTL).ifPresent(builder::expireAfterWrite);
            cache = builder.build();
        }

        return new TuGraphDynamicTableSource(connectionOptions, label, primaryKey,
                config.get(SCAN_FETCH_SIZE), cache, fieldNames, fieldTypes, fullDataType);
    }

    private static String require(ReadableConfig config, ConfigOption<String> option) {
        return config.getOptional(option).orElseThrow(() -> new ValidationException(
                "Option '" + option.key() + "' is required for a TuGraph edge source."));
    }
}
