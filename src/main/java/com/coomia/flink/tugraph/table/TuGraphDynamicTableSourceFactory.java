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
 * Factory for the {@code 'connector' = 'tugraph'} Table/SQL <em>source</em> (bounded vertex scan and
 * dimension-table lookup), discovered via Java SPI. Shares the {@code tugraph} identifier with the
 * sink factory; Flink selects this one when a source is requested.
 *
 * <p>v0.2 reads vertices only ({@code element.type = vertex}).
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
        if (elementType != ElementType.VERTEX) {
            throw new ValidationException("The TuGraph source supports only '"
                    + ELEMENT_TYPE.key() + "' = vertex in this release.");
        }

        String label = config.getOptional(VERTEX_LABEL).orElseThrow(() -> new ValidationException(
                "Option '" + VERTEX_LABEL.key() + "' is required for a TuGraph source."));

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        List<String> names = schema.getColumnNames();
        String[] fieldNames = names.toArray(new String[0]);
        LogicalType[] fieldTypes = schema.getColumnDataTypes().stream()
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new);
        DataType fullDataType = schema.toPhysicalRowDataType();

        // Order/scan key: explicit option, else the table's primary key, else none (unordered paging).
        String primaryKey = config.getOptional(VERTEX_PRIMARY_KEY).orElseGet(() ->
                schema.getPrimaryKey().map(pk -> pk.getColumns().get(0)).orElse(null));

        TuGraphSinkOptions connectionOptions = TuGraphSinkOptions.builder()
                .uri(config.get(URI))
                .auth(config.get(USERNAME), config.get(PASSWORD))
                .graph(config.get(GRAPH))
                .maxRetries(config.get(LOOKUP_MAX_RETRIES))
                .connectionTimeoutMs(config.get(CONNECTION_TIMEOUT).toMillis())
                .maxConnectionPoolSize(config.get(MAX_CONNECTION_POOL_SIZE))
                .build();

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
}
