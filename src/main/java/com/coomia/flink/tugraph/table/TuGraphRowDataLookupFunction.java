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
import com.coomia.flink.tugraph.client.TuGraphConnection;
import com.coomia.flink.tugraph.cypher.CypherQueryBuilder;
import com.coomia.flink.tugraph.cypher.CypherStatement;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Synchronous {@link LookupFunction} that point-queries TuGraph for a vertex by its join key and
 * returns the projected columns. Wrapped with a {@code LookupCache} by the table source when
 * caching is configured.
 *
 * <p>This is the connector's main streaming-enrichment entry point: a stream is joined against a
 * TuGraph vertex table to attach its properties / entity profile.
 */
public class TuGraphRowDataLookupFunction extends LookupFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(TuGraphRowDataLookupFunction.class);

    private final TuGraphSinkOptions options;
    private final CypherQueryBuilder queryBuilder;
    private final String label;
    private final String[] keyNames;
    private final LogicalType[] keyTypes;
    private final String[] returnNames;
    private final LogicalType[] returnTypes;
    private final String whereClause;
    private final Map<String, Object> whereParams;

    private transient TuGraphConnection connection;
    private transient RowToRowDataConverter rowConverter;

    public TuGraphRowDataLookupFunction(TuGraphSinkOptions options,
                                        CypherQueryBuilder queryBuilder,
                                        String label,
                                        String[] keyNames, LogicalType[] keyTypes,
                                        String[] returnNames, LogicalType[] returnTypes,
                                        String whereClause, Map<String, Object> whereParams) {
        this.options = options;
        this.queryBuilder = queryBuilder;
        this.label = label;
        this.keyNames = keyNames;
        this.keyTypes = keyTypes;
        this.returnNames = returnNames;
        this.returnTypes = returnTypes;
        this.whereClause = whereClause;
        this.whereParams = whereParams;
    }

    @Override
    public void open(FunctionContext context) {
        this.connection = new TuGraphConnection(options);
        this.connection.open();
        this.rowConverter = new RowToRowDataConverter(returnNames, returnTypes);
        LOG.info("Opened TuGraph lookup on label '{}' keyed by {}", label, Arrays.toString(keyNames));
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        List<Object> keyValues = new ArrayList<>(keyNames.length);
        for (int i = 0; i < keyNames.length; i++) {
            keyValues.add(RowDataConversions.toJava(keyRow, i, keyTypes[i]));
        }
        CypherStatement stmt = queryBuilder.buildVertexLookup(
                label, Arrays.asList(keyNames), keyValues, Arrays.asList(returnNames), whereClause, whereParams);

        List<Map<String, Object>> rows = connection.read(stmt);
        List<RowData> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            result.add(rowConverter.convert(row));
        }
        return result;
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
