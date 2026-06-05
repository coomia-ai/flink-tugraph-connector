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
import com.coomia.flink.tugraph.cypher.CypherQueryBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Objects;

/**
 * {@link DynamicTableSource} for {@code 'connector' = 'tugraph'} reads. Supports a bounded
 * {@link ScanTableSource} (paginated full vertex scan) and a {@link LookupTableSource} (point lookup
 * for streaming dimension-table enrichment, with an optional {@code LookupCache}), plus
 * {@link SupportsProjectionPushDown} so only the needed columns are returned from TuGraph.
 */
public class TuGraphDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private final TuGraphSinkOptions options;
    private final CypherQueryBuilder queryBuilder;
    private final String label;
    private final String primaryKey;
    private final int fetchSize;
    private final LookupCache cache;

    private final String[] allFieldNames;
    private final LogicalType[] allFieldTypes;
    private final DataType fullDataType;

    /** Projection: produced column i maps to {@code allFieldNames[projectedFields[i]]}. */
    private int[] projectedFields;
    private DataType producedDataType;

    public TuGraphDynamicTableSource(TuGraphSinkOptions options, String label, String primaryKey,
                                     int fetchSize, LookupCache cache,
                                     String[] allFieldNames, LogicalType[] allFieldTypes,
                                     DataType fullDataType) {
        this.options = Objects.requireNonNull(options, "options");
        this.queryBuilder = new CypherQueryBuilder();
        this.label = Objects.requireNonNull(label, "label");
        this.primaryKey = primaryKey;
        this.fetchSize = fetchSize;
        this.cache = cache;
        this.allFieldNames = allFieldNames;
        this.allFieldTypes = allFieldTypes;
        this.fullDataType = fullDataType;
        this.producedDataType = fullDataType;
        this.projectedFields = identity(allFieldNames.length);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        String[] names = producedNames();
        LogicalType[] types = producedTypes();
        TypeInformation<RowData> producedType = runtimeProviderContext.createTypeInformation(producedDataType);
        return InputFormatProvider.of(new TuGraphRowDataInputFormat(
                options, queryBuilder, label, names, types, primaryKey, fetchSize, producedType));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] names = producedNames();
        LogicalType[] types = producedTypes();

        int[][] keys = context.getKeys();
        String[] keyNames = new String[keys.length];
        LogicalType[] keyTypes = new LogicalType[keys.length];
        for (int i = 0; i < keys.length; i++) {
            int keyIndex = keys[i][0]; // nested keys are not supported
            keyNames[i] = names[keyIndex];
            keyTypes[i] = types[keyIndex];
        }

        TuGraphRowDataLookupFunction function = new TuGraphRowDataLookupFunction(
                options, queryBuilder, label, keyNames, keyTypes, names, types);
        return cache != null
                ? PartialCachingLookupProvider.of(function, cache)
                : LookupFunctionProvider.of(function);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        int[] indexes = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            indexes[i] = projectedFields[i][0];
        }
        this.projectedFields = indexes;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        TuGraphDynamicTableSource copy = new TuGraphDynamicTableSource(
                options, label, primaryKey, fetchSize, cache, allFieldNames, allFieldTypes, fullDataType);
        copy.projectedFields = this.projectedFields.clone();
        copy.producedDataType = this.producedDataType;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "TuGraph[" + options.uri() + ", graph=" + options.graph() + ", label=" + label + "]";
    }

    private String[] producedNames() {
        String[] names = new String[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            names[i] = allFieldNames[projectedFields[i]];
        }
        return names;
    }

    private LogicalType[] producedTypes() {
        LogicalType[] types = new LogicalType[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            types[i] = allFieldTypes[projectedFields[i]];
        }
        return types;
    }

    private static int[] identity(int n) {
        int[] a = new int[n];
        for (int i = 0; i < n; i++) {
            a[i] = i;
        }
        return a;
    }
}
