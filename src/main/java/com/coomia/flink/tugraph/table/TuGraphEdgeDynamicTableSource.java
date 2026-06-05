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
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Objects;

/**
 * {@link ScanTableSource} for {@code 'connector' = 'tugraph'} with {@code element.type = edge}: a
 * bounded scan of edges ({@code MATCH (a:Src)-[e:E]->(b:Dst)}) with projection push-down. Source/
 * destination key columns map to the endpoint vertices; the rest map to the edge.
 */
public class TuGraphEdgeDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown {

    private final TuGraphSinkOptions options;
    private final CypherQueryBuilder queryBuilder;
    private final String edgeLabel;
    private final String srcLabel;
    private final String srcKey;
    private final String srcCol;
    private final String dstLabel;
    private final String dstKey;
    private final String dstCol;
    private final int fetchSize;

    private final String[] allFieldNames;
    private final LogicalType[] allFieldTypes;
    private final DataType fullDataType;

    private int[] projectedFields;
    private DataType producedDataType;

    public TuGraphEdgeDynamicTableSource(TuGraphSinkOptions options,
                                         String edgeLabel,
                                         String srcLabel, String srcKey, String srcCol,
                                         String dstLabel, String dstKey, String dstCol,
                                         int fetchSize,
                                         String[] allFieldNames, LogicalType[] allFieldTypes,
                                         DataType fullDataType) {
        this.options = Objects.requireNonNull(options, "options");
        this.queryBuilder = new CypherQueryBuilder();
        this.edgeLabel = edgeLabel;
        this.srcLabel = srcLabel;
        this.srcKey = srcKey;
        this.srcCol = srcCol;
        this.dstLabel = dstLabel;
        this.dstKey = dstKey;
        this.dstCol = dstCol;
        this.fetchSize = fetchSize;
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
        return InputFormatProvider.of(new TuGraphEdgeRowDataInputFormat(
                options, queryBuilder, edgeLabel, srcLabel, srcKey, srcCol, dstLabel, dstKey, dstCol,
                names, types, fetchSize, producedType));
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
        TuGraphEdgeDynamicTableSource copy = new TuGraphEdgeDynamicTableSource(
                options, edgeLabel, srcLabel, srcKey, srcCol, dstLabel, dstKey, dstCol, fetchSize,
                allFieldNames, allFieldTypes, fullDataType);
        copy.projectedFields = this.projectedFields.clone();
        copy.producedDataType = this.producedDataType;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "TuGraph[" + options.uri() + ", graph=" + options.graph() + ", edge=" + edgeLabel + "]";
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
