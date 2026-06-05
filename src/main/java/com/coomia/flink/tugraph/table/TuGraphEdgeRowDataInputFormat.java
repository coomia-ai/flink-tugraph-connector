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
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Bounded {@link InputFormat} that scans edges of a label
 * ({@code MATCH (a:Src)-[e:E]->(b:Dst) RETURN ... ORDER BY a.srcKey SKIP/LIMIT}), emitting
 * {@link RowData}. Single split (parallelism 1).
 */
public class TuGraphEdgeRowDataInputFormat implements InputFormat<RowData, GenericInputSplit>,
        ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final TuGraphSinkOptions options;
    private final CypherQueryBuilder queryBuilder;
    private final String edgeLabel;
    private final String srcLabel;
    private final String srcKey;
    private final String srcCol;
    private final String dstLabel;
    private final String dstKey;
    private final String dstCol;
    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;
    private final int fetchSize;
    private final TypeInformation<RowData> producedType;

    private transient TuGraphConnection connection;
    private transient RowToRowDataConverter rowConverter;
    private transient List<Map<String, Object>> page;
    private transient int pageIdx;
    private transient long skip;
    private transient boolean finished;

    public TuGraphEdgeRowDataInputFormat(TuGraphSinkOptions options, CypherQueryBuilder queryBuilder,
                                         String edgeLabel, String srcLabel, String srcKey, String srcCol,
                                         String dstLabel, String dstKey, String dstCol,
                                         String[] fieldNames, LogicalType[] fieldTypes, int fetchSize,
                                         TypeInformation<RowData> producedType) {
        this.options = options;
        this.queryBuilder = queryBuilder;
        this.edgeLabel = edgeLabel;
        this.srcLabel = srcLabel;
        this.srcKey = srcKey;
        this.srcCol = srcCol;
        this.dstLabel = dstLabel;
        this.dstKey = dstKey;
        this.dstCol = dstCol;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.fetchSize = fetchSize;
        this.producedType = producedType;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public GenericInputSplit[] createInputSplits(int minNumSplits) {
        return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(GenericInputSplit split) {
        this.connection = new TuGraphConnection(options);
        this.connection.open();
        this.rowConverter = new RowToRowDataConverter(fieldNames, fieldTypes);
        this.skip = 0;
        this.finished = false;
        loadNextPage();
    }

    private void loadNextPage() {
        CypherStatement stmt = queryBuilder.buildEdgeScan(
                edgeLabel, srcLabel, srcKey, srcCol, dstLabel, dstKey, dstCol,
                Arrays.asList(fieldNames), skip, fetchSize);
        page = connection.read(stmt);
        pageIdx = 0;
        skip += fetchSize;
        if (page.size() < fetchSize) {
            finished = true;
        }
    }

    @Override
    public boolean reachedEnd() {
        while (page != null && pageIdx >= page.size()) {
            if (finished) {
                return true;
            }
            loadNextPage();
        }
        return page == null || page.isEmpty();
    }

    @Override
    public RowData nextRecord(RowData reuse) {
        return rowConverter.convert(page.get(pageIdx++));
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        page = Collections.emptyList();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
