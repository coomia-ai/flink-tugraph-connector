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

import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.GraphElement;
import com.coomia.flink.tugraph.element.Vertex;
import com.coomia.flink.tugraph.sink.ElementConverter;
import com.coomia.flink.tugraph.table.TuGraphConnectorOptions.ElementType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts a Flink {@link RowData} into a {@link Vertex} or {@link Edge} according to the table
 * connector configuration, applying the type mapping in the requirements document (§7.2).
 *
 * <p>Mapping summary: strings stay strings; all integer types widen to {@code Long}; all floating /
 * decimal types widen to {@code Double}; booleans stay booleans; date/time/timestamp render to
 * ISO-8601 strings; {@code null} values are dropped (never written so they cannot overwrite). Nested
 * types (ARRAY/MAP/ROW) are out of scope for v0.1 and rejected.
 */
public class RowDataToElementConverter implements ElementConverter<RowData> {

    private static final long serialVersionUID = 1L;

    private final ElementType elementType;
    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;

    // Vertex configuration.
    private final String vertexLabel;
    private final int primaryKeyIndex;

    // Edge configuration.
    private final String edgeLabel;
    private final String srcLabel;
    private final String srcKey;
    private final int srcIndex;
    private final String dstLabel;
    private final String dstKey;
    private final int dstIndex;

    private RowDataToElementConverter(Builder b) {
        this.elementType = b.elementType;
        this.fieldNames = b.fieldNames;
        this.fieldTypes = b.fieldTypes;
        this.vertexLabel = b.vertexLabel;
        this.primaryKeyIndex = b.primaryKeyIndex;
        this.edgeLabel = b.edgeLabel;
        this.srcLabel = b.srcLabel;
        this.srcKey = b.srcKey;
        this.srcIndex = b.srcIndex;
        this.dstLabel = b.dstLabel;
        this.dstKey = b.dstKey;
        this.dstIndex = b.dstIndex;
    }

    @Override
    public GraphElement convert(RowData row) {
        return elementType == ElementType.VERTEX ? toVertex(row) : toEdge(row);
    }

    private Vertex toVertex(RowData row) {
        Map<String, Object> props = new LinkedHashMap<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Object value = extract(row, i, fieldTypes[i]);
            if (value != null) {
                props.put(fieldNames[i], value);
            }
        }
        Object pkValue = extract(row, primaryKeyIndex, fieldTypes[primaryKeyIndex]);
        return new Vertex(vertexLabel, fieldNames[primaryKeyIndex], pkValue, props);
    }

    private Edge toEdge(RowData row) {
        Map<String, Object> props = new LinkedHashMap<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            // Endpoint key columns identify the vertices; they are not edge properties.
            if (i == srcIndex || i == dstIndex) {
                continue;
            }
            Object value = extract(row, i, fieldTypes[i]);
            if (value != null) {
                props.put(fieldNames[i], value);
            }
        }
        Object srcValue = extract(row, srcIndex, fieldTypes[srcIndex]);
        Object dstValue = extract(row, dstIndex, fieldTypes[dstIndex]);
        return new Edge(edgeLabel, srcLabel, srcKey, srcValue, dstLabel, dstKey, dstValue, props);
    }

    /** Extract column {@code idx} as a Bolt-friendly Java value (or {@code null}). */
    private static Object extract(RowData row, int idx, LogicalType type) {
        if (row.isNullAt(idx)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return row.getString(idx).toString();
            case BOOLEAN:
                return row.getBoolean(idx);
            case TINYINT:
                return (long) row.getByte(idx);
            case SMALLINT:
                return (long) row.getShort(idx);
            case INTEGER:
                return (long) row.getInt(idx);
            case BIGINT:
                return row.getLong(idx);
            case FLOAT:
                return (double) row.getFloat(idx);
            case DOUBLE:
                return row.getDouble(idx);
            case DECIMAL: {
                DecimalType dt = (DecimalType) type;
                return row.getDecimal(idx, dt.getPrecision(), dt.getScale()).toBigDecimal().doubleValue();
            }
            case DATE:
                // Stored as epoch-day int.
                return LocalDate.ofEpochDay(row.getInt(idx)).toString();
            case TIME_WITHOUT_TIME_ZONE:
                // Stored as millis-of-day int.
                return LocalTime.ofNanoOfDay((long) row.getInt(idx) * 1_000_000L).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE: {
                TimestampType tt = (TimestampType) type;
                return row.getTimestamp(idx, tt.getPrecision()).toLocalDateTime().toString();
            }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                LocalZonedTimestampType lt = (LocalZonedTimestampType) type;
                return row.getTimestamp(idx, lt.getPrecision()).toInstant().toString();
            }
            default:
                throw new UnsupportedOperationException(
                        "TuGraph sink (v0.1) does not support column type " + type
                                + "; nested types (ARRAY/MAP/ROW) are planned for v0.2");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder assembled by {@link TuGraphDynamicTableSinkFactory} from the resolved schema. */
    public static final class Builder {
        private ElementType elementType;
        private String[] fieldNames;
        private LogicalType[] fieldTypes;
        private String vertexLabel;
        private int primaryKeyIndex = -1;
        private String edgeLabel;
        private String srcLabel;
        private String srcKey;
        private int srcIndex = -1;
        private String dstLabel;
        private String dstKey;
        private int dstIndex = -1;

        public Builder elementType(ElementType elementType) {
            this.elementType = elementType;
            return this;
        }

        public Builder schema(String[] fieldNames, LogicalType[] fieldTypes) {
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder vertex(String label, int primaryKeyIndex) {
            this.vertexLabel = label;
            this.primaryKeyIndex = primaryKeyIndex;
            return this;
        }

        public Builder edge(String edgeLabel,
                            String srcLabel, String srcKey, int srcIndex,
                            String dstLabel, String dstKey, int dstIndex) {
            this.edgeLabel = edgeLabel;
            this.srcLabel = srcLabel;
            this.srcKey = srcKey;
            this.srcIndex = srcIndex;
            this.dstLabel = dstLabel;
            this.dstKey = dstKey;
            this.dstIndex = dstIndex;
            return this;
        }

        public RowDataToElementConverter build() {
            return new RowDataToElementConverter(this);
        }
    }
}
