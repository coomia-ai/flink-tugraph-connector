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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Conversions between Flink {@code RowData} internal values and the plain Java values the Bolt
 * driver exchanges (String / Long / Double / Boolean). Mirrors the type mapping in the requirements
 * (§7.2) in both directions.
 */
final class RowDataConversions {

    private RowDataConversions() {
    }

    /** Extract column {@code idx} from a {@link RowData} as a Bolt-friendly Java value (or null). */
    static Object toJava(RowData row, int idx, LogicalType type) {
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
                return LocalDate.ofEpochDay(row.getInt(idx)).toString();
            case TIME_WITHOUT_TIME_ZONE:
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
                throw new UnsupportedOperationException("Unsupported lookup-key type: " + type);
        }
    }

    /** Convert a Bolt value into the internal {@code RowData} representation for {@code type}. */
    static Object toInternal(Object bolt, LogicalType type) {
        if (bolt == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(bolt.toString());
            case BOOLEAN:
                return (Boolean) bolt;
            case TINYINT:
                return ((Number) bolt).byteValue();
            case SMALLINT:
                return ((Number) bolt).shortValue();
            case INTEGER:
                return ((Number) bolt).intValue();
            case BIGINT:
                return ((Number) bolt).longValue();
            case FLOAT:
                return ((Number) bolt).floatValue();
            case DOUBLE:
                return ((Number) bolt).doubleValue();
            case DECIMAL: {
                DecimalType dt = (DecimalType) type;
                BigDecimal bd = bolt instanceof BigDecimal ? (BigDecimal) bolt : new BigDecimal(bolt.toString());
                return DecimalData.fromBigDecimal(bd, dt.getPrecision(), dt.getScale());
            }
            case DATE:
                return (int) LocalDate.parse(bolt.toString()).toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return (int) (LocalTime.parse(bolt.toString()).toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromLocalDateTime(LocalDateTime.parse(bolt.toString()));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromInstant(Instant.parse(bolt.toString()));
            default:
                throw new UnsupportedOperationException(
                        "TuGraph source (v0.2) does not support reading column type " + type);
        }
    }
}
