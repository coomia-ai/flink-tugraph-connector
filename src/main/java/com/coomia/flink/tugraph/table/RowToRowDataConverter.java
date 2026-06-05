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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.Map;

/**
 * Converts a TuGraph result row (a {@code Map} of alias to Bolt value, as returned by
 * {@code TuGraphConnection.read}) into a Flink {@link RowData} for the produced (projected) schema.
 */
public class RowToRowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;

    public RowToRowDataConverter(String[] fieldNames, LogicalType[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    /** @return an INSERT {@link RowData} with one field per produced column. */
    public RowData convert(Map<String, Object> row) {
        GenericRowData out = new GenericRowData(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            out.setField(i, RowDataConversions.toInternal(row.get(fieldNames[i]), fieldTypes[i]));
        }
        return out;
    }
}
