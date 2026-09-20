/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package com.coomia.flink.tugraph.cypher;

import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.Vertex;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Builds TuGraph 4.5.2 native bulk-upsert procedure calls. */
public final class NativeBulkStatementBuilder {

    public static final String ROWS_PARAM = "__onto_rows";
    public static final String SRC_ROW_FIELD = "_src";
    public static final String DST_ROW_FIELD = "_dst";
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private NativeBulkStatementBuilder() {
    }

    /** Build {@code db.upsertVertex(label, rows)} while preserving MERGE null-field semantics. */
    public static CypherStatement vertex(String label, String primaryKey, List<Vertex> vertices) {
        identifier(label, "vertex label");
        identifier(primaryKey, "primary key");
        requireNonEmpty(vertices, "vertex batch");
        List<Map<String, Object>> rows = new ArrayList<>(vertices.size());
        for (Vertex vertex : vertices) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : vertex.properties().entrySet()) {
                identifier(entry.getKey(), "vertex property");
                if (entry.getValue() != null && !entry.getKey().equals(primaryKey)) {
                    row.put(entry.getKey(), entry.getValue());
                }
            }
            row.put(primaryKey, vertex.primaryKeyValue());
            rows.add(row);
        }
        return new CypherStatement(
                "CALL db.upsertVertex('" + label + "', $" + ROWS_PARAM + ")",
                Map.of(ROWS_PARAM, rows));
    }

    /** Build {@code db.upsertEdge(label, src, dst, rows)} for endpoint-complete batches. */
    public static CypherStatement edge(String edgeLabel, String srcLabel, String srcKey,
                                       String dstLabel, String dstKey, List<Edge> edges) {
        identifier(edgeLabel, "edge label");
        identifier(srcLabel, "source label");
        identifier(srcKey, "source key");
        identifier(dstLabel, "destination label");
        identifier(dstKey, "destination key");
        requireNonEmpty(edges, "edge batch");
        List<Map<String, Object>> rows = new ArrayList<>(edges.size());
        for (Edge edge : edges) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : edge.properties().entrySet()) {
                identifier(entry.getKey(), "edge property");
                if (entry.getValue() != null) {
                    if (SRC_ROW_FIELD.equals(entry.getKey()) || DST_ROW_FIELD.equals(entry.getKey())) {
                        throw new IllegalArgumentException("edge property conflicts with endpoint field: "
                                + entry.getKey());
                    }
                    row.put(entry.getKey(), entry.getValue());
                }
            }
            row.put(SRC_ROW_FIELD, edge.srcValue());
            row.put(DST_ROW_FIELD, edge.dstValue());
            rows.add(row);
        }
        String cypher = "CALL db.upsertEdge('" + edgeLabel + "', {type:'" + srcLabel
                + "', key:'" + SRC_ROW_FIELD + "'}, {type:'" + dstLabel
                + "', key:'" + DST_ROW_FIELD + "'}, $" + ROWS_PARAM + ")";
        return new CypherStatement(cypher, Map.of(ROWS_PARAM, rows));
    }

    private static void requireNonEmpty(List<?> values, String name) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }

    private static void identifier(String value, String role) {
        if (value == null || !IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(role + " '" + value
                    + "' is not a valid TuGraph identifier");
        }
    }
}
