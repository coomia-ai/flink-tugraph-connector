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

package com.coomia.flink.tugraph.cypher;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Generates the read queries used by the source / lookup paths, tuned for TuGraph's openCypher
 * subset (verified live: plain identifiers, {@code RETURN} projection with aliases, {@code WHERE},
 * {@code ORDER BY ... SKIP ... LIMIT}, point match on {@code {key: $param}}).
 *
 * <h2>Generated templates</h2>
 * <pre>
 * -- lookup (point match by key) --
 * MATCH (n:Company {company_id: $_k0})
 * RETURN n.company_id AS company_id, n.name AS name
 *
 * -- scan (paginated full scan) --
 * MATCH (n:Company)
 * RETURN n.company_id AS company_id, n.name AS name
 * ORDER BY n.company_id SKIP 0 LIMIT 1000
 * </pre>
 */
public class CypherQueryBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /**
     * Build a point lookup matching a vertex by one or more key columns.
     *
     * @param label         vertex label
     * @param keyColumns    key property names (match keys)
     * @param keyValues     key values (parameterized; same order as {@code keyColumns})
     * @param returnColumns columns to project
     */
    public CypherStatement buildVertexLookup(String label, List<String> keyColumns,
                                             List<Object> keyValues, List<String> returnColumns) {
        String l = identifier(label, "vertex label");
        StringBuilder cypher = new StringBuilder("MATCH (n:").append(l).append(" {");
        Map<String, Object> params = new LinkedHashMap<>();
        for (int i = 0; i < keyColumns.size(); i++) {
            String k = identifier(keyColumns.get(i), "lookup key");
            String param = "_k" + i;
            if (i > 0) {
                cypher.append(", ");
            }
            cypher.append(k).append(": $").append(param);
            params.put(param, keyValues.get(i));
        }
        cypher.append("})\nRETURN ").append(returnClause(returnColumns));
        return new CypherStatement(cypher.toString(), params);
    }

    /**
     * Build a paginated full vertex scan. {@code ORDER BY} the (stable) order column makes paging
     * deterministic.
     *
     * @param label         vertex label
     * @param returnColumns columns to project
     * @param orderByColumn column to order by (typically the primary key); may be {@code null}
     * @param skip          rows to skip ({@code <= 0} omits SKIP)
     * @param limit         max rows to return ({@code < 0} omits LIMIT)
     */
    public CypherStatement buildVertexScan(String label, List<String> returnColumns,
                                           String orderByColumn, long skip, long limit) {
        String l = identifier(label, "vertex label");
        StringBuilder cypher = new StringBuilder("MATCH (n:").append(l).append(")\nRETURN ")
                .append(returnClause(returnColumns));
        if (orderByColumn != null) {
            cypher.append("\nORDER BY n.").append(identifier(orderByColumn, "order column"));
        }
        if (skip > 0) {
            cypher.append("\nSKIP ").append(skip);
        }
        if (limit >= 0) {
            cypher.append("\nLIMIT ").append(limit);
        }
        return new CypherStatement(cypher.toString(), Map.of());
    }

    private String returnClause(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("return columns must not be empty");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            String c = identifier(columns.get(i), "return column");
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("n.").append(c).append(" AS ").append(c);
        }
        return sb.toString();
    }

    private String identifier(String identifier, String role) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException(role + " must not be null or empty");
        }
        if (!IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(role + " '" + identifier
                    + "' is not a valid TuGraph identifier (expected [A-Za-z_][A-Za-z0-9_]*)");
        }
        return identifier;
    }
}
