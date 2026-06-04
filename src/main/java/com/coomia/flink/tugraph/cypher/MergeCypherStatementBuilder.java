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

import com.coomia.flink.tugraph.element.Edge;
import com.coomia.flink.tugraph.element.Vertex;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Default {@link CypherStatementBuilder}, tuned for TuGraph-DB's openCypher subset (verified against
 * TuGraph 4.x over Bolt). It deliberately avoids the Neo4j idioms TuGraph does not accept:
 *
 * <ul>
 *   <li><b>No back-quoted identifiers</b> — TuGraph treats {@code `Label`} as a literal name, so
 *       identifiers are emitted plain and validated instead of quoted.</li>
 *   <li><b>No {@code SET n += $map}</b> — properties are set individually ({@code SET n.k = $p_k}).</li>
 *   <li><b>No {@code UNWIND}-batched writes</b> — TuGraph mis-binds per-row values in
 *       {@code UNWIND ... SET n.k = row.k}, so each element is written as a single parameterized
 *       statement. The statements of one flush group run together (one Bolt session).</li>
 *   <li><b>Primary key is never in SET</b> — the {@code MERGE} pattern sets it; setting it again
 *       hits the unique index.</li>
 * </ul>
 *
 * <h2>Generated templates</h2>
 * <pre>
 * -- one statement per vertex --
 * MERGE (n:Company {company_id: $pk})
 * SET n.name = $p_name, n.reg_capital = $p_reg_capital
 *
 * -- one statement per edge --
 * MATCH (a:Company {company_id: $_src}), (b:Company {company_id: $_dst})
 * MERGE (a)-[e:INVEST]-&gt;(b)
 * SET e.ratio = $p_ratio
 * RETURN count(e) AS written
 * </pre>
 *
 * <p>{@code null} properties are dropped. Note TuGraph is not schemaless: the target vertex/edge
 * labels (with their primary key and properties) must already exist before the job runs.
 */
public class MergeCypherStatementBuilder implements CypherStatementBuilder {

    private static final long serialVersionUID = 3L;

    static final String PK_PARAM = "pk";
    static final String SRC_PARAM = "_src";
    static final String DST_PARAM = "_dst";
    static final String PROP_PREFIX = "p_";

    /** TuGraph identifiers: a letter or underscore followed by letters, digits or underscores. */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    @Override
    public List<CypherStatement> buildVertexUpsert(String label, String primaryKey, List<Vertex> batch) {
        requireNonEmpty(batch, "vertex batch");
        String l = identifier(label, "vertex label");
        String pk = identifier(primaryKey, "primary key");

        List<CypherStatement> statements = new ArrayList<>(batch.size());
        for (Vertex v : batch) {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put(PK_PARAM, v.primaryKeyValue());

            StringBuilder cypher = new StringBuilder()
                    .append("MERGE (n:").append(l).append(" {").append(pk).append(": $").append(PK_PARAM).append("})");
            appendSet(cypher, "n", nonPkKeys(v, primaryKey), v.properties(), params);

            statements.add(new CypherStatement(cypher.toString(), params));
        }
        return statements;
    }

    @Override
    public List<CypherStatement> buildEdgeUpsert(String edgeLabel,
                                                 String srcLabel, String srcKey,
                                                 String dstLabel, String dstKey,
                                                 List<Edge> batch) {
        requireNonEmpty(batch, "edge batch");
        String e = identifier(edgeLabel, "edge label");
        String sl = identifier(srcLabel, "edge source label");
        String sk = identifier(srcKey, "edge source key");
        String dl = identifier(dstLabel, "edge destination label");
        String dk = identifier(dstKey, "edge destination key");

        List<CypherStatement> statements = new ArrayList<>(batch.size());
        for (Edge edge : batch) {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put(SRC_PARAM, edge.srcValue());
            params.put(DST_PARAM, edge.dstValue());

            StringBuilder cypher = new StringBuilder()
                    .append("MATCH (a:").append(sl).append(" {").append(sk).append(": $").append(SRC_PARAM).append("}), ")
                    .append("(b:").append(dl).append(" {").append(dk).append(": $").append(DST_PARAM).append("})\n")
                    .append("MERGE (a)-[e:").append(e).append("]->(b)");
            appendSet(cypher, "e", nonNullKeys(edge.properties()), edge.properties(), params);
            cypher.append("\nRETURN count(e) AS ").append(WRITTEN_COUNT_FIELD);

            statements.add(new CypherStatement(cypher.toString(), params));
        }
        return statements;
    }

    @Override
    public List<CypherStatement> buildVertexDelete(String label, String primaryKey, List<Vertex> batch) {
        requireNonEmpty(batch, "vertex delete batch");
        String l = identifier(label, "vertex label");
        String pk = identifier(primaryKey, "primary key");
        // DETACH so any attached edges are removed with the vertex; a missing key is a no-op.
        String cypher = "MATCH (n:" + l + " {" + pk + ": $" + PK_PARAM + "}) DETACH DELETE n";

        List<CypherStatement> statements = new ArrayList<>(batch.size());
        for (Vertex v : batch) {
            Map<String, Object> params = new LinkedHashMap<>(1);
            params.put(PK_PARAM, v.primaryKeyValue());
            statements.add(new CypherStatement(cypher, params));
        }
        return statements;
    }

    @Override
    public List<CypherStatement> buildEdgeDelete(String edgeLabel,
                                                 String srcLabel, String srcKey,
                                                 String dstLabel, String dstKey,
                                                 List<Edge> batch) {
        requireNonEmpty(batch, "edge delete batch");
        String e = identifier(edgeLabel, "edge label");
        String sl = identifier(srcLabel, "edge source label");
        String sk = identifier(srcKey, "edge source key");
        String dl = identifier(dstLabel, "edge destination label");
        String dk = identifier(dstKey, "edge destination key");
        String cypher = "MATCH (a:" + sl + " {" + sk + ": $" + SRC_PARAM + "})-[e:" + e + "]->"
                + "(b:" + dl + " {" + dk + ": $" + DST_PARAM + "}) DELETE e";

        List<CypherStatement> statements = new ArrayList<>(batch.size());
        for (Edge edge : batch) {
            Map<String, Object> params = new LinkedHashMap<>(2);
            params.put(SRC_PARAM, edge.srcValue());
            params.put(DST_PARAM, edge.dstValue());
            statements.add(new CypherStatement(cypher, params));
        }
        return statements;
    }
    /** Append a {@code SET alias.key = $p_key, ...} clause for the given keys and bind their params. */
    private void appendSet(StringBuilder cypher, String alias, List<String> keys,
                           Map<String, Object> properties, Map<String, Object> params) {
        boolean first = true;
        for (String key : keys) {
            String k = identifier(key, alias.equals("e") ? "edge property" : "vertex property");
            String param = PROP_PREFIX + k;
            cypher.append(first ? "\nSET " : ", ");
            cypher.append(alias).append('.').append(k).append(" = $").append(param);
            params.put(param, properties.get(key));
            first = false;
        }
    }

    /** Sorted property keys excluding the primary key and {@code null} values. */
    private static List<String> nonPkKeys(Vertex v, String primaryKey) {
        TreeSet<String> keys = new TreeSet<>();
        for (Map.Entry<String, Object> en : v.properties().entrySet()) {
            if (!en.getKey().equals(primaryKey) && en.getValue() != null) {
                keys.add(en.getKey());
            }
        }
        return new ArrayList<>(keys);
    }

    /** Sorted property keys excluding {@code null} values. */
    private static List<String> nonNullKeys(Map<String, Object> properties) {
        TreeSet<String> keys = new TreeSet<>();
        for (Map.Entry<String, Object> en : properties.entrySet()) {
            if (en.getValue() != null) {
                keys.add(en.getKey());
            }
        }
        return new ArrayList<>(keys);
    }

    /**
     * Validate that an identifier is a plain TuGraph identifier and return it unquoted. TuGraph does
     * not support back-quoted identifiers, so anything outside {@code [A-Za-z_][A-Za-z0-9_]*} is
     * rejected rather than silently mis-quoted.
     */
    protected String identifier(String identifier, String role) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException(role + " must not be null or empty");
        }
        if (!IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(role + " '" + identifier
                    + "' is not a valid TuGraph identifier (expected [A-Za-z_][A-Za-z0-9_]*)");
        }
        return identifier;
    }

    private static void requireNonEmpty(List<?> batch, String name) {
        if (batch == null || batch.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }
}
