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
import java.util.Collections;
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
 *   <li><b>No back-quoted identifiers</b> — identifiers are emitted plain and validated.</li>
 *   <li><b>No {@code SET n += $map}</b> — properties are set individually.</li>
 *   <li><b>No {@code UNWIND}-batched writes</b> — each element is one parameterized statement.</li>
 *   <li><b>Primary key is never in SET</b> — the {@code MERGE} pattern sets it.</li>
 * </ul>
 *
 * <h2>Edge options</h2>
 * <ul>
 *   <li><b>{@code edgeMergeKeys}</b> — property columns folded into the edge MERGE match (e.g.
 *       {@code rel_type}). With a single edge label discriminated by a property, this keeps multiple
 *       relation types between the same vertex pair as distinct edges instead of collapsing them
 *       (last-write-wins). Verified on TuGraph: {@code MERGE (a)-[e:REL {rel_type:$v}]->(b)} keeps
 *       them separate and stays idempotent.</li>
 *   <li><b>{@code mergeEndpoints}</b> — when an endpoint may be missing
 *       ({@code edge.on-missing-endpoint = create}), the endpoints are {@code MERGE}d (bare vertex,
 *       key only) instead of {@code MATCH}ed, so an out-of-order at-least-once pipeline becomes
 *       eventually consistent.</li>
 * </ul>
 */
public class MergeCypherStatementBuilder implements CypherStatementBuilder {

    private static final long serialVersionUID = 4L;

    static final String PK_PARAM = "pk";
    static final String SRC_PARAM = "_src";
    static final String DST_PARAM = "_dst";
    static final String PROP_PREFIX = "p_";
    static final String MERGE_KEY_PREFIX = "mk_";

    /** TuGraph identifiers: a letter or underscore followed by letters, digits or underscores. */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final List<String> edgeMergeKeys;
    private final boolean mergeEndpoints;

    public MergeCypherStatementBuilder() {
        this(Collections.emptyList(), false);
    }

    /**
     * @param edgeMergeKeys edge property columns folded into the edge MERGE match key
     * @param mergeEndpoints {@code true} to MERGE (create-if-missing) edge endpoints instead of MATCH
     */
    public MergeCypherStatementBuilder(List<String> edgeMergeKeys, boolean mergeEndpoints) {
        this.edgeMergeKeys = edgeMergeKeys == null ? Collections.emptyList() : edgeMergeKeys;
        this.mergeEndpoints = mergeEndpoints;
    }

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

            StringBuilder cypher = new StringBuilder();
            if (mergeEndpoints) {
                // Create the endpoints if missing (eventually-consistent, out-of-order pipelines).
                cypher.append("MERGE (a:").append(sl).append(" {").append(sk).append(": $").append(SRC_PARAM).append("})\n")
                        .append("MERGE (b:").append(dl).append(" {").append(dk).append(": $").append(DST_PARAM).append("})\n");
            } else {
                cypher.append("MATCH (a:").append(sl).append(" {").append(sk).append(": $").append(SRC_PARAM).append("}), ")
                        .append("(b:").append(dl).append(" {").append(dk).append(": $").append(DST_PARAM).append("})\n");
            }
            cypher.append("MERGE (a)-[e:").append(e);
            appendMergeKeyPattern(cypher, edge, params);
            cypher.append("]->(b)");
            appendSet(cypher, "e", edgeSetKeys(edge), edge.properties(), params);
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

        List<CypherStatement> statements = new ArrayList<>(batch.size());
        for (Edge edge : batch) {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put(SRC_PARAM, edge.srcValue());
            params.put(DST_PARAM, edge.dstValue());

            StringBuilder cypher = new StringBuilder()
                    .append("MATCH (a:").append(sl).append(" {").append(sk).append(": $").append(SRC_PARAM).append("})-[e:").append(e);
            appendMergeKeyPattern(cypher, edge, params); // narrow deletion to the specific edge
            cypher.append("]->(b:").append(dl).append(" {").append(dk).append(": $").append(DST_PARAM).append("}) DELETE e");

            statements.add(new CypherStatement(cypher.toString(), params));
        }
        return statements;
    }

    /** Append a {@code {k: $mk_k, ...}} relationship property pattern for the merge keys. */
    private void appendMergeKeyPattern(StringBuilder cypher, Edge edge, Map<String, Object> params) {
        if (edgeMergeKeys.isEmpty()) {
            return;
        }
        cypher.append(" {");
        for (int i = 0; i < edgeMergeKeys.size(); i++) {
            String k = identifier(edgeMergeKeys.get(i), "edge merge key");
            String param = MERGE_KEY_PREFIX + k;
            if (i > 0) {
                cypher.append(", ");
            }
            cypher.append(k).append(": $").append(param);
            params.put(param, edge.properties().get(k));
        }
        cypher.append("}");
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

    /** Sorted edge property keys to SET: non-null and not part of the MERGE match key. */
    private List<String> edgeSetKeys(Edge edge) {
        TreeSet<String> keys = new TreeSet<>();
        for (Map.Entry<String, Object> en : edge.properties().entrySet()) {
            if (en.getValue() != null && !edgeMergeKeys.contains(en.getKey())) {
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
