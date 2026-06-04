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

/**
 * Default {@link CypherStatementBuilder} using batched {@code UNWIND ... MERGE ... SET x += $map}.
 *
 * <p>One statement per batch upserts every element idempotently, which is what makes the
 * at-least-once sink safe to replay (a re-sent element MERGEs onto the existing node/edge and only
 * refreshes properties — no duplicates).
 *
 * <h2>Generated templates</h2>
 * <pre>
 * -- vertices --
 * UNWIND $batch AS row
 * MERGE (n:`Company` {`company_id`: row.id})
 * SET n += row.props
 *
 * -- edges --
 * UNWIND $batch AS row
 * MATCH (a:`Company` {`company_id`: row.src}), (b:`Company` {`company_id`: row.dst})
 * MERGE (a)-[e:`INVEST`]-&gt;(b)
 * SET e += row.props
 * RETURN count(e) AS written
 * </pre>
 *
 * <p>Each {@code $batch} entry is a {@code Map}: vertices carry {@code {id, props}}; edges carry
 * {@code {src, dst, props}}. {@code null} properties are dropped so they never overwrite existing
 * values. For edges, rows whose endpoints do not both match produce no edge and are counted as
 * skipped via the returned {@code written} total.
 *
 * <p>If a target TuGraph build does not support {@code SET x += $map}, swap in an alternative
 * builder that expands properties individually; the sink and connection layers are unaffected.
 */
public class MergeCypherStatementBuilder implements CypherStatementBuilder {

    private static final long serialVersionUID = 1L;

    static final String BATCH_PARAM = "batch";
    static final String ROW_ID = "id";
    static final String ROW_SRC = "src";
    static final String ROW_DST = "dst";
    static final String ROW_PROPS = "props";

    @Override
    public CypherStatement buildVertexUpsert(String label, String primaryKey, List<Vertex> batch) {
        requireNonEmpty(batch, "vertex batch");
        String l = sanitizeIdentifier(label, "vertex label");
        String pk = sanitizeIdentifier(primaryKey, "primary key");

        String cypher = "UNWIND $" + BATCH_PARAM + " AS row\n"
                + "MERGE (n:" + l + " {" + pk + ": row." + ROW_ID + "})\n"
                + "SET n += row." + ROW_PROPS;

        List<Map<String, Object>> rows = new ArrayList<>(batch.size());
        for (Vertex v : batch) {
            Map<String, Object> row = new LinkedHashMap<>(2);
            row.put(ROW_ID, v.primaryKeyValue());
            row.put(ROW_PROPS, nonNullProperties(v.properties()));
            rows.add(row);
        }
        return new CypherStatement(cypher, singletonBatch(rows));
    }

    @Override
    public CypherStatement buildEdgeUpsert(String edgeLabel,
                                           String srcLabel, String srcKey,
                                           String dstLabel, String dstKey,
                                           List<Edge> batch) {
        requireNonEmpty(batch, "edge batch");
        String e = sanitizeIdentifier(edgeLabel, "edge label");
        String sl = sanitizeIdentifier(srcLabel, "edge source label");
        String sk = sanitizeIdentifier(srcKey, "edge source key");
        String dl = sanitizeIdentifier(dstLabel, "edge destination label");
        String dk = sanitizeIdentifier(dstKey, "edge destination key");

        String cypher = "UNWIND $" + BATCH_PARAM + " AS row\n"
                + "MATCH (a:" + sl + " {" + sk + ": row." + ROW_SRC + "}), "
                + "(b:" + dl + " {" + dk + ": row." + ROW_DST + "})\n"
                + "MERGE (a)-[e:" + e + "]->(b)\n"
                + "SET e += row." + ROW_PROPS + "\n"
                + "RETURN count(e) AS " + WRITTEN_COUNT_FIELD;

        List<Map<String, Object>> rows = new ArrayList<>(batch.size());
        for (Edge edge : batch) {
            Map<String, Object> row = new LinkedHashMap<>(3);
            row.put(ROW_SRC, edge.srcValue());
            row.put(ROW_DST, edge.dstValue());
            row.put(ROW_PROPS, nonNullProperties(edge.properties()));
            rows.add(row);
        }
        return new CypherStatement(cypher, singletonBatch(rows));
    }

    /**
     * Wrap an identifier in backticks, escaping any embedded backtick by doubling it. This keeps
     * arbitrary label / property names safe to splice into the query (they cannot be parameterized).
     */
    protected String sanitizeIdentifier(String identifier, String role) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException(role + " must not be null or empty");
        }
        return "`" + identifier.replace("`", "``") + "`";
    }

    /** Copy properties, dropping {@code null} values so they do not overwrite existing data. */
    private static Map<String, Object> nonNullProperties(Map<String, Object> properties) {
        Map<String, Object> out = new LinkedHashMap<>(Math.max(4, properties.size()));
        for (Map.Entry<String, Object> en : properties.entrySet()) {
            if (en.getValue() != null) {
                out.put(en.getKey(), en.getValue());
            }
        }
        return out;
    }

    private static Map<String, Object> singletonBatch(List<Map<String, Object>> rows) {
        Map<String, Object> params = new LinkedHashMap<>(1);
        params.put(BATCH_PARAM, rows);
        return params;
    }

    private static void requireNonEmpty(List<?> batch, String name) {
        if (batch == null || batch.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }
}
