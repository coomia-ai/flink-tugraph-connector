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

package com.coomia.flink.tugraph.element;

import java.util.Map;
import java.util.Objects;

/**
 * A directed graph edge.
 *
 * <p>Write semantics (idempotent upsert):
 * <pre>
 *   MATCH (a:srcLabel {srcKey: srcValue}), (b:dstLabel {dstKey: dstValue})
 *   MERGE (a)-[e:label]-&gt;(b) SET e += properties
 * </pre>
 *
 * <p>When an endpoint is missing (the source or destination vertex has not been written yet) the
 * behaviour is governed by the {@code edge.on-missing-endpoint} option (skip / fail).
 *
 * @see com.coomia.flink.tugraph.cypher.MergeCypherStatementBuilder
 */
public final class Edge extends GraphElement {

    private static final long serialVersionUID = 1L;

    private final String srcLabel;
    private final String srcKey;
    private final Object srcValue;
    private final String dstLabel;
    private final String dstKey;
    private final Object dstValue;

    /**
     * @param label      edge label
     * @param srcLabel   source vertex label
     * @param srcKey     source vertex match property (primary-key column)
     * @param srcValue   source vertex match value
     * @param dstLabel   destination vertex label
     * @param dstKey     destination vertex match property (primary-key column)
     * @param dstValue   destination vertex match value
     * @param properties edge properties ({@code null} values are skipped at write time)
     */
    public Edge(String label,
                String srcLabel, String srcKey, Object srcValue,
                String dstLabel, String dstKey, Object dstValue,
                Map<String, Object> properties) {
        super(Objects.requireNonNull(label, "label"), properties);
        this.srcLabel = Objects.requireNonNull(srcLabel, "srcLabel");
        this.srcKey = Objects.requireNonNull(srcKey, "srcKey");
        this.srcValue = srcValue;
        this.dstLabel = Objects.requireNonNull(dstLabel, "dstLabel");
        this.dstKey = Objects.requireNonNull(dstKey, "dstKey");
        this.dstValue = dstValue;
    }

    @Override
    public Kind kind() {
        return Kind.EDGE;
    }

    public String srcLabel() {
        return srcLabel;
    }

    public String srcKey() {
        return srcKey;
    }

    public Object srcValue() {
        return srcValue;
    }

    public String dstLabel() {
        return dstLabel;
    }

    public String dstKey() {
        return dstKey;
    }

    public Object dstValue() {
        return dstValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Edge)) {
            return false;
        }
        Edge edge = (Edge) o;
        return label.equals(edge.label)
                && srcLabel.equals(edge.srcLabel)
                && srcKey.equals(edge.srcKey)
                && Objects.equals(srcValue, edge.srcValue)
                && dstLabel.equals(edge.dstLabel)
                && dstKey.equals(edge.dstKey)
                && Objects.equals(dstValue, edge.dstValue)
                && properties.equals(edge.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, srcLabel, srcKey, srcValue, dstLabel, dstKey, dstValue, properties);
    }

    @Override
    public String toString() {
        return "Edge{(" + srcLabel + ":" + srcValue + ")-[" + label + "]->("
                + dstLabel + ":" + dstValue + "), props=" + properties.size() + "}";
    }
}
