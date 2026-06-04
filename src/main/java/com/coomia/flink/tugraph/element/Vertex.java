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
 * A graph vertex.
 *
 * <p>Write semantics (idempotent upsert):
 * {@code MERGE (n:label {primaryKey: <pkValue>}) SET n += properties}.
 *
 * @see com.coomia.flink.tugraph.cypher.MergeCypherStatementBuilder
 */
public final class Vertex extends GraphElement {

    private static final long serialVersionUID = 1L;

    private final String primaryKey;
    private final Object primaryKeyValue;

    /**
     * @param label           vertex label (TuGraph vertex label)
     * @param primaryKey      primary-key property name (a stable identifier column)
     * @param primaryKeyValue primary-key value used as the MERGE match key
     * @param properties      all properties (may include the primary key; {@code null} values are
     *                        skipped at write time)
     */
    public Vertex(String label, String primaryKey, Object primaryKeyValue, Map<String, Object> properties) {
        super(Objects.requireNonNull(label, "label"), properties);
        this.primaryKey = Objects.requireNonNull(primaryKey, "primaryKey");
        this.primaryKeyValue = primaryKeyValue;
    }

    /** Convenience factory mirroring the constructor. */
    public static Vertex of(String label, String primaryKey, Object primaryKeyValue, Map<String, Object> properties) {
        return new Vertex(label, primaryKey, primaryKeyValue, properties);
    }

    @Override
    public Kind kind() {
        return Kind.VERTEX;
    }

    /** @return the primary-key property name. */
    public String primaryKey() {
        return primaryKey;
    }

    /** @return the primary-key value. */
    public Object primaryKeyValue() {
        return primaryKeyValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Vertex)) {
            return false;
        }
        Vertex vertex = (Vertex) o;
        return label.equals(vertex.label)
                && primaryKey.equals(vertex.primaryKey)
                && Objects.equals(primaryKeyValue, vertex.primaryKeyValue)
                && properties.equals(vertex.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, primaryKey, primaryKeyValue, properties);
    }

    @Override
    public String toString() {
        return "Vertex{label=" + label + ", " + primaryKey + "=" + primaryKeyValue
                + ", props=" + properties.size() + "}";
    }
}
