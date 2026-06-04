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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Base type for a graph element ({@link Vertex} or {@link Edge}).
 *
 * <p>The sink buffers writes in units of {@code GraphElement}. On flush they are grouped by
 * {@link Kind} and {@code label} (plus endpoint labels for edges) and turned into a single
 * parameterized batch Cypher statement by the
 * {@code com.coomia.flink.tugraph.cypher.CypherStatementBuilder}.
 *
 * <p>Instances are immutable and {@link Serializable} so they can be carried through Flink's
 * record pipeline and checkpointed buffers.
 */
public abstract class GraphElement implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Discriminator for the two element kinds. */
    public enum Kind {
        VERTEX,
        EDGE
    }

    /** Element label (TuGraph vertex/edge label). Never {@code null}. */
    protected final String label;

    /**
     * All properties of this element. May include the primary-key value for vertices. Properties
     * whose value is {@code null} are skipped at write time so they never overwrite existing data.
     */
    protected final Map<String, Object> properties;

    protected GraphElement(String label, Map<String, Object> properties) {
        this.label = label;
        this.properties = properties == null ? Collections.emptyMap() : properties;
    }

    /** @return whether this element is a vertex or an edge. */
    public abstract Kind kind();

    /** @return the element label. */
    public String label() {
        return label;
    }

    /** @return the (immutable view of) element properties. */
    public Map<String, Object> properties() {
        return properties;
    }
}
