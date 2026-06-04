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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable, parameterized Cypher statement: the query text plus its named parameters.
 *
 * <p>Identifiers (labels, property keys) are baked into {@link #cypher()} because Cypher does not
 * allow them to be parameterized; all <em>values</em> travel in {@link #parameters()} so they are
 * never string-concatenated into the query (injection-safe, plan-cache friendly).
 */
public final class CypherStatement implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String cypher;
    private final Map<String, Object> parameters;

    public CypherStatement(String cypher, Map<String, Object> parameters) {
        this.cypher = Objects.requireNonNull(cypher, "cypher");
        this.parameters = parameters == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(parameters);
    }

    /** @return the Cypher query text. */
    public String cypher() {
        return cypher;
    }

    /** @return the named parameters (immutable). */
    public Map<String, Object> parameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CypherStatement)) {
            return false;
        }
        CypherStatement that = (CypherStatement) o;
        return cypher.equals(that.cypher) && parameters.equals(that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cypher, parameters);
    }

    @Override
    public String toString() {
        return "CypherStatement{cypher='" + cypher + "', parameters=" + parameters + '}';
    }
}
