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

import java.io.Serializable;
import java.util.List;

/**
 * Turns a homogeneous batch of graph elements into a single parameterized batch Cypher statement.
 *
 * <p>This is the connector's main extension point for dialect portability. TuGraph implements a
 * <em>subset</em> of openCypher, so the exact support for {@code UNWIND}, {@code MERGE} and
 * {@code SET n += $map} must be verified against the target instance. When the default
 * {@link MergeCypherStatementBuilder} is incompatible, supply an alternative implementation (for
 * example one that expands {@code SET n.k = row.props.k} property-by-property) without touching the
 * sink or the connection layer.
 *
 * <p>Implementations must be {@link Serializable} (the sink is shipped to task managers) and
 * stateless / thread-confined per writer.
 */
public interface CypherStatementBuilder extends Serializable {

    /**
     * Build a vertex upsert for a batch sharing the same {@code (label, primaryKey)}.
     *
     * @param label      vertex label
     * @param primaryKey primary-key property name
     * @param batch      vertices to upsert (must be non-empty)
     * @return a parameterized batch statement
     */
    CypherStatement buildVertexUpsert(String label, String primaryKey, List<Vertex> batch);

    /**
     * Build an edge upsert for a batch sharing the same edge label and endpoint definitions.
     *
     * <p>The generated statement returns the number of edges actually written (rows whose source
     * and destination vertices both matched) under the alias {@link #WRITTEN_COUNT_FIELD}, so the
     * caller can derive the count of skipped edges and enforce the missing-endpoint policy.
     *
     * @param edgeLabel edge label
     * @param srcLabel  source vertex label
     * @param srcKey    source vertex match property
     * @param dstLabel  destination vertex label
     * @param dstKey    destination vertex match property
     * @param batch     edges to upsert (must be non-empty)
     * @return a parameterized batch statement
     */
    CypherStatement buildEdgeUpsert(String edgeLabel,
                                    String srcLabel, String srcKey,
                                    String dstLabel, String dstKey,
                                    List<Edge> batch);

    /** Result alias carrying the number of edges written, used to compute skipped endpoints. */
    String WRITTEN_COUNT_FIELD = "written";
}
