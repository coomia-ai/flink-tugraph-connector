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
 * Turns a homogeneous batch of graph elements into the Cypher statements that upsert them.
 *
 * <p>A batch maps to a <em>list</em> of statements because TuGraph's openCypher subset cannot
 * always express a whole batch in one query: vertices that share a label but differ in which
 * properties are present become one {@code UNWIND} statement per property-set, and edges become one
 * parameterized statement each (TuGraph rejects matching two endpoints inside an {@code UNWIND}).
 * Run the returned statements together in a single transaction.
 *
 * <p>This is the connector's main extension point for dialect portability. Implementations must be
 * {@link Serializable} (the sink is shipped to task managers).
 */
public interface CypherStatementBuilder extends Serializable {

    /**
     * Build the statement(s) that upsert a batch of vertices sharing {@code (label, primaryKey)}.
     *
     * @param label      vertex label
     * @param primaryKey primary-key property name
     * @param batch      vertices to upsert (must be non-empty)
     * @return one or more parameterized statements to run in a single transaction
     */
    List<CypherStatement> buildVertexUpsert(String label, String primaryKey, List<Vertex> batch);

    /**
     * Build the statement(s) that upsert a batch of edges sharing the edge label and endpoint
     * definitions.
     *
     * <p>Each returned statement reports the number of edges it wrote (rows whose endpoints both
     * matched) under {@link #WRITTEN_COUNT_FIELD}, so the caller can derive the count of skipped
     * edges and enforce the missing-endpoint policy.
     *
     * @param edgeLabel edge label
     * @param srcLabel  source vertex label
     * @param srcKey    source vertex match property
     * @param dstLabel  destination vertex label
     * @param dstKey    destination vertex match property
     * @param batch     edges to upsert (must be non-empty)
     * @return one parameterized statement per edge, to run in a single transaction
     */
    List<CypherStatement> buildEdgeUpsert(String edgeLabel,
                                          String srcLabel, String srcKey,
                                          String dstLabel, String dstKey,
                                          List<Edge> batch);

    /** Result alias carrying the number of edges written, used to compute skipped endpoints. */
    String WRITTEN_COUNT_FIELD = "written";
}
