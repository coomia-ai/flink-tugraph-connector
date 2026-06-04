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

package com.coomia.flink.tugraph.it;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

/** Small read-back helpers for the integration tests, using the raw Bolt driver. */
final class GraphAssertions {

    private GraphAssertions() {
    }

    static long countVertices(TuGraphTestContainer container, String label) {
        return count(container, "MATCH (n:`" + label + "`) RETURN count(n) AS c");
    }

    static long countEdges(TuGraphTestContainer container, String edgeLabel) {
        return count(container, "MATCH ()-[e:`" + edgeLabel + "`]->() RETURN count(e) AS c");
    }

    static long count(TuGraphTestContainer container, String cypher) {
        try (Driver driver = GraphDatabase.driver(
                container.boltUri(),
                AuthTokens.basic(TuGraphTestContainer.username(), TuGraphTestContainer.password()));
                Session session = driver.session()) {
            return session.run(cypher).single().get("c").asLong();
        }
    }
}
