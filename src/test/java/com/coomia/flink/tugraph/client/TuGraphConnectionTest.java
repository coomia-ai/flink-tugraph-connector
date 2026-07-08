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

package com.coomia.flink.tugraph.client;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TuGraphConnection}'s schema-error classification. */
class TuGraphConnectionTest {

    @Test
    void missingVertexLabel_matchesTuGraphSchemaError() {
        // The exact server error reported when a vertex label is absent from the graph schema.
        assertThat(TuGraphConnection.isMissingVertexLabel(
                new DatabaseException("Neo.DatabaseError.General.UnknownError",
                        "No such vertex label: orders"))).isTrue();
        assertThat(TuGraphConnection.isMissingVertexLabel(
                new ClientException("Neo.ClientError.Statement.SyntaxError",
                        "No such vertex label: Person"))).isTrue();
    }

    @Test
    void missingVertexLabel_rejectsOtherErrors() {
        assertThat(TuGraphConnection.isMissingVertexLabel(
                new DatabaseException("Neo.DatabaseError.General.UnknownError",
                        "Txn fails to commit"))).isFalse();
        assertThat(TuGraphConnection.isMissingVertexLabel(
                new DatabaseException("Neo.DatabaseError.General.UnknownError", null))).isFalse();
    }

    @Test
    void missingVertexLabel_neverMatchesTransientFailures() {
        // Transient failures must keep going through the retry loop, whatever their message.
        assertThat(TuGraphConnection.isMissingVertexLabel(
                new TransientException("Neo.TransientError.General.TemporaryDisabled",
                        "No such vertex label: orders"))).isFalse();
        assertThat(TuGraphConnection.isMissingVertexLabel(
                new ServiceUnavailableException("No such vertex label: orders"))).isFalse();
    }
}
