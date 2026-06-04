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

package com.coomia.flink.tugraph;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TuGraphSinkOptions} construction and validation. */
class TuGraphSinkOptionsTest {

    @Test
    void build_appliesDefaults() {
        TuGraphSinkOptions o = TuGraphSinkOptions.builder()
                .uri("bolt://localhost:7687")
                .auth("admin", "secret")
                .build();

        assertThat(o.graph()).isEqualTo("default");
        assertThat(o.batchSize()).isEqualTo(500);
        assertThat(o.batchIntervalMs()).isEqualTo(1_000L);
        assertThat(o.maxRetries()).isEqualTo(3);
        assertThat(o.connectionTimeoutMs()).isEqualTo(15_000L);
        assertThat(o.maxConnectionPoolSize()).isEqualTo(10);
        assertThat(o.onMissingEndpoint()).isEqualTo(TuGraphSinkOptions.OnMissingEndpoint.SKIP);
    }

    @Test
    void build_rejectsMissingUri() {
        assertThatThrownBy(() -> TuGraphSinkOptions.builder().auth("u", "p").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uri");
    }

    @Test
    void build_rejectsBlankUsername() {
        assertThatThrownBy(() -> TuGraphSinkOptions.builder()
                .uri("bolt://localhost:7687").username("  ").password("p").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("username");
    }

    @Test
    void build_rejectsNullPassword() {
        assertThatThrownBy(() -> TuGraphSinkOptions.builder()
                .uri("bolt://localhost:7687").username("u").build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void build_rejectsNonPositiveBatchSize() {
        assertThatThrownBy(() -> TuGraphSinkOptions.builder()
                .uri("bolt://localhost:7687").auth("u", "p").batchSize(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchSize");
    }

    @Test
    void toString_masksPassword() {
        TuGraphSinkOptions o = TuGraphSinkOptions.builder()
                .uri("bolt://localhost:7687").auth("admin", "topsecret").build();
        assertThat(o.toString()).doesNotContain("topsecret");
    }
}
