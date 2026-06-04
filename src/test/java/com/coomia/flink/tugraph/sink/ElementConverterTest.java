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

package com.coomia.flink.tugraph.sink;

import com.coomia.flink.tugraph.element.GraphElement.ChangeOp;
import com.coomia.flink.tugraph.element.Vertex;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the DataStream delete API ({@code asDelete()} + identity converter). */
class ElementConverterTest {

    @Test
    void identityConverterDerivesDeleteFromOp() {
        ElementConverter<Vertex> converter = ElementConverter.identity();
        Vertex upsert = new Vertex("V", "id", "v1", Map.of("id", "v1"));
        Vertex delete = upsert.asDelete();

        assertThat(converter.isDelete(upsert)).isFalse();
        assertThat(converter.isDelete(delete)).isTrue();
        assertThat(converter.convert(upsert)).isSameAs(upsert);
    }

    @Test
    void asDeleteIsImmutableAndIdempotent() {
        Vertex upsert = new Vertex("V", "id", "v1", Map.of("id", "v1"));
        Vertex delete = upsert.asDelete();

        assertThat(upsert.op()).isEqualTo(ChangeOp.UPSERT); // original unchanged
        assertThat(delete.op()).isEqualTo(ChangeOp.DELETE);
        assertThat(delete).isNotSameAs(upsert);
        assertThat(delete.asDelete()).isSameAs(delete); // no-op when already a delete
        // Same identity data, different op -> not equal.
        assertThat(delete).isNotEqualTo(upsert);
    }
}
