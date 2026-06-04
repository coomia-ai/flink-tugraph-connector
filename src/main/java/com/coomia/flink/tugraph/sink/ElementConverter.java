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

import com.coomia.flink.tugraph.element.GraphElement;

import java.io.Serializable;

/**
 * Converts an input record into a {@link GraphElement} so the sink writer can buffer a single
 * element type regardless of the upstream API.
 *
 * <p>The DataStream path uses an identity converter (the records are already {@code GraphElement}s);
 * the Table/SQL path uses {@code RowDataToElementConverter}.
 *
 * @param <InputT> upstream record type
 */
@FunctionalInterface
public interface ElementConverter<InputT> extends Serializable {

    /**
     * @param record an upstream record (never {@code null})
     * @return the graph element to write, or {@code null} to drop the record
     */
    GraphElement convert(InputT record);

    /**
     * @param record an upstream record
     * @return {@code true} if the record represents a deletion (the element should be removed)
     *         rather than an upsert. Defaults to {@code false} (upsert / append).
     */
    default boolean isDelete(InputT record) {
        return false;
    }

    /** @return the identity converter for streams that already emit {@link GraphElement}s. */
    static <T extends GraphElement> ElementConverter<T> identity() {
        return record -> record;
    }
}
