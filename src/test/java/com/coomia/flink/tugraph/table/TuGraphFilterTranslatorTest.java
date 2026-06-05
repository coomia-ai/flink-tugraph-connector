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

package com.coomia.flink.tugraph.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TuGraphFilterTranslator} (predicate -> Cypher WHERE push-down). */
class TuGraphFilterTranslatorTest {

    private static FieldReferenceExpression field(String name) {
        return new FieldReferenceExpression(name, DataTypes.DOUBLE(), 0, 0);
    }

    private static FieldReferenceExpression strField(String name) {
        return new FieldReferenceExpression(name, DataTypes.STRING(), 0, 0);
    }

    private static CallExpression call(FunctionDefinition fn, ResolvedExpression... args) {
        return CallExpression.anonymous(fn, List.of(args), DataTypes.BOOLEAN());
    }

    @Test
    void translatesGreaterThanFieldLiteral() {
        ResolvedExpression gt = call(BuiltInFunctionDefinitions.GREATER_THAN,
                field("reg_capital"), new ValueLiteralExpression(150.0d));

        TuGraphFilterTranslator.Translation t = TuGraphFilterTranslator.translate(List.of(gt));
        assertThat(t.whereClause).isEqualTo("n.reg_capital > $f0");
        assertThat(t.params).containsEntry("f0", 150.0d);
        assertThat(t.accepted).hasSize(1);
        assertThat(t.remaining).isEmpty();
    }

    @Test
    void flipsOperatorWhenLiteralIsOnTheLeft() {
        // 150 < reg_capital  ==>  reg_capital > 150
        ResolvedExpression lt = call(BuiltInFunctionDefinitions.LESS_THAN,
                new ValueLiteralExpression(150.0d), field("reg_capital"));
        TuGraphFilterTranslator.Translation t = TuGraphFilterTranslator.translate(List.of(lt));
        assertThat(t.whereClause).isEqualTo("n.reg_capital > $f0");
    }

    @Test
    void translatesEqualsAndIn() {
        ResolvedExpression eq = call(BuiltInFunctionDefinitions.EQUALS,
                strField("name"), new ValueLiteralExpression("Beta"));
        ResolvedExpression in = call(BuiltInFunctionDefinitions.IN,
                strField("company_id"), new ValueLiteralExpression("p1"), new ValueLiteralExpression("p3"));

        TuGraphFilterTranslator.Translation t = TuGraphFilterTranslator.translate(List.of(eq, in));
        assertThat(t.whereClause).isEqualTo("n.name = $f0 AND n.company_id IN $f1");
        assertThat(t.params).containsEntry("f0", "Beta");
        assertThat(t.params.get("f1")).isEqualTo(List.of("p1", "p3"));
        assertThat(t.accepted).hasSize(2);
    }

    @Test
    void leavesUnsupportedPredicateAsRemaining() {
        // field > field is not a field/literal comparison -> not pushed.
        ResolvedExpression fieldVsField = call(BuiltInFunctionDefinitions.GREATER_THAN,
                field("a"), field("b"));
        ResolvedExpression supported = call(BuiltInFunctionDefinitions.EQUALS,
                strField("name"), new ValueLiteralExpression("Beta"));

        TuGraphFilterTranslator.Translation t =
                TuGraphFilterTranslator.translate(List.of(fieldVsField, supported));
        assertThat(t.whereClause).isEqualTo("n.name = $f0");
        assertThat(t.accepted).containsExactly(supported);
        assertThat(t.remaining).containsExactly(fieldVsField);
    }
}
