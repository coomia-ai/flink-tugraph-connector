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

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Translates Flink filter predicates into a TuGraph {@code WHERE} clause for push-down. Each
 * top-level conjunct is translated independently: supported ones become {@code n.col <op> $param}
 * (or {@code n.col IN $param}) and are pushed to TuGraph; the rest are returned for Flink to apply.
 *
 * <p>Supported: {@code =, <>, >, >=, <, <=} between a column and a literal (either operand order),
 * and {@code IN} of literals. Values are parameterized (injection-safe) and normalized to the Bolt
 * Java types TuGraph compares against.
 */
final class TuGraphFilterTranslator {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private TuGraphFilterTranslator() {
    }

    /** The outcome of translating a list of filters. */
    static final class Translation {
        /** Combined accepted predicates (ANDed), without the leading {@code WHERE}; null if none. */
        final String whereClause;
        final Map<String, Object> params;
        final List<ResolvedExpression> accepted;
        final List<ResolvedExpression> remaining;

        Translation(String whereClause, Map<String, Object> params,
                    List<ResolvedExpression> accepted, List<ResolvedExpression> remaining) {
            this.whereClause = whereClause;
            this.params = params;
            this.accepted = accepted;
            this.remaining = remaining;
        }
    }

    static Translation translate(List<ResolvedExpression> filters) {
        List<String> fragments = new ArrayList<>();
        Map<String, Object> params = new LinkedHashMap<>();
        List<ResolvedExpression> accepted = new ArrayList<>();
        List<ResolvedExpression> remaining = new ArrayList<>();
        int[] counter = {0};

        for (ResolvedExpression filter : filters) {
            String fragment = toCypher(filter, params, counter);
            if (fragment != null) {
                fragments.add(fragment);
                accepted.add(filter);
            } else {
                remaining.add(filter);
            }
        }
        String where = fragments.isEmpty() ? null : String.join(" AND ", fragments);
        return new Translation(where, params, accepted, remaining);
    }

    private static String toCypher(ResolvedExpression expr, Map<String, Object> params, int[] counter) {
        if (!(expr instanceof CallExpression)) {
            return null;
        }
        CallExpression call = (CallExpression) expr;
        FunctionDefinition fn = call.getFunctionDefinition();
        List<ResolvedExpression> args = call.getResolvedChildren();

        String op = comparisonOp(fn);
        if (op != null && args.size() == 2) {
            return comparison(args.get(0), args.get(1), op, params, counter);
        }
        if (fn == BuiltInFunctionDefinitions.IN) {
            return in(args, params, counter);
        }
        return null;
    }

    private static String comparisonOp(FunctionDefinition fn) {
        if (fn == BuiltInFunctionDefinitions.EQUALS) {
            return "=";
        } else if (fn == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return "<>";
        } else if (fn == BuiltInFunctionDefinitions.GREATER_THAN) {
            return ">";
        } else if (fn == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return ">=";
        } else if (fn == BuiltInFunctionDefinitions.LESS_THAN) {
            return "<";
        } else if (fn == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return "<=";
        }
        return null;
    }

    private static String comparison(ResolvedExpression a, ResolvedExpression b, String op,
                                     Map<String, Object> params, int[] counter) {
        if (a instanceof FieldReferenceExpression && b instanceof ValueLiteralExpression) {
            return compare((FieldReferenceExpression) a, (ValueLiteralExpression) b, op, false, params, counter);
        }
        if (b instanceof FieldReferenceExpression && a instanceof ValueLiteralExpression) {
            return compare((FieldReferenceExpression) b, (ValueLiteralExpression) a, op, true, params, counter);
        }
        return null;
    }

    private static String compare(FieldReferenceExpression field, ValueLiteralExpression literal,
                                  String op, boolean flipped, Map<String, Object> params, int[] counter) {
        String name = field.getName();
        if (!IDENTIFIER.matcher(name).matches()) {
            return null;
        }
        Object value = literalValue(literal);
        if (value == null) {
            return null;
        }
        String effectiveOp = flipped ? flip(op) : op;
        String param = "f" + counter[0]++;
        params.put(param, value);
        return "n." + name + " " + effectiveOp + " $" + param;
    }

    private static String in(List<ResolvedExpression> args, Map<String, Object> params, int[] counter) {
        if (args.isEmpty() || !(args.get(0) instanceof FieldReferenceExpression)) {
            return null;
        }
        FieldReferenceExpression field = (FieldReferenceExpression) args.get(0);
        if (!IDENTIFIER.matcher(field.getName()).matches()) {
            return null;
        }
        List<Object> values = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            if (!(args.get(i) instanceof ValueLiteralExpression)) {
                return null;
            }
            Object value = literalValue((ValueLiteralExpression) args.get(i));
            if (value == null) {
                return null;
            }
            values.add(value);
        }
        if (values.isEmpty()) {
            return null;
        }
        String param = "f" + counter[0]++;
        params.put(param, values);
        return "n." + field.getName() + " IN $" + param;
    }

    /** Extract a literal as a Bolt-friendly value (integral -> Long, floating -> Double, ...). */
    private static Object literalValue(ValueLiteralExpression literal) {
        if (literal.isNull()) {
            return null;
        }
        Class<?> conversionClass = literal.getOutputDataType().getConversionClass();
        Object value = literal.getValueAs(conversionClass).orElse(null);
        if (value == null) {
            return null;
        }
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        if (value instanceof Float || value instanceof Double) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue();
        }
        if (value instanceof Boolean) {
            return value;
        }
        if (value instanceof String) {
            return value;
        }
        if (value instanceof CharSequence) {
            return value.toString();
        }
        return null; // unsupported literal type -> not pushed
    }

    private static String flip(String op) {
        switch (op) {
            case ">":
                return "<";
            case ">=":
                return "<=";
            case "<":
                return ">";
            case "<=":
                return ">=";
            default:
                return op; // = and <> are symmetric
        }
    }
}
