/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.query.engine.parsing.expression.set;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;

/**
 * Set INTERSECT expression is used to retain the results of right SELECT statement present in the results of left SELECT statement.
 * Every SELECT statement within INTERSECT must have the same number of columns. The INTERSECT operator selects all values by default.
 * To eliminate duplicates, INTERSECT DISTINCT should be used.
 *
 * @param <T> Input parameter type
 */
public class IntersectExpression<T> extends AbstractSetExpression<T, Stream<Map<String, Object>>>
{
    /**
     * Constructor initializes children expression list
     *
     * @param distinct Distinct flag
     * @param left Left expression
     * @param right Right expression
     */
    public IntersectExpression(
        final boolean distinct,
        final ExpressionNode<T, Stream<Map<String, Object>>> left,
        final ExpressionNode<T, Stream<Map<String, Object>>> right
    )
    {
        super(distinct, left, right);
    }

    /**
     * Performs INTERSECT operation on both set expressions
     *
     * @param value Object to handle
     *
     * @return Stream combining entities from both expressions
     */
    @Override
    public Stream<Map<String, Object>> apply(final T value)
    {
        final Stream<Map<String, Object>> leftStream = evaluateChild(0, value);
        final Stream<Map<String, Object>> rightStream = evaluateChild(1, value);
        final List<Map<String, Object>> left = leftStream.collect(Collectors.toList());
        final List<Map<String, Object>> right = rightStream.collect(Collectors.toList());
        left.retainAll(right);
        return _distinct ? left.stream().distinct() : left.stream();
    }
}