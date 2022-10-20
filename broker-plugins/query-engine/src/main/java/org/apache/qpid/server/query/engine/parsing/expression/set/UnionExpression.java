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
 * Set UNION expression is used to combine the results of two or more SELECT statements. Every SELECT statement
 * within UNION must have the same number of columns. The UNION operator selects all values by default.
 * To eliminate duplicates, UNION DISTINCT should be used.
 *
 * @param <T> Input parameter type
 */
public class UnionExpression<T> extends AbstractSetExpression<T, Stream<Map<String, Object>>>
{
    /**
     * Constructor initializes children expression list
     *
     * @param distinct Distinct flag
     * @param left Left expression
     * @param right Right expression
     */
    public UnionExpression(
        final boolean distinct,
        final ExpressionNode<T, Stream<Map<String, Object>>> left,
        final ExpressionNode<T, Stream<Map<String, Object>>> right
    )
    {
        super(distinct, left, right);
    }

    /**
     * Performs UNION operation on both set expressions
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
        left.addAll(right);
        if (_distinct)
        {
            return left.stream().distinct();
        }
        return left.stream();
    }
}
