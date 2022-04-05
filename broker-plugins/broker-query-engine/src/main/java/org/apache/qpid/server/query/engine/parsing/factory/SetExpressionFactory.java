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
package org.apache.qpid.server.query.engine.parsing.factory;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.set.IntersectExpression;
import org.apache.qpid.server.query.engine.parsing.expression.set.MinusExpression;
import org.apache.qpid.server.query.engine.parsing.expression.set.UnionExpression;

/**
 * Factory creating set expressions
 */
public final class SetExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private SetExpressionFactory()
    {

    }

    /**
     * Creates an IntersectExpression instance
     *
     * @param distinct Distinct boolean flag
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     *
     * @return IntersectExpression instance
     */
    public static <T> IntersectExpression<T> intersect(
        final boolean distinct,
        final ExpressionNode<T, Stream<Map<String, Object>>> left,
        final ExpressionNode<T, Stream<Map<String, Object>>> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new IntersectExpression<>(distinct, left, right);
    }

    /**
     * Creates a MinusExpression instance
     *
     * @param distinct Distinct boolean flag
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     *
     * @return MinusExpression instance
     */
    public static <T> MinusExpression<T> minus(
        final boolean distinct,
        final ExpressionNode<T, Stream<Map<String, Object>>> left,
        final ExpressionNode<T, Stream<Map<String, Object>>> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new MinusExpression<>(distinct, left, right);
    }

    /**
     * Creates an UnionExpression instance
     *
     * @param distinct Distinct boolean flag
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     *
     * @return UnionExpression instance
     */
    public static <T> UnionExpression<T> union(
        final boolean distinct,
        final ExpressionNode<T, Stream<Map<String, Object>>> left,
        final ExpressionNode<T, Stream<Map<String, Object>>> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new UnionExpression<>(distinct, left, right);
    }
}
