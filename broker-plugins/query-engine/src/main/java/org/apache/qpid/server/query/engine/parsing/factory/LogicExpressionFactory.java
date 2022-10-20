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

import java.util.Objects;
import java.util.function.Predicate;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.logic.AndExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.ExpressionWrapperExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.NotExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.OrExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.PredicateWrapperExpression;
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;

/**
 * Factory creating logic expressions
 */
public final class LogicExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private LogicExpressionFactory()
    {

    }

    /**
     * Creates an AndExpression instance
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return AndExpression instance
     */
    public static <T, R> AndExpression<T> and(
        final String alias,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new AndExpression<>(alias, left, right);
    }

    /**
     * Creates an OrExpression instance
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return OrExpression instance
     */
    public static <T, R> OrExpression<T> or(
        final String alias,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new OrExpression<>(alias, left, right);
    }

    /**
     * Wraps expression into a predicate
     *
     * @param expression Source expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Predicate
     */
    @SuppressWarnings("unchecked")
    public static <T, R> Predicate<T> toPredicate(
        final ExpressionNode<T, R> expression
    )
    {
        Objects.requireNonNull(expression, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        if (expression instanceof Predicate)
        {
            return (Predicate<T>) expression;
        }
        return new PredicateWrapperExpression<>(expression);
    }

    /**
     * Wraps predicate into an expression
     *
     * @param predicate Predicate instance
     *
     * @param <T> Input parameter type
     *
     * @return Expression instance
     */
    @SuppressWarnings("unchecked")
    public static <T> Expression<T, Boolean> toFunction(
        final Predicate<T> predicate
    )
    {
        Objects.requireNonNull(predicate, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        if (predicate instanceof Expression)
        {
            return (Expression<T, Boolean>) predicate;
        }
        return new ExpressionWrapperExpression<>(predicate);
    }

    /**
     * Creates a NotExpression from a source expression
     *
     * @param value Source expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return NotExpression instance
     */
    public static <T, R> NotExpression<T, R> negate(
        final Expression<T, R> value
    )
    {
        Objects.requireNonNull(value, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        if (!(value instanceof Predicate))
        {
            throw QueryParsingException.of(Errors.NEGATION.NOT_SUPPORTED, StringUtils.getClassName(value));
        }
        return new NotExpression<>(value);
    }
}
