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

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.BetweenExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.EqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.GreaterThanExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.GreaterThanOrEqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.InExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.IsNullExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.LessThanExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.LessThanOrEqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.LikeExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.NotExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Factory creating comparison expressions
 */
public final class ComparisonExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private ComparisonExpressionFactory()
    {

    }

    /**
     * Creates EQUAL expression
     *
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return EqualExpression instance
     */
    public static <T, R> Predicate<T> equalExpression(final ExpressionNode<T, R> left, final ExpressionNode<T, R> right)
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new EqualExpression<>(left, right);
    }

    /**
     * Creates NOT EQUAL expression
     *
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return NotExpression instance
     */
    public static <T, R> Predicate<T> notEqualExpression(
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new NotExpression<>(new EqualExpression<>(left, right));
    }

    /**
     * Creates IS NULL expression
     *
     * @param left Left expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return IsNullExpression instance
     */
    public static <T, R> Predicate<T> isNullExpression(final ExpressionNode<T, R> left)
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new IsNullExpression<>(left);
    }

    /**
     * Creates IS NOT NULL expression
     *
     * @param left Left expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return NotExpression instance
     */
    public static <T, R> Predicate<T> isNotNullExpression(final ExpressionNode<T, R> left)
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new NotExpression<>(new IsNullExpression<>(left));
    }

    /**
     * Creates LIKE expression
     *
     * @param left Left expression
     * @param source String source
     * @param escape Escape characters
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return LikeExpression instance
     */
    public static <T, R> LikeExpression<T, R> likeExpression(
        final ExpressionNode<T, R> left,
        final String source,
        final String escape
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new LikeExpression<>(left, source, escape);
    }

    /**
     * Creates BETWEEN expression
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param low Low Expression
     * @param high High expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return BetweenExpression
     */
    public static <T, R> BetweenExpression<T, R> betweenExpression(
        final String alias,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> low,
        final ExpressionNode<T, R> high
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(low, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(high, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new BetweenExpression<>(alias, left, low, high);
    }

    /**
     * Creates GREATER THAN expression
     *
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return GreaterThanExpression instance
     */
    public static <T, R> GreaterThanExpression<T, R> greaterThanExpression(
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new GreaterThanExpression<>(left, right);
    }

    /**
     * Creates GREATER THAN OR EQUAL expression
     *
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return GreaterThanOrEqualExpression instance
     */
    public static <T, R> Predicate<T> greaterThanOrEqualExpression(
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new GreaterThanOrEqualExpression<>(left, right);
    }

    /**
     * Creates LESS THAN expression
     *
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return LessThanExpression instance
     */
    public static <T, R> Predicate<T> lessThanExpression(
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new LessThanExpression<>(left, right);
    }

    /**
     * Creates LESS THAN OR EQUAL expression
     *
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return LessThanOrEqualExpression instance
     */
    public static <T, R> Predicate<T> lessThanOrEqualExpression(
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new LessThanOrEqualExpression<>(left, right);
    }

    /**
     * Creates IN expression
     *
     * @param left Left expression
     * @param right List of expressions
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return InExpression instance
     */
    public static <T, R> InExpression<T, R> inExpression(
        final ExpressionNode<T, R> left,
        final List<ExpressionNode<T, R>> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSIONS_NULL);
        final List<ExpressionNode<T, ?>> list = Stream.concat(Stream.of(left), right.stream()).collect(Collectors.toList());
        return new InExpression<>(list);
    }

    /**
     * Creates IN expression
     *
     * @param left Left expression
     * @param right Select expression containing subquery
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return InExpression instance
     */
    public static <T, R> InExpression<T, R> inExpression(
        final ExpressionNode<T, R> left,
        final SelectExpression<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        return new InExpression<>(left, right);
    }
}
