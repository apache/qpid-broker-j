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

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.arithmetic.DivideExpression;
import org.apache.qpid.server.query.engine.parsing.expression.arithmetic.MinusExpression;
import org.apache.qpid.server.query.engine.parsing.expression.arithmetic.ModExpression;
import org.apache.qpid.server.query.engine.parsing.expression.arithmetic.MultiplyExpression;
import org.apache.qpid.server.query.engine.parsing.expression.arithmetic.PlusExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;

/**
 * Factory creating arithmetic expressions
 */
public final class ArithmeticExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private ArithmeticExpressionFactory()
    {

    }

    /**
     * Creates DIVIDE expression
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return DivideExpression instance
     */
    public static <T, R> ExpressionNode<T, R> divide(
        final String alias,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        final DivideExpression<T, R> result = new DivideExpression<>(alias, left, right);
        return result.isInstantlyEvaluable()
            ? ConstantExpression.of(result.getAlias(), result.apply(null))
            : result;
    }

    /**
     * Creates MINUS expression
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return MinusExpression instance
     */
    public static <T, R> ExpressionNode<T, R> minus(
        final String alias,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        final MinusExpression<T, R> result = new MinusExpression<>(alias, left, right);
        return result.isInstantlyEvaluable()
            ? ConstantExpression.of(result.getAlias(), result.apply(null))
            : result;
    }

    /**
     * Creates MOD expression
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return ModExpression instance
     */
    public static <T, R> ExpressionNode<T, R> mod(
        final String alias,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        final ModExpression<T, R> result = new ModExpression<>(alias, left, right);
        return result.isInstantlyEvaluable()
            ? ConstantExpression.of(result.getAlias(), result.apply(null))
            : result;
    }

    /**
     * Creates MULTIPLY expression
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return MultiplyExpression instance
     */
    public static <T, R> ExpressionNode<T, R> multiply(
        final String alias,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        final MultiplyExpression<T, R> result = new MultiplyExpression<>(alias, left, right);
        return result.isInstantlyEvaluable()
            ? ConstantExpression.of(result.getAlias(), result.apply(null))
            : result;
    }

    /**
     * Creates PLUS expression
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param right Right expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return PlusExpression instance
     */
    public static <T, R> ExpressionNode<T, R> plus(
        final String alias,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        Objects.requireNonNull(left, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        Objects.requireNonNull(right, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        final PlusExpression<T, R> result =  new PlusExpression<>(alias, left, right);
        return result.isInstantlyEvaluable()
            ? ConstantExpression.of(result.getAlias(), result.apply(null))
            : result;
    }
}
