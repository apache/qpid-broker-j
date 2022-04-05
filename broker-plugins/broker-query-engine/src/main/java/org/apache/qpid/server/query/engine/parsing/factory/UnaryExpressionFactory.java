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

import java.math.BigDecimal;
import java.util.Objects;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.NullLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.NumberLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;

/**
 * Factory creating unary expressions
 */
public final class UnaryExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private UnaryExpressionFactory()
    {

    }

    /**
     * Negates the expression supplied
     *
     * @param expression ExpressionNode
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Negated expression
     */
    @SuppressWarnings("unchecked")
    public static <T, R> ExpressionNode<T, R> negate(final ExpressionNode<T, R> expression)
    {
        Objects.requireNonNull(expression, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        if (expression instanceof NumberLiteralExpression)
        {
            final NumberLiteralExpression<T> numberLiteralExpression = (NumberLiteralExpression<T>) expression;
            final Number number = numberLiteralExpression.get();
            if (number == null)
            {
                return new NullLiteralExpression<>();
            }
            return (ExpressionNode<T, R>) new NumberLiteralExpression<T>(NumberConverter.narrow(BigDecimal.ZERO.subtract(new BigDecimal(number.toString()))));
        }
        throw QueryParsingException.of(Errors.NEGATION.NOT_SUPPORTED, StringUtils.getClassName(expression));
    }
}
