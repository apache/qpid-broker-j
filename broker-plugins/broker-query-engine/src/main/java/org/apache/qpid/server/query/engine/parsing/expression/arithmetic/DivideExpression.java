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
package org.apache.qpid.server.query.engine.parsing.expression.arithmetic;

import java.math.BigDecimal;
import java.util.Objects;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;

/**
 * Arithmetic division operation
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
public class DivideExpression<T, R> extends AbstractArithmeticExpression<T, R>
{
    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param left Left child expressions
     * @param right Right child expressions
     */
    public DivideExpression(final String alias, final ExpressionNode<T, R> left, final ExpressionNode<T, R> right)
    {
        super(alias, left, right);
        if (right instanceof ConstantExpression && Objects.equals(0, ((ConstantExpression<?, ?>) right).get()))
        {
            throw QueryParsingException.of(Errors.ARITHMETIC.ZERO_DIVISION);
        }
    }

    /**
     * Performs arithmetic division operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Division result
     */
    @Override
    public R apply(final T value)
    {
        final Object arg1 = evaluateChild(0, value);
        final Object arg2 = evaluateChild(1, value);

        if (arg1 == null || arg2 == null)
        {
            return null;
        }

        if (!(arg1 instanceof Number) || !(arg2 instanceof Number))
        {
            throw QueryParsingException.of(
                Errors.ARITHMETIC.NOT_SUPPORTED,
                "/", arg1, StringUtils.getClassName(arg1), arg2, StringUtils.getClassName(arg2)
            );
        }

        if (Objects.equals(0, arg2))
        {
            throw QueryParsingException.of(Errors.ARITHMETIC.ZERO_DIVISION);
        }

        final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
        final BigDecimal result = new BigDecimal(arg1.toString()).divide(new BigDecimal(arg2.toString()), querySettings.getDecimalDigits(), querySettings.getRoundingMode());
        return NumberConverter.narrow(result);
    }
}
