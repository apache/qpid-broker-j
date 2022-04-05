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
package org.apache.qpid.server.query.engine.parsing.expression.comparison;

import java.math.BigDecimal;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.set.EmptySetExpression;
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * Comparison LESS THAN operation. Evaluates left expression against right expression.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class LessThanExpression<T, R> extends AbstractComparisonExpression<T, Boolean>
{
    /**
     * Argument type validator
     */
    private final FunctionParameterTypePredicate<R> _typeValidator = FunctionParameterTypePredicate.<R>builder()
        .allowDateTimeTypes()
        .allowEnums()
        .allowNumbers()
        .allowComparables()
        .allowStrings()
        .disallowBooleans()
        .build();

    /**
     * Constructor initializes children expression list
     *
     * @param left Left child expression
     * @param right Right child expression
     */
    public LessThanExpression(final ExpressionNode<T, ?> left, final ExpressionNode<T, ?> right)
    {
        super(left, right);
        _operator = "<";
    }

    /**
     * Performs LESS THAN comparison using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Boolean result of value evaluation
     */
    @Override
    @SuppressWarnings("unchecked")
    public Boolean apply(final T value)
    {
        final R left = evaluateChild(0, value);
        final R right = evaluateChild(1, value);

        if (right instanceof EmptySetExpression)
        {
            return Boolean.FALSE;
        }

        if (!_typeValidator.test(left) || !_typeValidator.test(right))
        {
            throw QueryEvaluationException.of(
                Errors.COMPARISON.INAPPLICABLE,
                StringUtils.getClassName(left),
                StringUtils.getClassName(right)
            );
        }

        if (left instanceof Number && right instanceof Number)
        {
            return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString())) < 0;
        }

        if (DateTimeConverter.isDateTime(left) || DateTimeConverter.isDateTime(right))
        {
            return DateTimeConverter.toInstantMapper().apply(left).compareTo(DateTimeConverter.toInstantMapper().apply(right)) < 0;
        }

        if (left instanceof Comparable && right instanceof Comparable)
        {
            final Comparable<R> leftComparable = (Comparable<R>) left;
            return leftComparable.compareTo(right) < 0;
        }
        throw QueryParsingException.of(Errors.COMPARISON.INAPPLICABLE, StringUtils.getClassName(left), StringUtils.getClassName(right));
    }
}
