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
package org.apache.qpid.server.query.engine.parsing.expression.function.datetime;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * The DATEADD() function adds a time/date interval to a date and returns the date.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class DateAddExpression<T, R> extends AbstractFunctionExpression<T, Instant>
{
    /**
     * Number argument type validator
     */
    private final FunctionParameterTypePredicate<Number> _numberValidator = FunctionParameterTypePredicate.<Number>builder()
        .allowNumbers()
        .build();

    /**
     * Datetime argument type validator
     */
    private final FunctionParameterTypePredicate<R> _dateTimeValidator = FunctionParameterTypePredicate.<R>builder()
        .allowDateTimeTypes()
        .allowStrings()
        .build();

    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public DateAddExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireParameters(3, args, this);
    }

    /**
     * Performs dateadd operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting datetime
     */
    @Override
    public Instant apply(final T value)
    {
        final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
        final ChronoUnit datePart = evaluateChild(0, null);
        final Number amountToAdd = evaluateChild(1, null, _numberValidator);
        final R dateTime = evaluateChild(2, value, _dateTimeValidator);
        final Instant instant = DateTimeConverter.toInstantMapper().apply(dateTime);
        return instant.atZone(querySettings.getZoneId()).plus(amountToAdd.longValue(), datePart).toInstant();
    }
}
