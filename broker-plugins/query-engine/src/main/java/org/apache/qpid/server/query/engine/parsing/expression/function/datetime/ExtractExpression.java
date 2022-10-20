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
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * The EXTRACT() function extracts a part from a given date.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ExtractExpression<T, R> extends AbstractFunctionExpression<T, Number>
{
    /**
     * Argument type validator
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
    public ExtractExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireParameters(2, args, this);
    }

    /**
     * Performs extract operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting number
     */
    @Override
    public Number apply(final T value)
    {
        final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
        final ChronoUnit datePart = evaluateChild(0, null);
        final R dateTime = evaluateChild(1, value, _dateTimeValidator);
        final Instant instant = DateTimeConverter.toInstantMapper().apply(dateTime);
        final ZonedDateTime zonedDateTime = instant.atZone(querySettings.getZoneId());
        switch (datePart)
        {
            case YEARS:
                return zonedDateTime.getYear();
            case MONTHS:
                return zonedDateTime.getMonthValue();
            case WEEKS:
                return zonedDateTime.get(ChronoField.ALIGNED_WEEK_OF_YEAR);
            case DAYS:
                return zonedDateTime.getDayOfMonth();
            case HOURS:
                return zonedDateTime.get(ChronoField.HOUR_OF_DAY);
            case MINUTES:
                return zonedDateTime.getMinute();
            case SECONDS:
                return zonedDateTime.getSecond();
            case MILLIS:
                return NumberConverter.narrow(zonedDateTime.getLong(ChronoField.MILLI_OF_SECOND));
        }
        throw QueryEvaluationException.of(Errors.FUNCTION.DATEPART_NOT_SUPPORTED, datePart);
    }
}
