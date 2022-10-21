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
package org.apache.qpid.server.query.engine.parsing.expression.function.string;

import java.util.List;
import java.util.Optional;

import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * POSITION takes a search pattern and a source string as parameters and returns the position of the first occurrence
 * of a pattern in a source string. If the pattern is not found within the source string, this function returns 0.
 * Optionally takes third integer parameter, defining from which position search should be started. Third parameter
 * should be an integer greater than 0. If source string is null, returns zero.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class PositionExpression<T, R> extends AbstractFunctionExpression<T, Integer>
{
    /**
     * Argument type validator
     */
    private final FunctionParameterTypePredicate<R> _arg1Validator = FunctionParameterTypePredicate.<R>builder()
        .allowBooleans()
        .allowEnums()
        .allowDateTimeTypes()
        .allowNumbers()
        .allowStrings()
        .build();

    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public PositionExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireMinParameters(2, args, this);
        FunctionParametersValidator.requireMaxParameters(3, args, this);
    }

    /**
     * Performs position operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting string
     */
    @Override
    public Integer apply(final T value)
    {
        // evaluate function arguments
        final R searchPattern = evaluateChild(0, value, _arg1Validator);
        final R source = evaluateChild(1, value, _arg1Validator);
        final Expression<T, R> searchFromExpression = getChild(2);
        final R searchFrom;
        if (searchFromExpression == null)
        {
            searchFrom = null;
        }
        else
        {
            searchFrom = Optional.ofNullable(searchFromExpression.apply(value))
                .orElseThrow(() -> QueryEvaluationException.functionParameterLessThanOne(_functionName, 3));
        }

        // if source string is null, return zero
        if (source == null)
        {
            return 0;
        }

        // convert first parameter to string
        final String convertedSubstring = DateTimeConverter.isDateTime(searchPattern)
            ? DateTimeConverter.toStringMapper().apply(searchPattern)
            : String.valueOf(searchPattern);

        if (convertedSubstring.isEmpty())
        {
            throw QueryEvaluationException.emptyFunctionParameter(_functionName, 1);
        }

        // convert second parameter to string
        final String convertedSource = DateTimeConverter.isDateTime(source)
            ? DateTimeConverter.toStringMapper().apply(source)
            : String.valueOf(source);

        // check for searchFromExpression parameter and calculate position considering it
        if (searchFrom != null)
        {
            if (!(searchFrom instanceof Integer) || ((Integer) searchFrom) < 1)
            {
                throw QueryEvaluationException.functionParameterLessThanOne(_functionName, 3);
            }
            return convertedSource.indexOf(convertedSubstring, (Integer) searchFrom - 1) + 1;
        }

        // calculate position
        return convertedSource.indexOf(convertedSubstring) + 1;
    }
}
