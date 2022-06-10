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
import java.util.Objects;

import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * REPLACE takes a source parameter, a pattern parameter and a replacement parameter. Replaces all occurrences of
 * pattern in source string with the replacement strings. If source string is NULL, returns NULL.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ReplaceExpression<T, R> extends AbstractFunctionExpression<T, String>
{
    /**
     * Argument type validator
     */
    private final FunctionParameterTypePredicate<R> _typeValidator = FunctionParameterTypePredicate.<R>builder()
        .allowNulls()
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
    public ReplaceExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireParameters(3, args, this);
    }

    /**
     * Performs replace operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting string
     */
    @Override
    public String apply(final T value)
    {
        // evaluate function arguments
        final R source = evaluateChild(0, value, _typeValidator);
        final R pattern = evaluateChild(1, value, _typeValidator);
        final R replacement = evaluateChild(2, value, _typeValidator);

        // if source is null, return null
        if (source == null)
        {
            return null;
        }

        // convert source to string
        final String convertedSource = DateTimeConverter.isDateTime(source)
            ? DateTimeConverter.toStringMapper().apply(source)
            : String.valueOf(source);

        if (pattern == null || Objects.equals("", pattern))
        {
            return convertedSource;
        }

        // convert pattern and replacement to string
        final String convertedPattern = DateTimeConverter.isDateTime(pattern)
                ? DateTimeConverter.toStringMapper().apply(pattern)
                : String.valueOf(pattern);
        final String convertedReplacement = replacement == null ? "" : DateTimeConverter.isDateTime(replacement)
                ? DateTimeConverter.toStringMapper().apply(replacement)
                : String.valueOf(replacement);

        // perform replace on the string parameter
        return convertedSource.replace(convertedPattern, convertedReplacement);
    }
}
