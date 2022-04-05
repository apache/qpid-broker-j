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

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * SUBSTRING takes a source parameter, a start index parameter and optional length parameter. Returns substring
 * of a source string from the start index to the end or using the length parameter. If source string is NULL,
 * return NULL. If start index is negative, function extracts from the end of the string.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class SubstringExpression<T, R> extends AbstractFunctionExpression<T, String>
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
    public SubstringExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireMinParameters(2, args, this);
        FunctionParametersValidator.requireMaxParameters(3, args, this);
    }

    /**
     * Performs substring operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting string
     */
    @Override
    public String apply(final T value)
    {
        // evaluate source argument
        final R source = evaluateChild(0, value, _typeValidator);

        // if source is null, return null
        if (source == null)
        {
            return null;
        }

        // convert source to string
        final String arg = DateTimeConverter.isDateTime(source)
            ? DateTimeConverter.toStringMapper().apply(source)
            : String.valueOf(source);

        // evaluate start index
        int startIndex = getRequiredParameter(1, value, Integer.class, Errors.FUNCTION.PARAMETER_NOT_INTEGER);

        if (startIndex == 0)
        {
            startIndex = 1;
        }

        // evaluate length
        final Optional<Integer> optEndIndex = getOptionalParameter(2, value, Integer.class, Errors.FUNCTION.PARAMETER_NOT_INTEGER);

        if (!optEndIndex.isPresent())
        {
            return arg.substring(startIndex - 1);
        }

        // perform substring on the source parameter
        if (startIndex < 0)
        {
            startIndex = arg.length() + startIndex + 1;
        }
        if (optEndIndex.get() <= 0)
        {
            return "";
        }
        final int endIndex = Math.min(arg.length(), startIndex + (optEndIndex.get() - 1));
        return arg.substring(startIndex - 1, endIndex);
    }
}
