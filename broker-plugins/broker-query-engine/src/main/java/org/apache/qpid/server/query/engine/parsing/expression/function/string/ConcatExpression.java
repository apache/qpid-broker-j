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
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * CONCAT takes a variable number of arguments and concatenates them into a single string. It requires a minimum of
 * one input value, otherwise CONCAT will raise an error. CONCAT implicitly converts all arguments to string types
 * before concatenation. The implicit conversion to strings follows the existing rules for data type conversions.
 * If any argument is NULL, CONCAT returns an empty string.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ConcatExpression<T, R> extends AbstractFunctionExpression<T, String>
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
    public ConcatExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireMinParameters(1, args, this);
    }

    /**
     * Performs concatenation operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting string
     */
    @Override
    @SuppressWarnings("unchecked")
    public String apply(final T value)
    {
        // evaluate parameters
        final List<R> args = getChildren().stream().map(expression -> (R) expression.apply(value)).collect(Collectors.toList());

        // if any argument is null, return empty string
        if (args.stream().anyMatch(Objects::isNull))
        {
            return "";
        }

        // check for argument types
        final List<?> invalidArgs = args.stream().filter(_typeValidator.negate()).map(StringUtils::getClassName).collect(Collectors.toList());
        if (!invalidArgs.isEmpty())
        {
            throw QueryParsingException.of(Errors.FUNCTION.PARAMETERS_INVALID, _functionName, invalidArgs);
        }

        // concatenate strings
        return args.stream().map(DateTimeConverter.toStringMapper()).map(String::valueOf).collect(Collectors.joining(""));
    }
}
