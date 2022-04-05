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
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * TRIM takes a string parameter and removes both leading and trailing spaces from it. The implicit conversion to
 * strings follows the existing rules for data type conversions. If any argument is NULL, TRIM returns NULL.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class TrimExpression<T, R> extends AbstractFunctionExpression<T, String>
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
    public TrimExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireMinParameters(1, args, this);
        FunctionParametersValidator.requireMaxParameters(2, args, this);
    }

    /**
     * Performs trim operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting string
     */
    @Override
    public String apply(final T value)
    {
        // evaluate function argument
        final R arg = evaluateChild(0, value, _typeValidator);

        // if argument is null, return null
        if (arg == null)
        {
            return null;
        }

        // evaluate characters to strip
        final Optional<String> optionalChars = getOptionalParameter(
            1, value, String.class, Errors.FUNCTION.PARAMETER_NOT_STRING
        );

        // convert parameter to string
        String converted = DateTimeConverter.isDateTime(arg)
            ? DateTimeConverter.toStringMapper().apply(arg)
            : String.valueOf(arg);

        // perform trim on the string parameter
        converted = !optionalChars.isPresent()
            ? StringUtils.stripStart(converted,null)
            : StringUtils.stripStart(converted, optionalChars.get());
        return !optionalChars.isPresent()
            ? StringUtils.stripEnd(converted, null)
            : StringUtils.stripEnd(converted, optionalChars.get());
    }
}
