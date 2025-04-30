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

import java.util.regex.Pattern;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * Comparison LIKE operation. Evaluates left expression against a pattern.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class LikeExpression<T, R> extends AbstractComparisonExpression<T, Boolean>
{
    /** Repeated wildcards pattern */
    private static final Pattern REPEATED_WILDCARDS = Pattern.compile("%{2,}");

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

    /** Merges a sequence of '%' into a single '%' */
    private static <X> ExpressionNode<X, ?> toConstantExpression(String source)
    {
        if (source.contains("%%"))
        {
            source = REPEATED_WILDCARDS.matcher(source).replaceAll("%");
        }
        return new ConstantExpression<>(source);
    }

    /**
     * Constructor initializes children expression list
     *
     * @param left Left child expression
     * @param source Source string
     * @param escape Escape character
     */
    public LikeExpression(final ExpressionNode<T, R> left, final String source, final String escape)
    {
        super("", left, toConstantExpression(source), new ConstantExpression<>(escape));
    }

    /**
     * Performs LIKE comparison using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Boolean result of value evaluation
     */
    @Override
    public Boolean apply(final T value)
    {
        final R arg = evaluateChild(0, value);
        if (arg == null)
        {
            return Boolean.FALSE;
        }
        if (!_typeValidator.test(arg))
        {
            throw QueryParsingException.of(Errors.FUNCTION.PARAMETER_INVALID , arg, StringUtils.getClassName(arg));
        }
        final String convertedPattern = evaluateChild(1, null);
        final String convertedValue = String.valueOf(arg);
        final String patternSource = evaluateChild(2, null);
        final Pattern pattern = sqlPatternToRegex(convertedPattern, patternSource);
        return pattern.matcher(convertedValue).matches();
    }

    /**
     * Converts SQL pattern syntax to java regular expression
     *
     * @param pattern SQL pattern
     * @param escape Escape character
     *
     * @return Regex pattern
     */
    private Pattern sqlPatternToRegex(final String pattern, final String escape)
    {
        if (pattern == null)
        {
            throw new IllegalArgumentException("Null pattern");
        }

        if (pattern.length() == 0)
        {
            throw new IllegalArgumentException("Empty pattern");
        }

        final Character escapeChar = escape == null ? null : escape.charAt(0);
        final StringBuilder buffer = new StringBuilder();
        buffer.append("^");
        for (int j = 0; j < pattern.length(); j++)
        {
            final char nextChar = pattern.charAt(j);
            if (escapeChar != null && escapeChar == nextChar)
            {
                j++;
                if (j >= pattern.length())
                {
                    break;
                }
                final char t = pattern.charAt(j);
                buffer.append("\\");
                buffer.append(t);
            }
            else if (nextChar == '%')
            {
                buffer.append('.').append('*');
            }
            else if (nextChar == '?')
            {
                buffer.append('.');
            }
            else if (nextChar == '.'
                     || nextChar == '/'
                     || nextChar == '$'
                     || nextChar == '^')
            {
                buffer.append('\\').append(nextChar);
            }
            else
            {
                buffer.append(nextChar);
            }
        }
        buffer.append("$");
        return Pattern.compile(buffer.toString());
    }
}
