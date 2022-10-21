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
package org.apache.qpid.server.query.engine.parsing.factory;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.AvgExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.CountExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.MaxExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.MinExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.SumExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.CurrentTimestampExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.DateAddExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.DateDiffExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.DateExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.ExtractExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.nulls.CoalesceExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.numeric.AbsExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.numeric.RoundExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.numeric.TruncExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.ConcatExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.LeftTrimExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.LengthExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.LowerExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.PositionExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.ReplaceExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.RightTrimExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.SubstringExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.TrimExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.UpperExpression;

/**
 * Factory creating function expressions
 */
// factory follows the single responsibility principle
@SuppressWarnings("java:S1200")
public final class FunctionExpressionFactory
{
    /**
     * Function names
     */
    private enum Functions
    {
        ABS,
        AVG,
        COALESCE,
        CONCAT,
        COUNT,
        CURRENT_TIMESTAMP,
        DATE,
        DATEADD,
        DATEDIFF,
        EXTRACT,
        LEN,
        LENGTH,
        LOWER,
        LTRIM,
        MAX,
        MIN,
        POSITION,
        REPLACE,
        ROUND,
        RTRIM,
        SUBSTR,
        SUBSTRING,
        SUM,
        TRIM,
        TRUNC,
        UPPER
    }

    private static final List<String> FUNCTION_NAMES = Arrays.stream(Functions.values()).map(Enum::name)
        .collect(Collectors.toList());

    /**
     * Mapping between function name and function creator
     */
    private static final Map<Functions, BiFunction<?, ?, ?>> FUNCTIONS = ImmutableMap.<Functions, BiFunction<?, ?, ?>>builder()
        .put(Functions.ABS, absExpression())
        .put(Functions.AVG, avgExpression())
        .put(Functions.COALESCE, coalesceExpression())
        .put(Functions.CONCAT, concatExpression())
        .put(Functions.COUNT, countExpression())
        .put(Functions.CURRENT_TIMESTAMP, currentTimestampExpression())
        .put(Functions.DATE, dateExpression())
        .put(Functions.DATEADD, dateAddExpression())
        .put(Functions.DATEDIFF, dateDiffExpression())
        .put(Functions.EXTRACT, extractExpression())
        .put(Functions.LEN, lenExpression())
        .put(Functions.LENGTH, lenExpression())
        .put(Functions.LOWER, lowerExpression())
        .put(Functions.LTRIM, ltrim())
        .put(Functions.MAX, maxExpression())
        .put(Functions.MIN, minExpression())
        .put(Functions.POSITION, positionExpression())
        .put(Functions.REPLACE, replaceExpression())
        .put(Functions.ROUND, roundExpression())
        .put(Functions.RTRIM, rtrimExpression())
        .put(Functions.SUBSTR, substringExpression())
        .put(Functions.SUBSTRING, substringExpression())
        .put(Functions.SUM, sumExpression())
        .put(Functions.TRIM, trimExpression())
        .put(Functions.TRUNC, truncExpression())
        .put(Functions.UPPER, upperExpression())
        .build();

    /**
     * Shouldn't be instantiated directly
     */
    private FunctionExpressionFactory()
    {

    }

    /**
     * Creates ABS function
     *
     * @param <T> Input parameter type
     *
     * @return AbsExpression instance
     */
    private static <T> BiFunction<String, List<ExpressionNode<T, ?>>, AbsExpression<T>> absExpression()
    {
        return AbsExpression::new;
    }

    /**
     * Creates AVG function
     *
     * @param <T> Input parameter type
     *
     * @return AvgExpression instance
     */
    private static <T> BiFunction<String, List<ExpressionNode<T, ?>>, AvgExpression<T>> avgExpression()
    {
        return AvgExpression::new;
    }

    /**
     * Creates COALESCE function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return CoalesceExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, CoalesceExpression<T, R>> coalesceExpression()
    {
        return CoalesceExpression::new;
    }

    /**
     * Creates CONCAT function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return ConcatExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, ConcatExpression<T, R>> concatExpression()
    {
        return ConcatExpression::new;
    }

    /**
     * Creates COUNT function
     *
     * @param <T> Input parameter type
     *
     * @return CountExpression instance
     */
    private static <T> BiFunction<String, List<ExpressionNode<T, ?>>, CountExpression<T>> countExpression()
    {
        return CountExpression::new;
    }

    /**
     * Creates CURRENT_TIMESTAMP function
     *
     * @param <T> Input parameter type
     *
     * @return CurrentTimestampExpression instance
     */
    private static <T> BiFunction<String, List<ExpressionNode<T, ?>>, CurrentTimestampExpression<T>> currentTimestampExpression()
    {
        return CurrentTimestampExpression::new;
    }

    /**
     * Creates DATE function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return DateExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, DateExpression<T, R>> dateExpression()
    {
        return DateExpression::new;
    }

    /**
     * Creates DATEADD function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return DateAddExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, DateAddExpression<T, R>> dateAddExpression()
    {
        return DateAddExpression::new;
    }

    /**
     * Creates DATEDIFF function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return DateDiffExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, DateDiffExpression<T, R>> dateDiffExpression()
    {
        return DateDiffExpression::new;
    }

    /**
     * Creates EXTRACT function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return ExtractExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, ExtractExpression<T, R>> extractExpression()
    {
        return ExtractExpression::new;
    }

    /**
     * Creates LEN / LENGTH function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return LengthExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, LengthExpression<T, R>> lenExpression()
    {
        return LengthExpression::new;
    }

    /**
     * Creates LOWER function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return LowerExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, LowerExpression<T, R>> lowerExpression()
    {
        return LowerExpression::new;
    }

    /**
     * Creates LTRIM function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return LeftTrimExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, LeftTrimExpression<T, R>> ltrim()
    {
        return LeftTrimExpression::new;
    }

    /**
     * Creates MAX function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return MaxExpression instance
     */
    private static <T, R extends Comparable<R>> BiFunction<String, List<ExpressionNode<T, ?>>, MaxExpression<T, R>> maxExpression()
    {
        return MaxExpression::new;
    }

    /**
     * Creates MIN function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return MinExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, MinExpression<T, R>> minExpression()
    {
        return MinExpression::new;
    }

    /**
     * Creates POSITION function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return PositionExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, PositionExpression<T,R>> positionExpression()
    {
        return PositionExpression::new;
    }

    /**
     * Creates REPLACE function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return ReplaceExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, ReplaceExpression<T,R>> replaceExpression()
    {
        return ReplaceExpression::new;
    }

    /**
     * Creates ROUND function
     *
     * @param <T> Input parameter type
     *
     * @return RoundExpression instance
     */
    private static <T> BiFunction<String, List<ExpressionNode<T, ?>>, RoundExpression<T>> roundExpression()
    {
        return RoundExpression::new;
    }

    /**
     * Creates RTRIM function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return RightTrimExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, RightTrimExpression<T,R>> rtrimExpression()
    {
        return RightTrimExpression::new;
    }

    /**
     * Creates SUBSTR / SUBSTRING function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return SubstringExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, SubstringExpression<T,R>> substringExpression()
    {
        return SubstringExpression::new;
    }

    /**
     * Creates SUM function
     *
     * @param <T> Input parameter type
     *
     * @return SumExpression instance
     */
    private static <T> BiFunction<String, List<ExpressionNode<T, ?>>, SumExpression<T>> sumExpression()
    {
        return SumExpression::new;
    }

    /**
     * Creates TRIM function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return TrimExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, TrimExpression<T,R>> trimExpression()
    {
        return TrimExpression::new;
    }

    /**
     * Creates TRUNC function
     *
     * @param <T> Input parameter type
     *
     * @return TruncExpression instance
     */
    private static <T> BiFunction<String, List<ExpressionNode<T, ?>>, TruncExpression<T>> truncExpression()
    {
        return TruncExpression::new;
    }

    /**
     * Creates UPPER function
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return UpperExpression instance
     */
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, UpperExpression<T,R>> upperExpression()
    {
        return UpperExpression::new;
    }

    /**
     * Retrieves function creator by name
     *
     * @param key Function name enum
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return BiFunction taking function name and arguments as parameters
     */
    @SuppressWarnings("unchecked")
    private static <T, R> BiFunction<String, List<ExpressionNode<T, ?>>, ExpressionNode<T, R>> getFunction(
        final Functions key
    )
    {
        return (BiFunction<String, List<ExpressionNode<T, ?>>, ExpressionNode<T, R>>) FUNCTIONS.get(key);
    }

    /**
     * Creates function expression
     *
     * @param alias Function alias
     * @param name Function name
     * @param args List of function arguments
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Function expression
     */
    public static <T, R> ExpressionNode<T, R> createFunction(
        final String alias,
        final String name,
        final List<ExpressionNode<T, ?>> args
    )
    {
        Objects.requireNonNull(name, Errors.VALIDATION.FUNCTION_NAME_NULL);
        Objects.requireNonNull(args, Errors.VALIDATION.FUNCTION_ARGS_NULL);
        final String normalizedName = name.toUpperCase(Locale.US);
        if(!FUNCTION_NAMES.contains(normalizedName))
        {
            throw QueryParsingException.of(Errors.FUNCTION.NOT_FOUND, name);
        }
        final Functions function = Functions.valueOf(normalizedName);
        return FunctionExpressionFactory.<T,R>getFunction(function).apply(alias, args);
    }
}
