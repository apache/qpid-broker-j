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
package org.apache.qpid.server.query.engine.parsing.converter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;

/**
 * Utility class for numeric conversions
 */
public final class NumberConverter
{
    /**
     * Conversion rules
     */
    private static final Map<Class<?>, Map<Class<?>, Function<Object, ?>>> CONVERSIONS = ImmutableMap.<Class<?>, Map<Class<?>, Function<Object, ?>>>builder()
        .put(Double.class, ImmutableMap.<Class<?>, Function<Object, ?>>builder()
            .put(BigDecimal.class, arg -> ((BigDecimal) arg).doubleValue())
            .put(Byte.class, arg -> ((Byte) arg).doubleValue())
            .put(Double.class, arg -> arg)
            .put(Float.class, arg -> ((Float) arg).doubleValue())
            .put(Integer.class, arg -> ((Integer) arg).doubleValue())
            .put(Long.class, arg -> ((Long) arg).doubleValue())
            .put(Number.class, arg -> ((Number) arg).doubleValue())
            .put(Short.class, arg -> ((Short) arg).doubleValue())
            .put(String.class, arg -> Double.parseDouble((String)arg))
            .build()
        )
        .put(Long.class, ImmutableMap.<Class<?>, Function<Object, ?>>builder()
            .put(BigDecimal.class, arg -> ((BigDecimal) arg).longValue())
            .put(Byte.class, arg -> ((Byte) arg).longValue())
            .put(Double.class, arg -> ((Double) arg).longValue())
            .put(Float.class, arg -> ((Float) arg).longValue())
            .put(Integer.class, arg -> ((Integer) arg).longValue())
            .put(Long.class, arg -> arg)
            .put(Number.class, arg -> ((Number) arg).longValue())
            .put(Short.class, arg -> ((Short) arg).longValue())
            .put(String.class, arg -> Long.parseLong((String)arg))
            .build()
        ).build();

    /**
     * Shouldn't be instantiated directly
     */
    private NumberConverter()
    {

    }

    /**
     * Retrieves current query settings
     *
     * @return QuerySettings instance
     */
    private static QuerySettings querySettings()
    {
        return EvaluationContextHolder.getEvaluationContext().get(EvaluationContext.QUERY_SETTINGS);
    }

    /**
     * Retrieves current decimal digits setting
     *
     * @return Decimal digits setting
     */
    private static int decimalDigits()
    {
        return querySettings().getDecimalDigits();
    }

    /**
     * Retrieves current max big decimal value setting
     *
     * @return Max big decimal value setting
     */
    private static BigDecimal maxBigDecimalValue()
    {
        return querySettings().getMaxBigDecimalValue();
    }

    /**
     * Retrieves current rounding mode setting
     *
     * @return Rounding mode setting
     */
    private static RoundingMode roundingMode()
    {
        return querySettings().getRoundingMode();
    }

    /**
     * Retrieves conversion function using target type and object supplied
     *
     * @param type Target type
     * @param value Object to convert
     *
     * @return Conversion function
     */
    private static Function<Object, ?> getConversion(final Class<?> type, final Object value)
    {
        final Map<Class<?>, Function<Object, ?>> availableConversions = CONVERSIONS.get(type);
        if (availableConversions == null)
        {
            throw QueryParsingException.of(Errors.CONVERSION.NOT_SUPPORTED, value, StringUtils.getClassName(value));
        }
        final Function<Object, ?> function = availableConversions.get(value.getClass());
        if (function == null)
        {
            throw QueryParsingException.of(Errors.CONVERSION.NOT_SUPPORTED, value, StringUtils.getClassName(value));
        }
        return function;
    }

    /**
     * Converts object to long
     *
     * @param value Object to convert
     *
     * @return Long instance
     */
    public static Long toLong(Object value)
    {
        if (value == null)
        {
            return null;
        }
        return (Long) getConversion(Long.class, value).apply(value);
    }

    /**
     * Converts object to double
     *
     * @param value Object to convert
     *
     * @return Double instance
     */
    public static Double toDouble(Object value)
    {
        if (value == null)
        {
            return null;
        }
        return (Double) getConversion(Double.class, value).apply(value);
    }

    /**
     * Converts BigDecimal to the strictest numeric type available in following order: Integer, Long, Double, BigDecimal
     *
     * @param bigDecimal BigDecimal instance
     *
     * @param <R> Return parameter value
     *
     * @return Converted value
     */
    @SuppressWarnings("unchecked")
    public static <R> R narrow(final BigDecimal bigDecimal)
    {
        if (bigDecimal == null)
        {
            return null;
        }

        if (bigDecimal.compareTo(maxBigDecimalValue()) >= 0 || bigDecimal.compareTo(maxBigDecimalValue().negate()) <= 0)
        {
            throw QueryParsingException.of(Errors.CONVERSION.MAX_VALUE_REACHED, bigDecimal);
        }

        if (bigDecimal.doubleValue() == (int) bigDecimal.doubleValue())
        {
            return (R) (Number) bigDecimal.intValue();
        }

        if (bigDecimal.doubleValue() == (long) bigDecimal.doubleValue() && bigDecimal.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0 && bigDecimal.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0)
        {
            return (R) (Number) bigDecimal.longValue();
        }

        if (bigDecimal.scale() != 0 && bigDecimal.compareTo(BigDecimal.valueOf(Double.MAX_VALUE)) <= 0 && bigDecimal.compareTo(BigDecimal.valueOf(-Double.MAX_VALUE)) >= 0)
        {
            return (R) (Number) bigDecimal.setScale(decimalDigits(), roundingMode()).doubleValue();
        }

        return (R) bigDecimal;
    }

    /**
     * Converts Number to the strictest numeric type available in following order: Integer, Long, Double, BigDecimal
     *
     * @param number Number instance
     *
     *
     * @return Converted value
     */
    public static Number narrow(final Number number)
    {
        if (number == null)
        {
            return null;
        }
        return narrow(new BigDecimal(number.toString()));
    }

    /**
     * Checks if string is a number
     *
     * @param source String to check
     *
     * @return True or false
     */
    public static boolean isNumber(final String source)
    {
        try
        {
            Integer.parseInt(source);
            return true;
        }
        catch (NumberFormatException e)
        {
            return false;
        }
    }
}
