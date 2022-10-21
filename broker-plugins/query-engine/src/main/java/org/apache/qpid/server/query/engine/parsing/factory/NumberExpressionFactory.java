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

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;

/**
 * Factory creating numeric expressions
 */
public final class NumberExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private NumberExpressionFactory()
    {

    }

    /**
     * Function providing executing callback or fallback conversion
     *
     * @param callback Primary conversion mechanism
     * @param fallback Fallback conversion mechanism
     *
     * @return Successful result of either a callback or fallback
     */
    private static Function<String, Number> fallbackWrapper(
        final Supplier<Number> callback,
        final Supplier<Number> fallback
    )
    {
        return arg ->
        {
            try
            {
                return callback.get();
            }
            catch (NumberFormatException e)
            {
                return fallback.get();
            }
        };
    }

    /**
     * Creates number from a decimal token
     *
     * @param source String source
     *
     * @return Number instance
     */
    public static Number fromDecimal(final String source)
    {
        final Number number = Optional.ofNullable(source)
            .map(text -> text.toUpperCase(Locale.US).endsWith("L") ? text.substring(0, text.length() - 1) : text)
            .map(text -> fallbackWrapper(() -> Long.valueOf(text), () -> new BigDecimal(text)))
            .map(f -> f.apply(source))
            .map(value -> value.longValue() == (int) value.longValue() ? value.intValue() : value)
            .orElse(null);
        return NumberConverter.narrow(number);
    }

    /**
     * Creates number from a hex token
     *
     * @param source String source
     *
     * @return Number instance
     */
    public static Number fromHex(final String source)
    {
        final Number number = Optional.ofNullable(source)
            .map(text -> (Number) Long.parseLong(text.substring(2), 16))
            .map(value -> value.longValue() == (int) value.longValue() ? value.intValue() : value)
            .orElse(null);
        return NumberConverter.narrow(number);
    }

    /**
     * Creates number from an octal token
     *
     * @param source String source
     *
     * @return Number instance
     */
    public static Number fromOctal(final String source)
    {
        final Number number = Optional.ofNullable(source)
            .map(text -> (Number) Long.parseLong(text, 8))
            .orElse(null);
        return NumberConverter.narrow(number);
    }

    /**
     * Creates number from a double token
     *
     * @param source String source
     *
     * @return Number instance
     */
    public static Number fromDouble(final String source)
    {
        final Number number = Optional.ofNullable(source)
            .map(Double::parseDouble)
            .orElse(null);
        return NumberConverter.narrow(number);
    }
}
