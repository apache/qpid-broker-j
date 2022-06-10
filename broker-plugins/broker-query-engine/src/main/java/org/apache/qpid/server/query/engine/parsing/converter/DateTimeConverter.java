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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.function.Function;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.utils.StringUtils;

/**
 * Utility class for datetime conversions
 */
public final class DateTimeConverter
{
    /**
     * Shouldn't be instantiated directly
     */
    private DateTimeConverter()
    {

    }

    /**
     * Retrieves current evaluation context
     *
     * @return EvaluationContext instance
     */
    private static EvaluationContext ctx()
    {
        return EvaluationContextHolder.getEvaluationContext();
    }

    /**
     * Retrieves current zone id
     *
     * @return ZoneId instance
     */
    private static ZoneId zoneId()
    {
        final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
        return querySettings.getZoneId();
    }

    /**
     * Retrieves current date pattern
     *
     * @return Date pattern
     */
    private static String datePattern()
    {
        final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
        return querySettings.getDatePattern();
    }

    /**
     * Retrieves current datetime pattern
     *
     * @return Datetime pattern
     */
    private static String dateTimePattern()
    {
        final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
        return querySettings.getDateTimePattern();
    }

    /**
     * Creates datetime formatter for dates
     *
     * @return DateTimeFormatter instance
     */
    private static DateTimeFormatter dateFormatter()
    {
        return new DateTimeFormatterBuilder().appendPattern(datePattern())
            .toFormatter().withZone(zoneId()).withResolverStyle(ResolverStyle.STRICT);
    }

    /**
     * Creates datetime formatter for datetime
     *
     * @return DateTimeFormatter instance
     */
    private static DateTimeFormatter dateTimeFormatter()
    {
        final DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().appendPattern(dateTimePattern());
        if (!ctx().contains(EvaluationContext.QUERY_DATETIME_PATTERN_OVERRIDEN))
        {
            builder.appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true);
        }
        return builder.toFormatter().withZone(zoneId()).withResolverStyle(ResolverStyle.STRICT);
    }

    /**
     * Converts date to string
     *
     * @param date Date instance
     *
     * @return String representation of date
     */
    public static String toString(final Date date)
    {
        if (date == null)
        {
            return null;
        }
        return dateTimeFormatter().format(date.toInstant());
    }

    /**
     * Converts instant to string
     *
     * @param instant Instant instance
     *
     * @return String representation of instant
     */
    public static String toString(final Instant instant)
    {
        if (instant == null)
        {
            return null;
        }
        return dateTimeFormatter().format(instant);
    }

    /**
     * Converts local date to string
     *
     * @param localDate LocalDate instance
     *
     * @return String representation of localDate
     */
    public static String toString(final LocalDate localDate)
    {
        if (localDate == null)
        {
            return null;
        }
        return dateFormatter().format(localDate);
    }

    /**
     * Converts local datetime to string
     *
     * @param dateTime LocalDateTime instance
     *
     * @return String representation of local dateTime
     */
    public static String toString(final LocalDateTime dateTime)
    {
        if (dateTime == null)
        {
            return null;
        }
        return dateTimeFormatter().format(dateTime);
    }

    /**
     * Converts date to instant
     *
     * @param date Date instance
     *
     * @return Instant instance
     */
    public static Instant toInstant(final Date date)
    {
        if (date == null)
        {
            return null;
        }
        return date.toInstant();
    }

    /**
     * Converts local date to instant
     *
     * @param localDate LocalDate instance
     *
     * @return Instant instance
     */
    public static Instant toInstant(final LocalDate localDate)
    {
        if (localDate == null)
        {
            return null;
        }
        return localDate.atStartOfDay(zoneId()).toInstant();
    }

    /**
     * Converts local datetime to instant
     *
     * @param localDateTime LocalDateTime instance
     *
     * @return Instant instance
     */
    public static Instant toInstant(final LocalDateTime localDateTime)
    {
        if (localDateTime == null)
        {
            return null;
        }
        return localDateTime.atZone(zoneId()).toInstant();
    }

    /**
     * Checks whether value provided belongs to datetime types or not
     *
     * @param value Value to check
     *
     * @param <R> Return parameter type
     *
     * @return True or false
     */
    public static <R> boolean isDateTime(final R value)
    {
        return value instanceof Date || value instanceof Instant || value instanceof LocalDate || value instanceof LocalDateTime;
    }

    /**
     * Returns function mapping datetime to string
     *
     * @param <T> Input parameter type
     *
     * @return Mapping function
     */
    public static <T> Function<T, String> toStringMapper()
    {
        return object -> {
            if (object instanceof Date)
            {
                return toString((Date) object);
            }
            if (object instanceof Instant)
            {
                return toString((Instant) object);
            }
            if (object instanceof LocalDate)
            {
                return toString((LocalDate) object);
            }
            if (object instanceof LocalDateTime)
            {
                return toString((LocalDateTime) object);
            }
            return String.valueOf(object);
        };
    }

    /**
     * Returns function mapping datetime to instant
     *
     * @param <T> Input parameter type
     *
     * @return Mapping function
     */
    public static <T> Function<T, Instant> toInstantMapper()
    {
        return object -> {
            try
            {
                if (object == null)
                {
                    return null;
                }
                if (object instanceof Date)
                {
                    return toInstant((Date) object);
                }
                if (object instanceof Instant)
                {
                    return (Instant) object;
                }
                if (object instanceof LocalDate)
                {
                    return toInstant((LocalDate) object);
                }
                if (object instanceof LocalDateTime)
                {
                    return toInstant((LocalDateTime) object);
                }
                if (object instanceof String)
                {
                    try
                    {
                        return LocalDateTime.parse((String) object, dateTimeFormatter()).atZone(zoneId()).toInstant();
                    }
                    catch (DateTimeParseException e)
                    {
                        try
                        {
                            return LocalDate.parse((String) object, dateFormatter()).atStartOfDay().atZone(zoneId()).toInstant();
                        }
                        catch (DateTimeParseException e1)
                        {
                            throw e;
                        }
                    }
                }
                throw QueryEvaluationException.of(Errors.CONVERSION.FAILED, StringUtils.getClassName(object), "instant");
            }
            catch (DateTimeParseException e)
            {
                throw QueryEvaluationException.of(e.getMessage());
            }
        };
    }

    /**
     * Converts datetime value to long (milliseconds)
     *
     * @param date Datetime value to be converted
     *
     * @param <R> Return parameter type
     *
     * @return Long
     */
    public static <R> Long toLong(final R date)
    {
        if (date == null)
        {
            return null;
        }
        if (date instanceof Date)
        {
            return ((Date) date).getTime();
        }
        if (date instanceof Instant)
        {
            return ((Instant) date).toEpochMilli();
        }
        throw QueryEvaluationException.of(Errors.CONVERSION.FAILED, StringUtils.getClassName(date), "long");
    }

    /**
     * Returns DateTimeFormatter used
     *
     * @return DateTimeFormatter instance
     */
    public static DateTimeFormatter getFormatter()
    {
        return dateTimeFormatter();
    }
}
