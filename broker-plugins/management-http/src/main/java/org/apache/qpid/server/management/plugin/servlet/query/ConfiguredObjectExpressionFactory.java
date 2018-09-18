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
package org.apache.qpid.server.management.plugin.servlet.query;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;

import org.apache.qpid.server.filter.Expression;
import org.apache.qpid.server.filter.NamedExpression;
import org.apache.qpid.server.model.ConfiguredObject;

public class ConfiguredObjectExpressionFactory
{

    private static final String PARENT_ATTR = "$parent";
    private static Set<String> SPECIAL_ATTRIBUTES = new HashSet<>(Arrays.asList(PARENT_ATTR));
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final DatatypeFactory DATATYPE_FACTORY;

    private static final DateTimeFormatter ISO_DATE_TIME_FORMAT =  new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalStart()
            .appendOffsetId()
            .optionalStart()
            .appendLiteral('[')
            .parseCaseSensitive()
            .appendZoneRegionId()
            .appendLiteral(']')
            .toFormatter()
            .withChronology(IsoChronology.INSTANCE);

    static
    {
        try
        {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
        }
        catch (DatatypeConfigurationException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    enum FilterFunction
    {
        CONCAT {
            @Override
            ConfiguredObjectExpression asExpression(final List<Expression> args)
            {
                return new ConfiguredObjectExpression()
                {
                    @Override
                    public Object evaluate(final ConfiguredObject<?> object)
                    {
                        StringBuilder buf = new StringBuilder();
                        for(Expression expr : args)
                        {
                            buf.append(expr.evaluate(object));
                        }
                        return buf.toString();
                    }
                };
            }
        },

        NOW {
            @Override
            ConfiguredObjectExpression asExpression(final List<Expression> args)
            {
                if (args != null && !args.isEmpty())
                {
                    throw new IllegalArgumentException(NOW.name() + " does not accept arguments.");
                }
                return new ConfiguredObjectExpression()
                {
                    @Override
                    public Object evaluate(final ConfiguredObject<?> object)
                    {
                        return new Date();
                    }
                };
            }
        },

        TO_DATE {
            @Override
            ConfiguredObjectExpression asExpression(final List<Expression> args)
            {
                if (args == null || args.size() != 1)
                {
                    throw new IllegalArgumentException(TO_DATE.name() + " requires a single argument.");
                }

                return new ConfiguredObjectExpression()
                {
                    @Override
                    public Object evaluate(final ConfiguredObject<?> object)
                    {
                        Object dateTime = args.get(0).evaluate(object);
                        if (!(dateTime instanceof String))
                        {
                            throw new IllegalArgumentException(TO_DATE.name() + " requires a string argument, not a " + dateTime.getClass());
                        }
                        try
                        {

                            return DateTimeFormatter.ISO_ZONED_DATE_TIME.parse((String)dateTime)
                                    .query(this::convertToDate);
                        }
                        catch (DateTimeParseException e1)
                        {
                            throw new IllegalArgumentException(TO_DATE
                                                               + " requires an ISO-8601 format date or date/time.",
                                                               e1);
                        }

                    }

                    private Date convertToDate(TemporalAccessor t)
                    {
                        if(!t.isSupported(ChronoField.INSTANT_SECONDS))
                        {
                            t = LocalDateTime.of(LocalDate.from(t), LocalTime.MIN).atOffset(ZoneOffset.UTC);
                        }
                        return new Date((t.getLong(ChronoField.INSTANT_SECONDS) * 1000L)
                                        + t.getLong(ChronoField.MILLI_OF_SECOND));
                        
                    }
                };
            }

        },

        DATE_ADD {
            @Override
            ConfiguredObjectExpression asExpression (final List<Expression> args)
            {
                if (args == null || args.size() != 2)
                {
                    throw new IllegalArgumentException(DATE_ADD.name() + " requires two arguments.");
                }

                return new ConfiguredObjectExpression()
                {
                    @Override
                    public Object evaluate(final ConfiguredObject<?> object)
                    {
                        Object date = args.get(0).evaluate(object);
                        Object period = args.get(1).evaluate(object);
                        if (!(date instanceof Date) || !(period instanceof String))
                        {
                            throw new IllegalArgumentException(String.format("%s requires a (Date, String) not a"
                                                                             + " (%s,%s)",
                                                                             DATE_ADD,
                                                                             date.getClass().getSimpleName(),
                                                                             period.getClass().getSimpleName()));
                        }
                        try
                        {
                            Date copy = new Date(((Date) date).getTime());
                            final Duration duration = DATATYPE_FACTORY.newDuration((String) period);
                            duration.addTo(copy);
                            return copy;
                        }
                        catch (IllegalArgumentException e)
                        {
                            throw new IllegalArgumentException(DATE_ADD + " requires an ISO-8601 format duration.", e);
                        }
                    }
                };
            }
        },

        TO_STRING {
            @Override
            ConfiguredObjectExpression asExpression(final List<Expression> args)
            {
                if (args == null || (args.size() == 0 || args.size() > 3))
                {
                    throw new IllegalArgumentException(TO_STRING.name() + " requires (Object[,{printf format specifier},[{timezone name}]]).");
                }

                return new ConfiguredObjectExpression()
                {
                    @Override
                    public Object evaluate(final ConfiguredObject<?> object)
                    {
                        Object obj = args.get(0).evaluate(object);
                        Object format = args.size() > 1 ? args.get(1).evaluate(object) : null;
                        Object timezoneName = args.size() > 2 ? args.get(2).evaluate(object) : null;
                        if (obj instanceof Date)
                        {
                            final Calendar cal = timezoneName == null ? Calendar.getInstance(UTC) : Calendar.getInstance(TimeZone.getTimeZone(
                                    (String) timezoneName));
                            cal.setTime((Date) obj);
                            if (format == null)
                            {
                                return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(cal.toInstant().atZone(ZoneId.of(timezoneName == null ? "UTC" : (String)timezoneName)));
                            }
                            else
                            {
                                return String.format((String)format, cal);
                            }
                        }
                        else
                        {
                            // TODO If obj itself is another configured object perhaps we should just use its name or id? The CO.toString value probably isn't too useful.
                            if (format == null)
                            {
                                return String.valueOf(obj);
                            }
                            else
                            {
                                return String.format((String)format, obj);
                            }
                        }
                    }
                };
            }
        };

        abstract ConfiguredObjectExpression asExpression( List<Expression> args );
    }


    public ConfiguredObjectExpression createConfiguredObjectExpression(final String propertyName)
    {
        return new ConfiguredObjectPropertyExpression(propertyName);
    }

    public ConfiguredObjectExpression createConfiguredObjectExpression(final String propertyName,
                                                                       final ConfiguredObjectExpression expression)
    {
        return new ChainedConfiguredObjectExpression(propertyName, expression);
    }


    public ConfiguredObjectExpression createConfiguredObjectExpression(final String propertyName,
                                                                       final int index)
    {
        return new IndexedConfiguredObjectExpression(propertyName, index);
    }

    public ConfiguredObjectExpression createFunctionExpression(String functionName, final List<Expression> args)
            throws ParseException
    {
        try
        {
            FilterFunction function = null;
            try
            {
                function = FilterFunction.valueOf(functionName.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new ParseException("Unknown function name : '" + functionName + "'");
            }
            return function.asExpression(args);
        }
        catch(IllegalArgumentException e)
        {
            throw new ParseException("Function parameter mismatch : '" + functionName + "'", e);
        }
    }

    private static class ConfiguredObjectPropertyExpression implements ConfiguredObjectExpression, NamedExpression<ConfiguredObject<?>>
    {

        private final String _propertyName;

        public ConfiguredObjectPropertyExpression(final String propertyName)
        {
            _propertyName = propertyName;
        }

        @Override
        public Object evaluate(final ConfiguredObject<?> object)
        {
            return object == null ? null : getValue(object);
        }

        private Object getValue(final ConfiguredObject<?> object)
        {

            if(SPECIAL_ATTRIBUTES.contains(_propertyName))
            {
                if(PARENT_ATTR.equals(_propertyName))
                {
                    return object.getParent();
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return object.getAttributeNames().contains(_propertyName)
                        ? object.getAttribute(_propertyName)
                        : object.getStatistics().get(_propertyName);
            }
        }

        @Override
        public String getName()
        {
            return _propertyName;
        }
    }

    private static class ChainedConfiguredObjectExpression implements ConfiguredObjectExpression
    {
        private final ConfiguredObjectPropertyExpression _first;
        private final ConfiguredObjectExpression _chainedExpression;

        public ChainedConfiguredObjectExpression(final String propertyName, final ConfiguredObjectExpression expression)
        {
            _first = new ConfiguredObjectPropertyExpression(propertyName);
            _chainedExpression = expression;
        }

        @Override
        public Object evaluate(final ConfiguredObject<?> object)
        {
            Object propertyValue = _first.evaluate(object);
            if(propertyValue instanceof ConfiguredObject)
            {
                return _chainedExpression.evaluate((ConfiguredObject)propertyValue);
            }
            else if(propertyValue instanceof Map && _chainedExpression instanceof ConfiguredObjectPropertyExpression)
            {
                return ((Map) propertyValue).get(((ConfiguredObjectPropertyExpression)_chainedExpression)._propertyName);
            }
            return null;
        }
    }

    private static class IndexedConfiguredObjectExpression implements ConfiguredObjectExpression
    {
        private final String _propertyName;
        private final int _index;

        public IndexedConfiguredObjectExpression(final String propertyName, final int index)
        {
            _propertyName = propertyName;
            _index = index;
        }

        @Override
        public Object evaluate(final ConfiguredObject<?> object)
        {
            Object propertyValue = object == null ? null : object.getAttribute(_propertyName);
            if(propertyValue instanceof Collection)
            {
                Iterator iter = ((Collection)propertyValue).iterator();
                int pos = 0;
                while(iter.hasNext() && pos < _index)
                {
                    iter.next();
                }
                if(pos == _index && iter.hasNext())
                {
                    return iter.next();
                }
            }
            return null;
        }
    }
}
