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

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import org.apache.qpid.filter.Expression;
import org.apache.qpid.server.model.ConfiguredObject;

public class ConfiguredObjectExpressionFactory
{

    private static final String PARENT_ATTR = "$parent";
    private static Set<String> SPECIAL_ATTRIBUTES = new HashSet<>(Arrays.asList(PARENT_ATTR));


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

        TO_DATE
                {
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
                            final Calendar calendar = DatatypeConverter.parseDateTime((String) dateTime);
                            return calendar.getTime();
                        }
                        catch (IllegalArgumentException e)
                        {
                            throw new IllegalArgumentException(TO_DATE
                                                               + " requires an ISO-8601 format date or date/time.", e);
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
            FilterFunction function = FilterFunction.valueOf(functionName.toUpperCase());
            return function.asExpression(args);
        }
        catch(IllegalArgumentException e)
        {
            throw new ParseException("Unknown function name " + functionName);
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
                    return object.getParent(object.getModel().getParentTypes(object.getCategoryClass()).iterator().next());
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
