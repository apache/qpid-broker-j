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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.filter.BooleanExpression;
import org.apache.qpid.filter.Expression;
import org.apache.qpid.server.model.ConfiguredObject;

public final class ConfiguredObjectQuery
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredObjectQuery.class);

    private static final String[] STANDARD_FIELDS = new String[]{ConfiguredObject.ID, ConfiguredObject.NAME};

    private final ConfiguredObjectExpressionFactory _expressionFactory = new ConfiguredObjectExpressionFactory();

    private final List<List<Object>> _results;
    private final List<String> _headers;

    interface HeadersAndValueExpressions
    {
        List<String> getHeaders();
        List<Expression> getValueExpressions();
    }

    public ConfiguredObjectQuery(List<ConfiguredObject<?>> objects, String selectClause, String whereClause)
    {

        HeadersAndValueExpressions headersAndValueExpressions = parseSelectClause(selectClause);

        List<ConfiguredObject<?>> filteredObjects = whereClause == null ? objects : filterObjects(objects, whereClause);

        _headers = headersAndValueExpressions.getHeaders();
        _results = evaluateResults(filteredObjects, headersAndValueExpressions.getValueExpressions());

    }

    public List<List<Object>> getResults()
    {
        return _results;
    }

    public List<String> getHeaders()
    {
        return _headers;
    }

    private HeadersAndValueExpressions parseSelectClause(final String selectClause)
    {
        final List<String> headers = new ArrayList<>();
        final List<Expression> valueExpressions = new ArrayList<>();
        if (selectClause != null)
        {
            ConfiguredObjectFilterParser parser = new ConfiguredObjectFilterParser();
            parser.setConfiguredObjectExpressionFactory(_expressionFactory);

            try
            {
                final List<Map<String, Expression>> expressions =
                        parser.parseSelect(selectClause);
                for (Map<String, Expression> expression : expressions)
                {
                    final Map.Entry<String, Expression> entry = expression.entrySet().iterator().next();
                    headers.add(entry.getKey());
                    valueExpressions.add(entry.getValue());
                }
            }
            catch (ParseException | TokenMgrError e)
            {
                throw new QueryException("Unable to parse select clause");
            }
        }
        else
        {
            for (String field : STANDARD_FIELDS)
            {
                headers.add(field);
                valueExpressions.add(_expressionFactory.createConfiguredObjectExpression(field));
            }
        }
        return new HeadersAndValueExpressions()
                {
                    @Override
                    public List<String> getHeaders()
                    {
                        return headers;
                    }

                    @Override
                    public List<Expression> getValueExpressions()
                    {
                        return valueExpressions;
                    }
                };
    }

    private List<ConfiguredObject<?>> filterObjects(final List<ConfiguredObject<?>> objects, final String whereClause)
    {
        List<ConfiguredObject<?>> filteredObjects = new ArrayList<>();
        ConfiguredObjectFilterParser parser = new ConfiguredObjectFilterParser();
        parser.setConfiguredObjectExpressionFactory(_expressionFactory);
        try
        {
            final BooleanExpression<ConfiguredObject> expression =
                    parser.parseWhere(whereClause);


            for (ConfiguredObject<?> object : objects)
            {
                try
                {
                    if (expression.matches(object))
                    {
                        filteredObjects.add(object);
                    }
                }
                catch (RuntimeException e)
                {
                    LOGGER.debug("Error while evaluating object against where clause", e);
                }
            }

        }
        catch (ParseException | TokenMgrError e)
        {
            throw new QueryException("Unable to parse where clause");
        }
        return filteredObjects;
    }


    private List<List<Object>> evaluateResults(final List<ConfiguredObject<?>> filteredObjects, List<Expression> valueExpressions)
    {
        List<List<Object>> values = new ArrayList<>();
        for (ConfiguredObject<?> object : filteredObjects)
        {
            List<Object> objectVals = new ArrayList<>();
            for (Expression<ConfiguredObject<?>> evaluator : valueExpressions)
            {
                Object value;
                try
                {
                    value = evaluator.evaluate(object);
                }
                catch (RuntimeException e)
                {
                    LOGGER.debug("Error while evaluating select clause", e);
                    value = null;
                }
                objectVals.add(value);
            }
            values.add(objectVals);
        }
        return values;
    }



}
