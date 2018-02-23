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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.filter.BooleanExpression;
import org.apache.qpid.server.filter.Expression;
import org.apache.qpid.server.filter.SelectorParsingException;
import org.apache.qpid.server.filter.OrderByExpression;
import org.apache.qpid.server.model.ConfiguredObject;

public final class ConfiguredObjectQuery
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredObjectQuery.class);

    private static final String[] STANDARD_FIELDS = new String[]{ConfiguredObject.ID, ConfiguredObject.NAME};
    public static final int DEFAULT_LIMIT = -1;
    public static final int DEFAULT_OFFSET = 0;

    private final ConfiguredObjectExpressionFactory _expressionFactory = new ConfiguredObjectExpressionFactory();

    private final List<List<Object>> _results;
    private final List<String> _headers;
    private final int _totalNumberOfRows;

    interface HeadersAndValueExpressions
    {
        List<String> getHeaders();
        List<Expression> getValueExpressions();
        boolean hasHeader(String name);
        Expression getValueExpressionForHeader(String name);
    }

    public ConfiguredObjectQuery(List<ConfiguredObject<?>> objects, String selectClause, String whereClause)
    {
        this(objects, selectClause, whereClause, null);
    }

    public ConfiguredObjectQuery(List<ConfiguredObject<?>> objects,
                                 String selectClause,
                                 String whereClause,
                                 String orderByClause)
    {
        this(objects, selectClause, whereClause, orderByClause, null, null);
    }

    public ConfiguredObjectQuery(List<ConfiguredObject<?>> objects,
                                 String selectClause,
                                 String whereClause,
                                 String orderByClause,
                                 String limitClause,
                                 String offsetClause)
    {
        int limit = toInt(limitClause, DEFAULT_LIMIT);
        int offset = toInt(offsetClause, DEFAULT_OFFSET);

        HeadersAndValueExpressions headersAndValueExpressions = parseSelectClause(selectClause);

        List<ConfiguredObject<?>> filteredObjects = whereClause == null ? objects : filterObjects(objects, whereClause);
        List<ConfiguredObject<?>> orderedObjects = orderByClause == null ? filteredObjects : orderObjects(filteredObjects,
                                                                                                          orderByClause,
                                                                                                          headersAndValueExpressions);
        List<ConfiguredObject<?>> limitedOrderedObjects = applyLimitAndOffset(orderedObjects, limit, offset);

        _headers = headersAndValueExpressions.getHeaders();
        _results = evaluateResults(limitedOrderedObjects, headersAndValueExpressions.getValueExpressions());
        _totalNumberOfRows = filteredObjects.size();
    }

    public List<List<Object>> getResults()
    {
        return _results;
    }

    public List<String> getHeaders()
    {
        return _headers;
    }

    public int getTotalNumberOfRows()
    {
        return _totalNumberOfRows;
    }

    private int toInt(String value, int defaultValue)
    {
        int returnValue = defaultValue;
        if (value != null)
        {
            try
            {
                returnValue = Integer.parseInt(value);
            }
            catch (NumberFormatException e)
            {
            }
        }
        return returnValue;
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
                throw new SelectorParsingException("Unable to parse select clause", e);
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
                        return Collections.unmodifiableList(headers);
                    }

                    @Override
                    public List<Expression> getValueExpressions()
                    {
                        return Collections.unmodifiableList(valueExpressions);
                    }

                    @Override
                    public boolean hasHeader(final String headerName)
                    {
                        return headers.contains(headerName);
                    }

                    @Override
                    public Expression getValueExpressionForHeader(final String headerName)
                    {
                        final int i = headers.indexOf(headerName);
                        if (i  < 0)
                        {
                            throw new IllegalStateException(String.format("No expression found for header '%s'", headerName));
                        }

                        return valueExpressions.get(i);
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
                    throw new EvaluationException("Error while evaluating object against where clause", e);
                }
            }

        }
        catch (ParseException | TokenMgrError e)
        {
            throw new SelectorParsingException("Unable to parse where clause", e);
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

    private List<ConfiguredObject<?>> applyLimitAndOffset(final List<ConfiguredObject<?>> orderedObjects, final int limit, int offset)
    {
        int size = orderedObjects.size();
        int firstIndex = offset < 0 ? Math.max(0, size + offset) : Math.min(size, offset);
        int lastIndex = limit < 0 ? size : Math.min(size, firstIndex + limit);

        return orderedObjects.subList(firstIndex, lastIndex);
    }


    class OrderByComparator implements Comparator<Object>
    {
        private final List<OrderByExpression> _orderByExpressions;

        public OrderByComparator(final List<OrderByExpression> orderByExpressions,
                                 final List<Expression> valueExpressions)
        {
            _orderByExpressions = new ArrayList<>(orderByExpressions);
            for (ListIterator<OrderByExpression> iterator = _orderByExpressions.listIterator(); iterator.hasNext(); )
            {
                OrderByExpression orderByExpression = iterator.next();
                if (orderByExpression.isColumnIndex())
                {
                    // column indices are starting from 1 by SQL spec
                    int index = orderByExpression.getColumnIndex();
                    if (index <= 0 || index > valueExpressions.size())
                    {
                        throw new EvaluationException(String.format("Invalid column index '%d' in orderBy clause", index));
                    }
                    else
                    {
                        orderByExpression = new OrderByExpression(valueExpressions.get(index - 1), orderByExpression.getOrder());
                        iterator.set(orderByExpression);
                    }
                }
            }
        }

        @Override
        public int compare(final Object o1, final Object o2)
        {
            int index = 0;
            int comparisonResult = 0;
            for (OrderByExpression orderByExpression : _orderByExpressions)
            {
                try
                {
                    Comparable left = (Comparable) orderByExpression.evaluate(o1);
                    Comparable right = (Comparable) orderByExpression.evaluate(o2);
                    if (left == null && right != null)
                    {
                        comparisonResult = -1;
                    }
                    else if (left != null && right == null)
                    {
                        comparisonResult = 1;
                    }
                    else if (left != null && right != null)
                    {
                        comparisonResult = left.compareTo(right);
                    }
                    if (comparisonResult != 0)
                    {
                        int order = 1;
                        if (orderByExpression.getOrder() == OrderByExpression.Order.DESC)
                        {
                            order = -1;
                        }
                        return order * comparisonResult;
                    }
                    index++;
                }
                catch (ClassCastException e)
                {
                    throw new EvaluationException(String.format("The orderBy expression at position '%d' is unsupported", index), e);
                }
            }
            return comparisonResult;
        }
    }

    private List<ConfiguredObject<?>> orderObjects(final List<ConfiguredObject<?>> unorderedResults,
                                                   final String orderByClause,
                                                   final HeadersAndValueExpressions headersAndValue)
    {
        List<OrderByExpression> orderByExpressions = parseOrderByClause(orderByClause, headersAndValue);
        List<ConfiguredObject<?>> orderedObjects = new ArrayList<>(unorderedResults.size());
        orderedObjects.addAll(unorderedResults);
        Comparator<Object> comparator = new OrderByComparator(orderByExpressions, headersAndValue.getValueExpressions());
        Collections.sort(orderedObjects, comparator);
        return orderedObjects;
    }

    private List<OrderByExpression> parseOrderByClause(final String orderByClause,
                                                       final HeadersAndValueExpressions headersAndValue)
    {
        final List<OrderByExpression> orderByExpressions;
        ConfiguredObjectFilterParser parser = new ConfiguredObjectFilterParser();
        parser.setConfiguredObjectExpressionFactory(new ConfiguredObjectExpressionFactory()
        {
            @Override
            public ConfiguredObjectExpression createConfiguredObjectExpression(final String propertyName)
            {
                if (headersAndValue.hasHeader(propertyName))
                {
                    Expression expression = headersAndValue.getValueExpressionForHeader(propertyName);
                    return object -> expression.evaluate(object);
                }
                else
                {
                    return super.createConfiguredObjectExpression(propertyName);
                }
            }
        });
        try
        {
            orderByExpressions = parser.parseOrderBy(orderByClause);
        }
        catch (ParseException | TokenMgrError e)
        {
            throw new SelectorParsingException("Unable to parse orderBy clause", e);
        }
        return orderByExpressions;
    }


}
