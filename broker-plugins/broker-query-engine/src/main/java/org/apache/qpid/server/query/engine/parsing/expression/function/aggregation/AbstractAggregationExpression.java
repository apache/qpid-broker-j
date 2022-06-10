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
package org.apache.qpid.server.query.engine.parsing.expression.function.aggregation;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.collector.CollectorType;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * Parent for aggregation function classes
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public abstract class AbstractAggregationExpression<T, R> extends AbstractFunctionExpression<T, R>
{
    /**
     * Cached aggregation result
     */
    private R _value;

    /**
     * Child select expression
     */
    private SelectExpression<T, R> _selectExpression;

    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public AbstractAggregationExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
    }

    /**
     * Should be called after aggregate() method returning cached result. When query contains HAVING clause,
     * extracts result from the nested map.
     *
     * @param value Object to handle
     *
     * @return Aggregation result
     */
    @SuppressWarnings("unchecked")
    public R apply (final T value)
    {
        if (_value != null)
        {
            if (_value instanceof Map)
            {
                final List<R> indexes = _selectExpression.getGroupBy().stream().map(item -> item.apply(value)).collect(Collectors.toList());
                Object result = _value;
                for (final R index : indexes)
                {
                    result = ((Map<?, ?>) result).get(index);
                }
                return (R) result;
            }
            return _value;
        }
        throw QueryParsingException.of(Errors.AGGREGATION.MISUSED, getClass().getSimpleName());
    }

    /**
     * Evaluates and validates input collection values
     *
     * @param items Input collection
     * @param _typeValidator Argument type validator
     *
     * @return List of evaluated values
     */
    protected List<?> getArguments(final List<T> items, final FunctionParameterTypePredicate<Object> _typeValidator)
    {
        final List<?> args = items.stream().map(getSource()).collect(Collectors.toList());
        final List<?> invalidArgs = args.stream().filter(_typeValidator.negate()).collect(Collectors.toList());
        if (!invalidArgs.isEmpty())
        {
            throw QueryEvaluationException.invalidFunctionParameters(_functionName, invalidArgs);
        }
        return args;
    }

    /**
     * Aggregation implementation
     *
     * @param items Collection to perform aggregation on
     *
     * @return Aggregation result
     */
    public abstract R aggregate(final List<T> items);

    /**
     * Type of the collector depends on the aggregation type
     *
     * @return CollectorType instance
     */
    public abstract CollectorType getCollectorType();

    public void setValue(final R value)
    {
        _value = value;
    }

    public void setSelect(final SelectExpression<T, R> selectExpression)
    {
        _selectExpression = selectExpression;
    }

    public Expression<T, R> getSource()
    {
        return getChild(0);
    }
}
