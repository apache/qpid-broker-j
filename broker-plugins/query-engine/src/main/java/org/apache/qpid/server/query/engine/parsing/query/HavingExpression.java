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
package org.apache.qpid.server.query.engine.parsing.query;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.AbstractAggregationExpression;
import org.apache.qpid.server.query.engine.parsing.factory.CollectorFactory;

/**
 * Contains expression for evaluation of having clause predicate
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class HavingExpression<T, R> extends AbstractExpressionNode<T, R> implements Predicate<T>
{
    /**
     * Cached result
     */
    private Map<String, Object> _result;

    /**
     * Constructor initializes children expression list
     *
     * @param selectExpression Select expression
     * @param expression Having expression
     */
    public HavingExpression(final SelectExpression<T, R> selectExpression, final ExpressionNode<T, R> expression)
    {
        super(selectExpression, expression);
    }

    /**
     * Evaluates an object against having clause
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    public R apply(final T value)
    {
        return evaluateChild(1, value);
    }

    /**
     * Evaluates an object against having clause
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    public boolean test(final T value)
    {
        return (Boolean) apply(value);
    }

    /**
     * Prepares boolean mask in form of a map having same structure as actual aggregation result, but containing
     * boolean values instead of actual values. Later this boolean mask is applied to the aggregation result to
     * filter it in accordance with the having predicate.
     *
     * @param selectExpression Select expression
     * @param items List of objects to be aggregated
     *
     * @param <A> Collector accumulation type
     */
    @SuppressWarnings("unchecked")
    public <A> void applyAggregation(
        final SelectExpression<T, R> selectExpression,
        final List<T> items
    )
    {
        final Map<String, Object> result = new LinkedHashMap<>();

        for (final AbstractAggregationExpression<T, R> aggregation : this.<R>getAggregations())
        {
            final Expression<T, R> source = aggregation.getSource();
            Collector<T, A, R> collector = CollectorFactory.<T, A, R>collector(aggregation.getCollectorType()).apply(source);
            final List<ProjectionExpression<T, R>> groupByItems = selectExpression.getGroupBy();
            for (int i = groupByItems.size() - 1; i >= 0; i--)
            {
                collector = (Collector<T, A, R>) Collectors.groupingBy(groupByItems.get(i), collector);
            }
            final Map<String, Object> aggregationResult = (Map<String, Object>) items.stream().collect(collector);
            aggregation.setValue((R) aggregationResult);
            aggregation.setSelect(selectExpression);
            if (result.isEmpty())
            {
                result.putAll(aggregationResult);
            }
            else
            {
                for (final Map.Entry<String, Object> entry : result.entrySet())
                {
                    final Boolean value = (Boolean) aggregationResult.get(entry.getKey());
                    if (value != null)
                    {
                        entry.setValue((Boolean) entry.getValue() && value);
                    }
                }
            }
        }
        _result = result;
    }

    /**
     * Filters aggregation result in accordance with the having predicate
     *
     * @param result Map with aggregation result
     */
    public void filter(final Map<String, R> result)
    {
        filter(result, _result);
    }

    /**
     * Goes recursively through the aggregation result and removes entries not in accordance with the
     * having predicate result.
     *
     * @param result Map with aggregation result
     * @param havingResult Map of boolean values representing having predicate
     */
    @SuppressWarnings("unchecked")
    private void filter(final Map<String, R> result, final Map<String, Object> havingResult)
    {
        for (final Map.Entry<String, Object> entry : havingResult.entrySet())
        {
            final Object object = entry.getValue();
            if (object instanceof Boolean)
            {
                final Boolean flag = (Boolean) object;
                if (!flag)
                {
                    result.remove(entry.getKey());
                }
            }
            if (object instanceof Map && result.containsKey(entry.getKey()))
            {
                filter((Map<String, R>) result.get(entry.getKey()), (Map<String, Object>) object);
                if (result.get(entry.getKey()) instanceof Map && ((Map<String, Object>) result.get(entry.getKey())).isEmpty())
                {
                    result.remove(entry.getKey());
                }
            }
        }
    }
}
