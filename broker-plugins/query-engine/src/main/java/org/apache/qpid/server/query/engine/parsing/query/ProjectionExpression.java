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

import java.util.List;
import java.util.Objects;

import org.apache.qpid.server.query.engine.evaluator.DateFormat;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.AbstractAggregationExpression;

/**
 * Contains information about a single item in a SELECT clause
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ProjectionExpression<T, R> extends AbstractExpressionNode<T, R>
{
    /**
     * Ordinal value
     */
    protected final int _ordinal;

    /**
     * Constructor stores properties
     *
     * @param alias Item alias
     * @param expression Item expression
     * @param ordinal Ordinal value
     */
    public ProjectionExpression(final String alias, final ExpressionNode<T, R> expression, final int ordinal)
    {
        super(alias, expression);
        _ordinal = ordinal;
    }

    /**
     * Evaluated an object against the expression
     *
     * @param object Object to handle
     *
     * @return Evaluation result
     */
    @SuppressWarnings("unchecked")
    public R apply(final T object)
    {
        final R result = evaluateChild(0, object);
        if (DateTimeConverter.isDateTime(result))
        {
            final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
            final DateFormat dateFormat = querySettings.getDateTimeFormat();
            if (dateFormat.equals(DateFormat.LONG))
            {
                return (R) DateTimeConverter.toLong(result);
            }
        }
        return result;
    }

    /**
     * Evaluates a collection of objects against an aggregation expression
     *
     * @param selectExpression Select expression
     * @param items Collection of objects
     *
     * @return Aggregation result
     */
    public R applyAggregation(final SelectExpression<T, R> selectExpression, final List<T> items)
    {
        final List<AbstractAggregationExpression<T, R>> aggregations = getAggregations();
        for (final AbstractAggregationExpression<T, R> aggregation : aggregations)
        {
            final R result = aggregation.aggregate(items);
            aggregation.setValue(result);
            aggregation.setSelect(selectExpression);
        }
        return apply(null);
    }

    /**
     * Returns wrapped expression
     *
     * @return Expression instance
     */
    public Expression<T, R> getExpression()
    {
        return getChild(0);
    }

    public void setAlias(final String alias)
    {
        _metadata.setAlias(alias);
    }

    public String getAlias()
    {
        return _metadata.getAlias() != null ? _metadata.getAlias() : getChild(0).getAlias();
    }

    public boolean isOrdinal()
    {
        return NumberConverter.isNumber(getAlias());
    }

    @Override()
    public String toString()
    {
        final ExpressionNode<T, R> expression = getChild(0);
        return Objects.equals(expression.getAlias(), getAlias())
            ? expression.getAlias() : expression.getAlias() + " as " + getAlias();
    }
}
