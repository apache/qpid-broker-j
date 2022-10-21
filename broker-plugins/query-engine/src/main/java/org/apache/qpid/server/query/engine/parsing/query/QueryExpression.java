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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.set.SetExpression;

/**
 * Contains information about a query, which contains WITH clause items, select expression, ORDER BY clause items
 * as well as LIMIT and OFFSET clauses.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class QueryExpression<T, R> extends AbstractExpressionNode<T, R>
{
    /**
     * List of WITH clause items, representing named subqueries
     */
    private List<WithItem<T, R>> _withItems = new ArrayList<>();

    /**
     * Set expression usually is represented by a select expression, but can also be a set operation (union, minus,
     * intersect).
     */
    private SetExpression<T, R> _setExpression;

    /**
     *  List of ORDER BY clause items
     */
    private final List<OrderItem<T, R>> _orderItems = new ArrayList<>();

    /**
     * Limit value
     */
    private Integer _limit;

    /**
     * Offset value
     */
    private Integer _offset;

    /**
     * Shouldn't be called as query is supposed to be passed to QueryEvaluator instance
     *
     * @param value Object to handle (must be an instance of a QueryEvaluator)
     *
     * @return Evaluation result
     */
    @Override()
    @SuppressWarnings("unchecked")
    public R apply(final T value)
    {
        if (!(value instanceof QueryEvaluator))
        {
            throw QueryEvaluationException.of(Errors.EVALUATION.EVALUATOR_NOT_SUPPLIED);
        }
        final QueryEvaluator queryEvaluator = (QueryEvaluator) value;
        return (R) queryEvaluator.evaluate(this);
    }

    /**
     * Returns domains used by query
     *
     * @return List of domains used by query
     */
    public List<String> getDomains()
    {
        final List<String> domains = new LinkedList<>();
        getDomains(_setExpression, domains);
        return domains;
    }

    /**
     * Recursively goes through the expression and retrieves domains used
     *
     * @param expression Expression to retrieve domains from
     * @param domains List to store domains
     */
    @SuppressWarnings("unchecked")
    private void getDomains(final ExpressionNode<T, ?> expression, final List<String> domains)
    {
        if (expression instanceof SelectExpression)
        {
            final SelectExpression<T, R> select = (SelectExpression<T, R>) expression;
            if (select.getFrom() != null)
            {
                domains.add(select.getFrom().getAlias());
            }
            if (select.getWhere() != null)
            {
                ((ExpressionNode<T, ?>) select.getWhere()).getChildren().forEach(child -> getDomains(child, domains));
            }
        }
        else
        {
            expression.getChildren().forEach(child -> getDomains(child, domains));
        }
    }

    public List<WithItem<T, R>> getWithItems()
    {
        return new ArrayList<>(_withItems);
    }

    public void setWithItems(final List<WithItem<T, R>> withItems)
    {
        _withItems = new ArrayList<>(withItems);
    }

    public SetExpression<T, R> getSelect()
    {
        return _setExpression;
    }

    public void setSelect(final SetExpression<T, R> expression)
    {
        _setExpression = expression;
    }

    public void addOrderItem(OrderItem<T, R> orderItem)
    {
        _orderItems.add(orderItem);
    }

    @SuppressWarnings({"findbugs:EI_EXPOSE_REP", "java:S2384"})
    //sonar: mutable list is returned intentionally, it may be modified during sorting process
    public List<OrderItem<T, R>> getOrderItems()
    {
        return _orderItems;
    }

    public Integer getLimit()
    {
        return _limit;
    }

    public void setLimit(final Integer limit)
    {
        _limit = limit;
    }

    public Integer getOffset()
    {
        return _offset;
    }

    public void setOffset(final Integer offset)
    {
        _offset = offset;
    }
}
