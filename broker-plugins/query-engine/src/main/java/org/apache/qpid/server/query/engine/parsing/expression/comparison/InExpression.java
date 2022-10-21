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
package org.apache.qpid.server.query.engine.parsing.expression.comparison;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.evaluator.SelectEvaluator;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Comparison IN operation. Evaluates left expression against list of expressions or against a subquery result.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
public class InExpression<T, R> extends AbstractComparisonExpression<T, Boolean>
{
    /**
     * Constructor initializes children expression list
     *
     * @param children Children expressions
     */
    public InExpression(final List<ExpressionNode<T, ?>> children)
    {
        super(children);
        final String firstAlias = getChild(0).getAlias();
        final String aliases = children.subList(1, children.size()).stream()
            .map(ExpressionNode::getAlias).collect(Collectors.joining(","));
        _metadata.setAlias(firstAlias + " in (" + aliases + ")");
    }

    /**
     * Constructor initializes children expression list
     *
     * @param left Left child expression
     * @param right Right child expression
     */
    public InExpression(final ExpressionNode<T, ?> left, final SelectExpression<T, R> right)
    {
        super(left, right);
    }

    /**
     * Performs IN comparison using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Boolean result of value evaluation
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Boolean apply(final T value)
    {
        final ExpressionNode<R, ?> left = getChild(0);
        final ExpressionNode<R, ?> right = getChild(1);
        if (right instanceof SelectExpression)
        {
            final SelectExpression<T, R> selectExpression = (SelectExpression<T, R>) right;
            final List<R> items = (List<R>) selectExpression.apply((T) new SelectEvaluator());
            return items.stream().anyMatch(item -> new EqualExpression(left, ConstantExpression.of(item)).apply(value));
        }
        else
        {
            final List<ExpressionNode<T, Object>> items = getChildren().subList(1, getChildren().size());
            return items.stream()
                .map(item -> (R) item.apply(value))
                .anyMatch(item -> new EqualExpression(ConstantExpression.of(left.apply((R) value)), ConstantExpression.of(item)).apply(item));
        }
    }
}
