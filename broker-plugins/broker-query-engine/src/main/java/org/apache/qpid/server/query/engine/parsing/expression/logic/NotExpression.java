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
package org.apache.qpid.server.query.engine.parsing.expression.logic;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.InExpression;

/**
 * Logical NOT expression.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
public class NotExpression<T, R> extends AbstractExpressionNode<T, Boolean> implements Predicate<T>
{
    /**
     * Constructor initializes children expression list
     *
     * @param source Source expression
     */
    @SuppressWarnings("unchecked")
    public NotExpression(final Expression<T, R> source)
    {
        super((ExpressionNode<T, ?>) source);
        if (source instanceof InExpression)
        {
            final List<ExpressionNode<T, R>> children = ((InExpression<T, R>) source).getChildren();
            final ExpressionNode<T, R> left = (ExpressionNode<T, R>) getChild(0).getChildren().get(0);
            _metadata.setAlias(left.getAlias() + " not in ("
                + children.subList(1, children.size()).stream().map(ExpressionNode::getAlias)
                    .collect(Collectors.joining(",")) + ")");
        }
        else
        {
            _metadata.setAlias("not" + ((ExpressionNode<T, R>) source).getAlias());
        }
    }

    /**
     * Performs logical NOT operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    @SuppressWarnings("unchecked")
    public Boolean apply(final T value)
    {
        final Predicate<T> source = (Predicate<T>) getChild(0);
        return !source.test(value);
    }

    /**
     * Performs logical NOT operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    public boolean test(final T value)
    {
        return apply(value);
    }
}
