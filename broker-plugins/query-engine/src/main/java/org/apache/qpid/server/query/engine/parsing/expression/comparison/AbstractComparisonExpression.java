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
import java.util.function.Predicate;

import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;

/**
 * Parent for comparison expression classes
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public abstract class AbstractComparisonExpression<T, R> extends AbstractExpressionNode<T, R> implements Predicate<T>
{
    /**
     * Comparison operator
     */
    protected String _operator;

    /**
     * Constructor initializes children expression list
     *
     * @param left Left child expression
     */
    public AbstractComparisonExpression(final ExpressionNode<T, ?> left)
    {
        super(left);
    }

    /**
     * Constructor initializes children expression list
     *
     * @param left Left child expression
     * @param right Right child expression
     */
    public AbstractComparisonExpression(final ExpressionNode<T, ?> left, final ExpressionNode<T, ?> right)
    {
        super(left, right);
    }

    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param left Left child expression
     * @param low Low child expression
     * @param high High child expression
     */
    public AbstractComparisonExpression(final String alias, final ExpressionNode<T, ?> left, final ExpressionNode<T, ?> low, final ExpressionNode<T, ?> high)
    {
        super(left, low, high);
        _metadata.setAlias(alias);
    }

    /**
     * Constructor initializes children expression list
     *
     * @param children Children expressions
     */
    public AbstractComparisonExpression(final List<ExpressionNode<T, ?>> children)
    {
        super(children);
    }

    /**
     * Evaluates value to a boolean result
     *
     * @param value Object to handle
     *
     * @return Boolean result of value evaluation
     */
    @Override
    public boolean test(final T value)
    {
        return (Boolean) apply(value);
    }

    /**
     * Returns expression alias
     *
     * @return Expression alias
     */
    @Override
    public String getAlias()
    {
        final ExpressionNode<T, ?> left = getChild(0);
        final ExpressionNode<T, ?> right = getChild(1);
        return _metadata.getAlias() != null ? _metadata.getAlias() : left.getAlias() + _operator + right.getAlias();
    }
}
