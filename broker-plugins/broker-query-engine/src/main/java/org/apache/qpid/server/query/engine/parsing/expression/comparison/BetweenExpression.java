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

import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.logic.AndExpression;

/**
 * Comparison BETWEEN operation. Evaluates given value against low and high parameters (inclusive).
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
public class BetweenExpression<T, R> extends AbstractComparisonExpression<T, Boolean>
{
    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param left Left child expression
     * @param low Low child expression
     * @param high High child expression
     */
    public BetweenExpression(
        final String alias,
        final ExpressionNode<T, ?> left,
        final ExpressionNode<T, ?> low,
        final ExpressionNode<T, ?> high
    )
    {
        super(alias, left, low, high);
    }

    /**
     * Performs BETWEEN comparison using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Boolean result of value evaluation
     */
    @Override
    public Boolean apply(final T value)
    {
        final ExpressionNode<T, R> source = getChild(0);
        final ExpressionNode<T, R> left = getChild(1);
        final ExpressionNode<T, Boolean> right = getChild(2);
        return new AndExpression<>(
            getAlias(),
            new GreaterThanOrEqualExpression<>(source, left),
            new LessThanOrEqualExpression<>(source, right)
        ).test(value);
    }
}
