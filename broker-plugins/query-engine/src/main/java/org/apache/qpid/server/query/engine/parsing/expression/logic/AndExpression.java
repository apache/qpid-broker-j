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

import java.util.function.Predicate;

import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;

/**
 * Logical AND expression.
 *
 * @param <T> Input parameter type
 */
public class AndExpression<T> extends AbstractExpressionNode<T, Boolean> implements Predicate<T>
{
    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param left Left expression
     * @param right Right expression
     */
    public AndExpression(final String alias, final ExpressionNode<T, ?> left, final ExpressionNode<T, ?> right)
    {
        super(alias,  left, right);
    }

    /**
     * Performs logical AND operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @SuppressWarnings("unchecked")
    @Override
    public Boolean apply(final T value)
    {
        final Predicate<T> left = (Predicate<T>) getChild(0);
        final Predicate<T> right = (Predicate<T>) getChild(1);
        return left.and(right).test(value);
    }

    /**
     * Performs logical AND operation using parameters and the value supplied
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
