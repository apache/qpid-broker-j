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
 * Wraps an expression into a predicate.
 *
 * @param <T> Input parameter type
 */
public class PredicateWrapperExpression<T, R> extends AbstractExpressionNode<T, Boolean> implements Predicate<T>
{
    /**
     * Constructor stores the expression value
     *
     * @param expression Expression to evaluate
     */
    public PredicateWrapperExpression(final ExpressionNode<T, R> expression)
    {
        super(expression);
    }

    /**
     * Calls stored expression using parameters and the value supplied
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

    /**
     * Calls stored expression using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    public Boolean apply(final T value)
    {
        final R result = evaluateChild(0, value);
        if (result == null)
        {
            return Boolean.FALSE;
        }
        if (result instanceof Boolean)
        {
            return (Boolean) result ;
        }
        return Boolean.FALSE;
    }
}
