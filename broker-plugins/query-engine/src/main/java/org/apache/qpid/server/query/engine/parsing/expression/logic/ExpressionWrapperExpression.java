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

/**
 * Wraps predicate into an expression.
 *
 * @param <T> Input parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ExpressionWrapperExpression<T> extends AbstractExpressionNode<T, Boolean>
{
    /**
     * Predicate
     */
    private final Predicate<T> _predicate;

    /**
     * Constructor stores the predicate value
     *
     * @param predicate Predicate to evaluate
     */
    public ExpressionWrapperExpression(final Predicate<T> predicate)
    {
        super();
        _predicate = predicate;
    }

    /**
     * Calls stored predicate using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    public Boolean apply(final T value)
    {
        return _predicate.test(value);
    }
}
