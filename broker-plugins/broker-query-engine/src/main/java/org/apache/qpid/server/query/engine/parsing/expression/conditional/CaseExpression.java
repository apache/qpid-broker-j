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
package org.apache.qpid.server.query.engine.parsing.expression.conditional;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;

/**
 * Conditional CASE operation. It goes through conditions and returns a value when the first condition is met
 * (like an if-then-else statement). So, once a condition is true, it will stop reading and return the result.
 * If no conditions are true, it returns the value in the ELSE clause.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class CaseExpression<T, R> extends AbstractExpressionNode<T, R>
{
    /**
     * List of conditions
     */
    final List<ExpressionNode<T,R>> _conditions;

    /**
     * List of outcomes
     */
    final List<ExpressionNode<T,R>> _outcomes;

    /**
     * Constructor initializes children expression list
     *
     * @param conditions List of condition expressions
     * @param outcomes List of outcome expressions
     */
    @SuppressWarnings("findbugs:EI_EXPOSE_REP2")
    // list of conditions and list of outcomes are stored intentionally
    public CaseExpression(final List<ExpressionNode<T,R>> conditions, final List<ExpressionNode<T,R>> outcomes)
    {
        super();
        if (conditions.isEmpty())
        {
            throw QueryValidationException.of(Errors.VALIDATION.CASE_CONDITIONS_EMPTY);
        }
        if (outcomes.isEmpty())
        {
            throw QueryValidationException.of(Errors.VALIDATION.CASE_OUTCOMES_EMPTY);
        }
        _conditions = conditions;
        _outcomes = outcomes;
        final String alias = (IntStream.range(0, conditions.size()).boxed()
            .map(i -> "when " + conditions.get(i).getAlias() + " then " + outcomes.get(i).getAlias() + " ")
            .collect(Collectors.joining(""))) + "else " + outcomes.get(outcomes.size() - 1).getAlias();
        _metadata.setAlias("case " + alias + " end");
    }

    /**
     * Performs CASE operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Boolean result of value evaluation
     */
    @Override
    public R apply(final T value)
    {
        for (int i = 0; i < _conditions.size(); i++)
        {
            final Expression<T,R> condition = _conditions.get(i);
            if (Objects.equals(Boolean.TRUE, condition.apply(value)))
            {
                return _outcomes.get(i).apply(value);
            }
        }
        return _outcomes.get(_outcomes.size() - 1).apply(value);
    }
}
