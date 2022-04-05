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
package org.apache.qpid.server.query.engine.parsing.expression.accessor;

import static org.apache.qpid.server.query.engine.evaluator.EvaluationContext.STATISTICS;

import java.util.UUID;

import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.model.ConfiguredObject;

/**
 * ConfiguredObject object accessor retrieves values from a broker objects hierarchy
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ConfiguredObjectAccessorExpression<T extends ConfiguredObject<?>, R> extends AbstractExpressionNode<T, R>
{
    /**
     * Object property name
     */
    private final String _property;

    /**
     * Constructor stores object property name
     *
     * @param property Object property name
     */
    public ConfiguredObjectAccessorExpression(final String property)
    {
        super();
        _property = property;
    }

    /**
     * Evaluates expression using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    @SuppressWarnings("unchecked")
    public R apply(final T value)
    {
        if ("*".equals(_property))
        {
            return (R) value;
        }
        if (STATISTICS.equals(_property))
        {
            return (R) value.getStatistics();
        }
        if (!value.getAttributeNames().contains(_property) && !value.getStatistics().containsKey(_property))
        {
            throw QueryEvaluationException.fieldNotFound(_property);
        }
        if (value.getAttributeNames().contains(_property))
        {
            final Object result = value.getAttribute(_property);

            if (result == null)
            {
                return (R) result;
            }
            if (result.getClass().equals(UUID.class))
            {
                return (R) result.toString();
            }
            if (result.getClass().isEnum())
            {
                return (R) result.toString();
            }
            return (R) result;
        }
        return (R) value.getStatistics().get(_property);
    }
}
