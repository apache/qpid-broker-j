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

import java.util.Map;

import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;

/**
 * Delegating object accessor delegates value retrieval to other accessor types
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class DelegatingObjectAccessor<T, R> extends AbstractExpressionNode<T, R>
{
    /**
     * Object property name
     */
    private final String _property;

    /**
     * Constructor stores alias and object property name
     *
     * @param alias Expression alias
     * @param property Object property name
     */
    public DelegatingObjectAccessor(final String alias, final String property)
    {
        super(alias);
        _metadata.setAccessor(true);
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
        if (ctx().containsAlias(_property))
        {
            final Expression<T, R> expression = ctx().removeAlias(_property);
            final R result = expression.apply(value);
            ctx().putAlias(_property, expression);
            return result;
        }
        if (value instanceof ConfiguredObject<?>)
        {
            final ConfiguredObject<?> configuredObject = (ConfiguredObject<?>) value;
            return new ConfiguredObjectAccessorExpression<ConfiguredObject<?>, R>(_property).apply(configuredObject);
        }
        if (value instanceof Map)
        {
            final Map<String, R> map = (Map<String, R>) value;
            return new MapObjectAccessor<R>(_property).apply(map);
        }
        return (R) new ObjectAccessorExpression<>(_property).apply(value);
    }

    @Override
    public String getAlias()
    {
        return _property;
    }

    @Override
    public String toString()
    {
        return getAlias();
    }
}
