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

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;

/**
 * Object accessor retrieves values from an object
 *
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ObjectAccessorExpression<T, R> extends AbstractExpressionNode<T, R>
{
    /**
     * Property name
     */
    private final String _property;

    /**
     * Constructor stores field values
     *
     * @param property Property name
     */
    public ObjectAccessorExpression(final String property)
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
            return (R) _property;
        }
        if (value == null)
        {
            return null;
        }
        if (_property.charAt(0) == '$')
        {
            return null;
        }
        try
        {
            final String property = _property.charAt(0) == '_' ? _property.substring(1) : _property;
            final String getter = "get" + property.substring(0, 1).toUpperCase(Locale.US) + property.substring(1);
            final PropertyDescriptor propertyDescriptor = new PropertyDescriptor(_property, value.getClass(), getter, null);
            final Method method = propertyDescriptor.getReadMethod();
            return (R) method.invoke(value);
        }
        catch (IllegalAccessException | IntrospectionException | InvocationTargetException e)
        {
            throw QueryEvaluationException.of(Errors.ACCESSOR.UNABLE_TO_ACCESS, e.getMessage());
        }
    }
}
