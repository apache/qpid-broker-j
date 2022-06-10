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
package org.apache.qpid.server.query.engine.parsing.expression.literal;

import java.util.function.Supplier;

import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;

/**
 * Holds a constant value.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ConstantExpression<T, R> extends AbstractExpressionNode<T, R> implements Supplier<R>
{
    /**
     * Constant value
     */
    private final R _value;

    /**
     * Constructor stores value
     *
     * @param value Constant value
     */
    public ConstantExpression(final R value)
    {
        super();
        _value = value;
    }

    /**
     * Constructor stores alias and value
     *
     * @param alias Expression alias
     * @param value Constant value
     */
    public ConstantExpression(final String alias, final R value)
    {
        super(alias);
        _value = value;
    }

    /**
     * Shorthand creation method
     *
     * @param value Constant value
     * @param <T> Input parameter type
     * @param <R> Output parameter type
     *
     * @return ConstantExpression instance
     */
    public static <T, R> ConstantExpression<T, R> of(final R value)
    {
        return new ConstantExpression<>(value);
    }

    /**
     * Shorthand creation method
     *
     * @param alias Expression alias
     * @param value Constant value
     * @param <T> Input parameter type
     * @param <R> Output parameter type
     *
     * @return ConstantExpression instance
     */
    public static <T, R> ConstantExpression<T, R> of(final String alias, final R value)
    {
        return new ConstantExpression<>(alias, value);
    }

    @Override
    public R apply(final T ignore)
    {
        return _value;
    }

    @Override
    public R get()
    {
        return _value;
    }

    /**
     * Alias getter
     *
     * @return Expression alias
     */
    @Override()
    public String getAlias()
    {
        return _metadata.getAlias() != null ? _metadata.getAlias() : toString();
    }

    @Override()
    public String toString()
    {
        return String.valueOf(_value);
    }
}
