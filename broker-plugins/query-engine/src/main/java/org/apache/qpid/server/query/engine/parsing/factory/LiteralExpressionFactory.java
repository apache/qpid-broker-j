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
package org.apache.qpid.server.query.engine.parsing.factory;

import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.NumberLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.FalseLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.NullLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.StringLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.TrueLiteralExpression;

/**
 * Factory creating literal expressions
 */
public final class LiteralExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private LiteralExpressionFactory()
    {

    }

    /**
     * Creates a FalseLiteralExpression instance
     *
     * @param <T> Input parameter type
     *
     * @return FalseLiteralExpression instance
     */
    public static <T> FalseLiteralExpression<T> createFalse()
    {
        return new FalseLiteralExpression<>();
    }

    /**
     * Creates a TrueLiteralExpression instance
     *
     * @param <T> Input parameter type
     *
     * @return TrueLiteralExpression instance
     */
    public static <T> TrueLiteralExpression<T> createTrue()
    {
        return new TrueLiteralExpression<>();
    }

    /**
     * Creates a NullLiteralExpression instance
     *
     * @param <T> Input parameter type
     *
     * @return NullLiteralExpression instance
     */
    public static <T, R> NullLiteralExpression<T, R> createNull()
    {
        return new NullLiteralExpression<>();
    }

    /**
     * Creates a StringLiteralExpression instance
     *
     * @param <T> Input parameter type
     *
     * @return StringLiteralExpression instance
     */
    public static <T> StringLiteralExpression<T> string(final String source)
    {
        return new StringLiteralExpression<>(source);
    }

    /**
     * Creates a NumberLiteralExpression instance from decimal source
     *
     * @param <T> Input parameter type
     *
     * @return NumberLiteralExpression instance
     */
    public static <T> NumberLiteralExpression<T> fromDecimal(final String source)
    {
        return new NumberLiteralExpression<>(NumberExpressionFactory.fromDecimal(source));
    }

    /**
     * Creates a NumberLiteralExpression instance from hex source
     *
     * @param <T> Input parameter type
     *
     * @return NumberLiteralExpression instance
     */
    public static <T> ExpressionNode<T, Number> fromHex(final String source)
    {
        return new NumberLiteralExpression<>(NumberExpressionFactory.fromHex(source));
    }

    /**
     * Creates a NumberLiteralExpression instance from octal source
     *
     * @param <T> Input parameter type
     *
     * @return NumberLiteralExpression instance
     */
    public static <T> ExpressionNode<T, Number> fromOctal(final String source)
    {
        return new NumberLiteralExpression<>(NumberExpressionFactory.fromOctal(source));
    }

    /**
     * Creates a NumberLiteralExpression instance from double source
     *
     * @param <T> Input parameter type
     *
     * @return NumberLiteralExpression instance
     */
    public static <T> ExpressionNode<T, Number> fromDouble(final String source)
    {
        return new NumberLiteralExpression<>(NumberExpressionFactory.fromDouble(source));
    }
}
