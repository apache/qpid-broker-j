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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.NullLiteralExpression;

/**
 * Tests designed to verify the {@link UnaryExpressionFactory} functionality
 */
public class UnaryExpressionFactoryTest
{
    @Before()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void negateNull()
    {
        try
        {
            UnaryExpressionFactory.negate(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void negateNullLiteralExpression()
    {
        try
        {
            UnaryExpressionFactory.negate(new NullLiteralExpression<>());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Negation of 'NullLiteralExpression' not supported", e.getMessage());
        }
    }

    @Test()
    public <T> void negateZero()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDecimal("0");
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(0, negated.apply(null));
    }

    @Test()
    public <T> void negatePositiveInteger()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDecimal("100");
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(-100, negated.apply(null));
    }

    @Test()
    public <T> void negateNegativeInteger()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDecimal("-100");
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(100, negated.apply(null));
    }

    @Test()
    public <T> void negatePositiveLong()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDecimal("10000000000");
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(-10000000000L, negated.apply(null));
    }

    @Test()
    public <T> void negateNegativeLong()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDecimal("-10000000000");
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(10000000000L, negated.apply(null));
    }

    @Test()
    public <T> void negatePositiveDouble()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDouble(".99");
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(-.99, negated.apply(null));
    }

    @Test()
    public <T> void negateNegativeDouble()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDouble("-.99");
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(.99, negated.apply(null));
    }

    @Test()
    public <T> void negatePositiveBigDecimal()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDecimal(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE).toString());
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(-9223372036854775808L, negated.apply(null));
    }

    @Test()
    public <T> void negateNegativeBigDecimal()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDecimal(BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE).toString());
        ExpressionNode<T, Number> negated = UnaryExpressionFactory.negate(expression);
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.valueOf(2L)), negated.apply(null));
    }
}
