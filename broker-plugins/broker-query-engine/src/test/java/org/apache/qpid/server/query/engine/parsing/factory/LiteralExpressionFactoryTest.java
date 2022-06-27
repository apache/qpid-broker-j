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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.FalseLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.NullLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.NumberLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.StringLiteralExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.TrueLiteralExpression;

/**
 * Tests designed to verify the {@link LiteralExpressionFactory} functionality
 */
public class LiteralExpressionFactoryTest
{
    @Before()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public <T> void falseLiteralExpression()
    {
        ExpressionNode<T, Boolean> expression = LiteralExpressionFactory.createFalse();
        assertEquals(FalseLiteralExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void nullLiteralExpression()
    {
        ExpressionNode<T, R> expression = LiteralExpressionFactory.createNull();
        assertEquals(NullLiteralExpression.class, expression.getClass());
    }

    @Test()
    public <T> void trueLiteralExpression()
    {
        ExpressionNode<T, Boolean> expression = LiteralExpressionFactory.createTrue();
        assertEquals(TrueLiteralExpression.class, expression.getClass());
    }

    @Test()
    public <T> void fromDecimal()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDecimal("0");
        assertEquals(NumberLiteralExpression.class, expression.getClass());
        assertEquals(0, expression.apply(null));
    }

    @Test()
    public <T> void fromHex()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromHex("0x0");
        assertEquals(NumberLiteralExpression.class, expression.getClass());
        assertEquals(0, expression.apply(null));
    }

    @Test()
    public <T> void fromOctal()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromOctal("01");
        assertEquals(NumberLiteralExpression.class, expression.getClass());
        assertEquals(1, expression.apply(null));
    }

    @Test()
    public <T> void fromDouble()
    {
        ExpressionNode<T, Number> expression = LiteralExpressionFactory.fromDouble("0.0");
        assertEquals(NumberLiteralExpression.class, expression.getClass());
        assertEquals(0, expression.apply(null));
    }

    @Test()
    public <T> void string()
    {
        ExpressionNode<T, String> expression = LiteralExpressionFactory.string("'test'");
        assertEquals(StringLiteralExpression.class, expression.getClass());
        assertEquals("'test'", expression.apply(null));
    }
}
