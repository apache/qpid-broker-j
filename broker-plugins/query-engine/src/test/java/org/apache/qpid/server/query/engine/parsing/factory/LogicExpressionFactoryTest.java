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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.AndExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.NotExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.OrExpression;

/**
 * Tests designed to verify the {@link LogicExpressionFactory} functionality
 */
public class LogicExpressionFactoryTest
{
    @Before()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void andWithChildExpressionNull()
    {
        try
        {
            LogicExpressionFactory.and(null, null, null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }

        try
        {
            LogicExpressionFactory.and(null, ConstantExpression.of(true), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void and()
    {
        final ExpressionNode<T, Boolean> expression = LogicExpressionFactory.and(null, ConstantExpression.of(true), ConstantExpression.of(true));
        assertEquals(AndExpression.class, expression.getClass());
    }

    @Test()
    public void orWithChildExpressionNull()
    {
        try
        {
            LogicExpressionFactory.or(null, null, null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }

        try
        {
            LogicExpressionFactory.or(null, ConstantExpression.of(true), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void or()
    {
        final ExpressionNode<T, Boolean> expression = LogicExpressionFactory.or(null, ConstantExpression.of(true), ConstantExpression.of(true));
        assertEquals(OrExpression.class, expression.getClass());
    }

    @Test()
    public void negateWithChildExpressionNull()
    {
        try
        {
            LogicExpressionFactory.negate(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void negate()
    {
        final ExpressionNode<T, Boolean> expression = LogicExpressionFactory.or(null, ConstantExpression.of(true), ConstantExpression.of(true));
        final ExpressionNode<T, Boolean> negated = LogicExpressionFactory.negate(expression);
        assertEquals(NotExpression.class, negated.getClass());

        assertEquals(NotExpression.class, LogicExpressionFactory.negate(LiteralExpressionFactory.createTrue()).getClass());
        assertEquals(NotExpression.class, LogicExpressionFactory.negate(LiteralExpressionFactory.createFalse()).getClass());
    }

    @Test()
    public void negateNotPredicate()
    {
        try
        {
            LogicExpressionFactory.negate(LiteralExpressionFactory.createNull());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Negation of 'NullLiteralExpression' not supported", e.getMessage());
        }
    }

    @Test()
    public void toPredicateWithChildExpressionNull()
    {
        try
        {
            LogicExpressionFactory.toPredicate(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void toFunctionWithChildExpressionNull()
    {
        try
        {
            LogicExpressionFactory.toFunction(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }
}
