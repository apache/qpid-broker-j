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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.BetweenExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.EqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.GreaterThanExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.GreaterThanOrEqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.InExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.IsNullExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.LessThanExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.LessThanOrEqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.NotExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Tests designed to verify the {@link ComparisonExpressionFactory} functionality
 */
public class ComparisonExpressionFactoryTest
{
    @BeforeEach()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void betweenWithNullLeft()
    {
        try
        {
            ComparisonExpressionFactory.betweenExpression(null, null, ConstantExpression.of(1), ConstantExpression.of(2));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void betweenWithNullLow()
    {
        try
        {
            ComparisonExpressionFactory.betweenExpression(null, ConstantExpression.of(1), null, ConstantExpression.of(2));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void betweenWithNullHigh()
    {
        try
        {
            ComparisonExpressionFactory.betweenExpression(null, ConstantExpression.of(1), ConstantExpression.of(2), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void between()
    {
       ExpressionNode<T, Boolean> expression = ComparisonExpressionFactory.betweenExpression(
            null,
            ConstantExpression.of(1),
            ConstantExpression.of(2), ConstantExpression.of(3));
       assertEquals(BetweenExpression.class, expression.getClass());
    }

    @Test()
    public void equalWithNullLeft()
    {
        try
        {
            ComparisonExpressionFactory.equalExpression(null, ConstantExpression.of(1));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void equalWithNullRight()
    {
        try
        {
            ComparisonExpressionFactory.equalExpression(ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void equal()
    {
        Predicate<T> expression = ComparisonExpressionFactory.equalExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(EqualExpression.class, expression.getClass());
    }

    @Test()
    public void notEqualWithNullLeft()
    {
        try
        {
            ComparisonExpressionFactory.notEqualExpression(null, ConstantExpression.of(1));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void notEqualWithNullRight()
    {
        try
        {
            ComparisonExpressionFactory.notEqualExpression(ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void notEqual()
    {
        Predicate<T> expression = ComparisonExpressionFactory.notEqualExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(NotExpression.class, expression.getClass());
    }

    @Test()
    public void greaterThanWithNullLeft()
    {
        try
        {
            ComparisonExpressionFactory.greaterThanExpression(null, ConstantExpression.of(1));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void greaterThanWithNullRight()
    {
        try
        {
            ComparisonExpressionFactory.greaterThanExpression(ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void greaterThan()
    {
        Predicate<T> expression = ComparisonExpressionFactory.greaterThanExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(GreaterThanExpression.class, expression.getClass());
    }

    @Test()
    public void greaterThanOrEqualWithNullLeft()
    {
        try
        {
            ComparisonExpressionFactory.greaterThanOrEqualExpression(null, ConstantExpression.of(1));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void greaterThanOrEqualWithNullRight()
    {
        try
        {
            ComparisonExpressionFactory.greaterThanOrEqualExpression(ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void greaterThanOrEqual()
    {
        Predicate<T> expression = ComparisonExpressionFactory.greaterThanOrEqualExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(GreaterThanOrEqualExpression.class, expression.getClass());
    }

    @Test()
    public void lessThanWithNullLeft()
    {
        try
        {
            ComparisonExpressionFactory.lessThanExpression(null, ConstantExpression.of(1));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void lessThanWithNullRight()
    {
        try
        {
            ComparisonExpressionFactory.lessThanExpression(ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void lessThan()
    {
        Predicate<T> expression = ComparisonExpressionFactory.lessThanExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(LessThanExpression.class, expression.getClass());
    }

    @Test()
    public void lessThanOrEqualWithNullLeft()
    {
        try
        {
            ComparisonExpressionFactory.lessThanOrEqualExpression(null, ConstantExpression.of(1));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void lessThanOrEqualWithNullRight()
    {
        try
        {
            ComparisonExpressionFactory.lessThanOrEqualExpression(ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void lessThanOrEqual()
    {
        Predicate<T> expression = ComparisonExpressionFactory.lessThanOrEqualExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(LessThanOrEqualExpression.class, expression.getClass());
    }

    @Test()
    public void inWithNullLeft()
    {
        try
        {
            ComparisonExpressionFactory.inExpression(null,new ArrayList<>());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }

        try
        {
            ComparisonExpressionFactory.inExpression(null, new SelectExpression<>());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T, R> void inWithNullRight()
    {
        try
        {
            ComparisonExpressionFactory.inExpression(ConstantExpression.of(null), (List<ExpressionNode<T, R>>) null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSIONS_NULL, e.getMessage());
        }

        try
        {
            ComparisonExpressionFactory.inExpression(ConstantExpression.of(null), (SelectExpression<T, R>) null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void in()
    {
        Predicate<T> expression = ComparisonExpressionFactory.inExpression(
                ConstantExpression.of(1),
                new ArrayList<>());
        assertEquals(InExpression.class, expression.getClass());
    }

    @Test()
    public void isNullWithNullExpression()
    {
        try
        {
            ComparisonExpressionFactory.isNullExpression(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void isNull()
    {
        Predicate<T> expression = ComparisonExpressionFactory.isNullExpression(ConstantExpression.of(1));
        assertEquals(IsNullExpression.class, expression.getClass());
    }

    @Test()
    public void isNotNullWithNullExpression()
    {
        try
        {
            ComparisonExpressionFactory.isNotNullExpression(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void isNotNull()
    {
        Predicate<T> expression = ComparisonExpressionFactory.isNotNullExpression(ConstantExpression.of(1));
        assertEquals(NotExpression.class, expression.getClass());
    }

    @Test()
    public void likeWithNullExpression()
    {
        try
        {
            ComparisonExpressionFactory.likeExpression(null, "", "");
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }
}
