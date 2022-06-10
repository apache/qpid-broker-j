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

public class ArithmeticExpressionFactoryTest
{
    @Before()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void divideWithLeftNull()
    {
        try
        {
            ArithmeticExpressionFactory.divide("", null, null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void divideWithRightNull()
    {
        try
        {
            ArithmeticExpressionFactory.divide("", ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void divideWithZeroDivision()
    {
        try
        {
            ArithmeticExpressionFactory.divide("", ConstantExpression.of(1), ConstantExpression.of(0));
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals(Errors.ARITHMETIC.ZERO_DIVISION, e.getMessage());
        }
    }

    @Test()
    public <T> void divide()
    {
        ExpressionNode<T, Integer> expression = ArithmeticExpressionFactory.divide("", ConstantExpression.of(1), ConstantExpression.of(1));
        assertEquals(ConstantExpression.class, expression.getClass());
    }

    @Test()
    public void minusWithLeftNull()
    {
        try
        {
            ArithmeticExpressionFactory.minus("", null, null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void minusWithRightNull()
    {
        try
        {
            ArithmeticExpressionFactory.minus("", ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void minus()
    {
        ExpressionNode<T, Integer> expression = ArithmeticExpressionFactory.minus("", ConstantExpression.of(1), ConstantExpression.of(1));
        assertEquals(ConstantExpression.class, expression.getClass());
    }

    @Test()
    public void modWithLeftNull()
    {
        try
        {
            ArithmeticExpressionFactory.mod("", null, null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void modWithRightNull()
    {
        try
        {
            ArithmeticExpressionFactory.mod("", ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void mod()
    {
        ExpressionNode<T, Integer> expression = ArithmeticExpressionFactory.mod("", ConstantExpression.of(1), ConstantExpression.of(1));
        assertEquals(ConstantExpression.class, expression.getClass());
    }

    @Test()
    public void multiplyWithLeftNull()
    {
        try
        {
            ArithmeticExpressionFactory.multiply("", null, null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void multiplyWithRightNull()
    {
        try
        {
            ArithmeticExpressionFactory.multiply("", ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void multiply()
    {
        ExpressionNode<T, Integer> expression = ArithmeticExpressionFactory.multiply("", ConstantExpression.of(1), ConstantExpression.of(1));
        assertEquals(ConstantExpression.class, expression.getClass());
    }

    @Test()
    public void plusWithLeftNull()
    {
        try
        {
            ArithmeticExpressionFactory.plus("", null, null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void plusWithRightNull()
    {
        try
        {
            ArithmeticExpressionFactory.plus("", ConstantExpression.of(1), null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public <T> void plus()
    {
        ExpressionNode<T, Integer> expression = ArithmeticExpressionFactory.plus("", ConstantExpression.of(1), ConstantExpression.of(1));
        assertEquals(ConstantExpression.class, expression.getClass());
    }
}
