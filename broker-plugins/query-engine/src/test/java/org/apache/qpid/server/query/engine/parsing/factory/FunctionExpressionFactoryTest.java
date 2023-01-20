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

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.AvgExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.CountExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.MaxExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.MinExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.SumExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.CurrentTimestampExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.DateAddExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.DateDiffExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.DateExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.datetime.ExtractExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.nulls.CoalesceExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.numeric.AbsExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.numeric.RoundExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.numeric.TruncExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.ConcatExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.LeftTrimExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.LengthExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.LowerExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.PositionExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.ReplaceExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.RightTrimExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.SubstringExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.TrimExpression;
import org.apache.qpid.server.query.engine.parsing.expression.function.string.UpperExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;

/**
 * Tests designed to verify the {@link FunctionExpressionFactory} functionality
 */
public class FunctionExpressionFactoryTest
{
    @BeforeEach()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public <T, R> void abs()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("abs(0)", "ABS", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(AbsExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void avg()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("avg(0)", "AVG", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(AvgExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void coalesce()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("coalesce(0)", "COALESCE", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(CoalesceExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void concat()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("concat(0)", "CONCAT", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(ConcatExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void count()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("count(0)", "COUNT", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(CountExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void currentTimestamp()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("current_timestamp()", "CURRENT_TIMESTAMP", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(CurrentTimestampExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void date()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("date()", "DATE", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(DateExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void dateAdd()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction(
        "dateadd()", "DATEADD",
            Arrays.asList(ConstantExpression.of(0), ConstantExpression.of(0), ConstantExpression.of(0)));
        assertEquals(DateAddExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void dateDiff()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("datediff()", "DATEDIFF",
            Arrays.asList(ConstantExpression.of(0), ConstantExpression.of(0), ConstantExpression.of(0)));
        assertEquals(DateDiffExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void extract()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("extract()", "EXTRACT",
            Arrays.asList(ConstantExpression.of(0), ConstantExpression.of(0)));
        assertEquals(ExtractExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void len()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("len()", "LEN", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(LengthExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void length()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("length()", "LENGTH", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(LengthExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void lower()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("lower()", "LOWER", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(LowerExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void ltrim()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("ltrim()", "LTRIM", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(LeftTrimExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void max()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("max()", "MAX", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(MaxExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void min()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("min()", "MIN", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(MinExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void position()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("position()", "POSITION",
            Arrays.asList(ConstantExpression.of(0), ConstantExpression.of(0)));
        assertEquals(PositionExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void replace()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("replace()", "REPLACE",
            Arrays.asList(ConstantExpression.of(0), ConstantExpression.of(0), ConstantExpression.of(0)));
        assertEquals(ReplaceExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void round()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("round()", "ROUND", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(RoundExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void rtrim()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("rtrim()", "RTRIM", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(RightTrimExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void substr()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("substr()", "SUBSTR",
            Arrays.asList(ConstantExpression.of(0), ConstantExpression.of(0)));
        assertEquals(SubstringExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void substring()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("substring()", "SUBSTRING",
            Arrays.asList(ConstantExpression.of(0), ConstantExpression.of(0)));
        assertEquals(SubstringExpression.class, expression.getClass());
    }

    @Test()
    public <T, R>void sum()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("sum()", "SUM", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(SumExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void trim()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("trim()", "TRIM", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(TrimExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void trunc()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("trunc()", "TRUNC", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(TruncExpression.class, expression.getClass());
    }

    @Test()
    public <T, R> void upper()
    {
        ExpressionNode<T, R> expression = FunctionExpressionFactory.createFunction("upper()", "UPPER", Collections.singletonList(ConstantExpression.of(0)));
        assertEquals(UpperExpression.class, expression.getClass());
    }

    @Test()
    public void functionNotFound()
    {
        try
        {
            FunctionExpressionFactory.createFunction("", "nonExistingFunction", Collections.emptyList());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'nonExistingFunction' not found", e.getMessage());
        }
    }

    @Test()
    public void functionNameNull()
    {
        try
        {
            FunctionExpressionFactory.createFunction("", null, Collections.emptyList());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.FUNCTION_NAME_NULL, e.getMessage());
        }
    }

    @Test()
    public void argsNull()
    {
        try
        {
            FunctionExpressionFactory.createFunction("", "CURRENT_TIMESTAMP", null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.FUNCTION_ARGS_NULL, e.getMessage());
        }
    }
}
