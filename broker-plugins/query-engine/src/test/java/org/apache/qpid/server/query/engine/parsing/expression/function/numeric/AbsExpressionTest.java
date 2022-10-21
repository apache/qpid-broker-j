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
package org.apache.qpid.server.query.engine.parsing.expression.function.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the public class {@link AbsExpression} functionality
 */
public class AbsExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void argInteger()
    {
        String query = "select abs(1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select abs(-1) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select abs(" + Integer.MIN_VALUE + ") as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals((long) Integer.MAX_VALUE + 1, result.get(0).get("result"));
    }

    @Test()
    public void argLong()
    {
        String query = "select abs(1L) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select abs(-1L) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select abs(" + Long.MIN_VALUE + ") as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE), result.get(0).get("result"));
    }

    @Test()
    public void argDouble()
    {
        String query = "select abs(1.0) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select abs(-1.0) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select abs(" + -Double.MAX_VALUE + ") as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(new BigDecimal(BigDecimal.valueOf(-Double.MAX_VALUE).abs().toPlainString()), result.get(0).get("result"));
    }

    @Test()
    public void argBigDecimal()
    {
        String query = "select abs(" + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.TEN) + ") as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN).add(BigDecimal.ONE), result.get(0).get("result"));
    }

    @Test()
    public void noArguments()
    {
        String query = "select abs() as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'ABS' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void nullArgument()
    {
        String query = "select abs(null) as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'ABS' invalid (parameter type: null)", e.getMessage());
        }
    }

    @Test()
    public void dateArgument()
    {
        String query = "select abs(lastUpdatedTime) as result from queue where name='QUEUE_0'";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'ABS' invalid (parameter type: Date)", e.getMessage());
        }
    }

    @Test()
    public void booleanArgument()
    {
        String query = "select abs(true) as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'ABS' invalid (parameter type: Boolean)", e.getMessage());
        }
    }

    @Test()
    public void invalidArgumentType()
    {
        String query = "select abs(statistics) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'ABS' invalid (parameter type: HashMap)", e.getMessage());
        }
    }
}
