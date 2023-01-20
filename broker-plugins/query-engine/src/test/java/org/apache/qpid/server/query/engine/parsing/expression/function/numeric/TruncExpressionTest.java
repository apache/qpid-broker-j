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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link TruncExpression} functionality
 */
public class TruncExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void oneArgument()
    {
        String query = "select trunc(2/3) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.66, result.get(0).get("result"));
    }

    @Test()
    public void twoArguments()
    {
        String query = "select trunc(2/3, 1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.6, result.get(0).get("result"));

        query = "select trunc(2/3, 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.66, result.get(0).get("result"));

        query = "select trunc(2/3, 3) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.666, result.get(0).get("result"));

        query = "select trunc(2/3, 4) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.6666, result.get(0).get("result"));
    }

    @Test()
    public void noArguments()
    {
        String query = "select trunc() as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'TRUNC' requires at least 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void nullArgument()
    {
        String query = "select trunc(null) as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'TRUNC' invalid (parameter type: null)", e.getMessage());
        }
    }

    @Test()
    public void integerArgument()
    {
        String query = "select trunc(1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));
    }

    @Test()
    public void longArgument()
    {
        String query = "select trunc(1L) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));
    }

    @Test()
    public void doubleArgument()
    {
        String query = "select trunc(2/3) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.66, result.get(0).get("result"));
    }

    @Test()
    public void bigDecimalArgument()
    {
        String query = "select trunc(" + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + ", 0) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE), result.get(0).get("result"));
    }

    @Test()
    public void dateArgument()
    {
        String query = "select trunc(lastUpdatedTime) as result from queue where name='QUEUE_0'";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'TRUNC' invalid (parameter type: Date)", e.getMessage());
        }
    }

    @Test()
    public void booleanArgument()
    {
        String query = "select trunc(true) as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'TRUNC' invalid (parameter type: Boolean)", e.getMessage());
        }
    }

    @Test()
    public void invalidArgumentType()
    {
        String query = "select trunc(statistics) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'TRUNC' invalid (parameter type: HashMap)", e.getMessage());
        }
    }
}
