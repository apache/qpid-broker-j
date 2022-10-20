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
package org.apache.qpid.server.query.engine.parsing.expression.function.string;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link LengthExpression} functionality
 */
public class LengthExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void oneArgument()
    {
        String query = "select len('TEST') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));
    }

    @Test()
    public void twoArguments()
    {
        String query = "select len('hello', 'world') as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'LEN' requires 1 parameter", e.getMessage());
        }

        query = "select length('hello', 'world') as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'LENGTH' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void noArguments()
    {
        String query = "select len() as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'LEN' requires 1 parameter", e.getMessage());
        }

        query = "select length() as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'LENGTH' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void nullArgument()
    {
        String query = "select len(null) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));
    }

    @Test()
    public void integerArgument()
    {
        String query = "select len(1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));
    }

    @Test()
    public void longArgument()
    {
        String query = "select len(1L) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));
    }

    @Test()
    public void doubleArgument()
    {
        String query = "select len(2/3) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(8, result.get(0).get("result"));
    }

    @Test()
    public void bigDecimalArgument()
    {
        String query = "select len(" + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + ") as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(19, result.get(0).get("result"));
    }

    @Test()
    public void booleanArgument()
    {
        String query = "select len(true) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));
    }

    @Test()
    public void invalidArgumentType()
    {
        String query = "select len(statistics) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'LEN' invalid (parameter type: HashMap)", e.getMessage());
        }

        query = "select length(statistics) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'LENGTH' invalid (parameter type: HashMap)", e.getMessage());
        }
    }
}
