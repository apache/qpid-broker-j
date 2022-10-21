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
package org.apache.qpid.server.query.engine.parsing.expression.arithmetic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the arithmetic expressions functionality
 */
public class ArithmeticExpressionsTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void addition()
    {
        String query = "select 1 + 1";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("1+1"));

        query = "select (1 + 1)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("(1+1)"));

        query = "select (1 + 1) + 1";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(3, result.get(0).get("(1+1)+1"));

        query = "select (1 + 1) + (1 + 1)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("(1+1)+(1+1)"));

        query = "select (1 + 1), (2 + 2), 3 + 3";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.size());
        assertEquals(2, result.get(0).get("(1+1)"));
        assertEquals(4, result.get(0).get("(2+2)"));
        assertEquals(6, result.get(0).get("3+3"));
    }

    @Test()
    public void subtraction()
    {
        String query = "select 3 - 1";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("3-1"));

        query = "select (3 - 1)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("(3-1)"));

        query = "select (4 - 1) - 1";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("(4-1)-1"));
    }

    @Test()
    public void multiplication()
    {
        String query = "select 1 * 2";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("1*2"));

        query = "select (1 * 2)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("(1*2)"));

        query = "select (1 * 2) * 3";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(6, result.get(0).get("(1*2)*3"));
    }

    @Test()
    public void division()
    {
        String query = "select 1 / 2";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.5, result.get(0).get("1/2"));

        query = "select (1 / 2)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.5, result.get(0).get("(1/2)"));

        query = "select (1 / 2) / 2";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.25, result.get(0).get("(1/2)/2"));
    }

    @Test()
    public void operatorPrecedence()
    {
        String query = "select 1 + 2 * 3 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(7, result.get(0).get("result"));

        query = "select (1 + 2) * 3 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(9, result.get(0).get("result"));

        query = "select 1 + 2 * 3 - 4 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(3, result.get(0).get("result"));

        query = "select (1 + 2) * (3 - 4) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(-3, result.get(0).get("result"));
    }

    @Test()
    public void signChangingOperations()
    {
        String query = "select -1 - -2 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select -1 + -2 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(-3, result.get(0).get("result"));

        query = "select 0 * -1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select 0 / -1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));
    }

    @Test()
    public void fractionOperations()
    {
        String query = "select 2 / 3 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.666667, result.get(0).get("result"));

        query = "select 1 / 3 + 2 / 3 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select 1 / 3 + 1 / 3 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.666666, result.get(0).get("result"));

        query = "select 1 / 3 * 1 / 3 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.111111, result.get(0).get("result"));

        query = "select (1 / 3) / (1 / 3) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));
    }

    @Test()
    public void veryLongPlusExpression()
    {
        StringBuilder query = new StringBuilder("select ");
        for (int i = 0; i <= 1000; i ++)
        {
            query.append(i).append(" ");
            if (i != 1000)
            {
                query.append(" + ");
            }
        }
        query.append(" as result");
        List<Map<String, Object>> result = _queryEvaluator.execute(query.toString()).getResults();
        assertEquals(1, result.size());
        assertEquals(500500, result.get(0).get("result"));
    }

    @Test()
    public void veryLongMinusExpression()
    {
        StringBuilder query = new StringBuilder("select ");
        for (int i = 0; i <= 1000; i ++)
        {
            query.append(i).append(" ");
            if (i != 1000)
            {
                query.append(" - ");
            }
        }
        query.append(" as result");
        List<Map<String, Object>> result = _queryEvaluator.execute(query.toString()).getResults();
        assertEquals(1, result.size());
        assertEquals(-500500, result.get(0).get("result"));
    }

    @Test()
    public void veryLongMultiplicationExpression()
    {
        StringBuilder query = new StringBuilder("select ");
        for (int i = 1; i <= 1000; i ++)
        {
            query.append(i).append(" ");
            if (i != 1000)
            {
                query.append(" * ");
            }
        }
        query.append(" as result");
        try
        {
            _queryEvaluator.execute(query.toString());
            fail("Expected exception not thrown");
        }
        catch (QueryParsingException e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertTrue(e.getMessage().startsWith("Reached maximal allowed big decimal value"));
        }
    }

    @Test()
    public void veryLongConcatExpression()
    {
        String string = IntStream.range(0, 1001).mapToObj(String::valueOf).collect(Collectors.joining());
        StringBuilder query = new StringBuilder("select ");
        for (int i = 0; i <= 1000; i ++)
        {
            query.append("'").append(i).append("'").append(" ");
            if (i != 1000)
            {
                query.append(" + ");
            }
        }
        query.append(" as result");
        List<Map<String, Object>> result = _queryEvaluator.execute(query.toString()).getResults();
        assertEquals(1, result.size());
        assertEquals(string, result.get(0).get("result"));
    }

    @Test()
    public void zeroDivision()
    {
        String query = "select 1 / 0";
        try
        {
            _queryEvaluator.execute(query).getResults();
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Divisor is equal to zero", e.getMessage());
        }
    }
}
