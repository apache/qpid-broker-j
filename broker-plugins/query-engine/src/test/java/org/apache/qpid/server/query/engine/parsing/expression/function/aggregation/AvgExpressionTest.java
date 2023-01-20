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
package org.apache.qpid.server.query.engine.parsing.expression.function.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link AvgExpression} functionality
 */
public class AvgExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void avg()
    {
        String query = "select round(avg(queueDepthMessages)) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(22.86, result.get(0).get("round(avg(queueDepthMessages))"));

        query = "select round(avg(queueDepthMessages)) - trunc(avg(queueDepthMessages)) as result from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(.01, result.get(0).get("result"));
    }

    @Test()
    public void avgArithmeticExpressions()
    {
        String query = "select round(avg(queueDepthMessages) + 10) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(32.86, result.get(0).get("round(avg(queueDepthMessages)+10)"));

        query = "select round(avg(queueDepthMessages) - 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(12.86, result.get(0).get("round(avg(queueDepthMessages)-10)"));

        query = "select round(avg(queueDepthMessages) * 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(228.57, result.get(0).get("round(avg(queueDepthMessages)*10)"));

        query = "select round(avg(queueDepthMessages) / 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(2.29, result.get(0).get("round(avg(queueDepthMessages)/10)"));

        query = "select round(avg(queueDepthMessages) % 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(2.86, result.get(0).get("round(avg(queueDepthMessages)%10)"));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void avgGroupBy()
    {
        String query = "select round(avg(queueDepthMessages)), overflowPolicy from queue group by overflowPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("round(avg(queueDepthMessages))");
        assertEquals(0, map.get("FLOW_TO_DISK"));
        assertEquals(0, map.get("RING"));
        assertEquals(0, map.get("REJECT"));
        assertEquals(0, map.get("PRODUCER_FLOW_CONTROL"));
        assertEquals(53.33, map.get("NONE"));

        query = "select round(avg(queueDepthMessages)), expiryPolicy from queue group by expiryPolicy";
        result = _queryEvaluator.execute(query).getResults();
        map = (Map<String, Object>) result.get(0).get("round(avg(queueDepthMessages))");
        assertEquals(22.86, map.get("DELETE"));
        assertEquals(22.86, map.get("ROUTE_TO_ALTERNATE"));

        query = "select round(avg(queueDepthMessages)), type from queue group by type";
        result = _queryEvaluator.execute(query).getResults();
        map = (Map<String, Object>) result.get(0).get("round(avg(queueDepthMessages))");
        assertEquals(22.86, map.get("standard"));

    }

    @Test()
    public void noArguments()
    {
        String query = "select avg() from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'AVG' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void twoArguments()
    {
        String query = "select avg(queueDepthMessages, queueDepthBytes) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'AVG' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void invalidArgumentType()
    {
        String query = "select avg(name) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameters of function 'AVG' invalid (invalid types: [String])", e.getMessage());
        }
    }

    @Test()
    public void subqueryInProjections()
    {
        String query = "select name, (select round(avg(queueDepthMessages)) from queue) as \"avg\" from broker";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(22.86, result.get(0).get("avg"));
    }

    @Test()
    public void subqueryInWhere()
    {
        String query = "select name from queue where queueDepthMessages > (select avg(queueDepthMessages) from queue)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(20, result.size());
        for (int i = 50; i < 70; i ++)
        {
            assertEquals("QUEUE_" + i, result.get(i - 50).get("name"));
        }
    }
}
