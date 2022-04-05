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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

public class SumExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void sum()
    {
        String query = "select sum(queueDepthMessages) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1600, result.get(0).get("sum(queueDepthMessages)"));

        query = "select sum(statistics['totalExpiredBytes']) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.get(0).get("sum(statistics['totalExpiredBytes'])"));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void sumGroupBy()
    {
        String query = "select sum(queueDepthMessages) from queue group by overflowPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("sum(queueDepthMessages)");
        assertEquals(0, map.get("FLOW_TO_DISK"));
        assertEquals(0, map.get("RING"));
        assertEquals(0, map.get("REJECT"));
        assertEquals(0, map.get("PRODUCER_FLOW_CONTROL"));
        assertEquals(1600, map.get("NONE"));

        query = "select sum(queueDepthMessages), overflowPolicy from queue group by overflowPolicy";
        result = _queryEvaluator.execute(query).getResults();

        map = (Map<String, Object>) result.get(0).get("sum(queueDepthMessages)");
        assertEquals(0, map.get("FLOW_TO_DISK"));
        assertEquals(0, map.get("RING"));
        assertEquals(0, map.get("REJECT"));
        assertEquals(0, map.get("PRODUCER_FLOW_CONTROL"));
        assertEquals(1600, map.get("NONE"));

        query = "select round(sum(queueDepthMessages)), overflowPolicy, expiryPolicy from queue group by overflowPolicy, expiryPolicy";
        result = _queryEvaluator.execute(query).getResults();

        map = (Map<String, Object>) result.get(0).get("round(sum(queueDepthMessages))");
        assertEquals(0, ((Map<String, Object>) map.get("FLOW_TO_DISK")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>) map.get("FLOW_TO_DISK")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(0, ((Map<String, Object>) map.get("RING")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>) map.get("RING")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(0, ((Map<String, Object>) map.get("REJECT")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>) map.get("REJECT")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(0, ((Map<String, Object>) map.get("PRODUCER_FLOW_CONTROL")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>) map.get("PRODUCER_FLOW_CONTROL")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(800, ((Map<String, Object>) map.get("NONE")).get("DELETE"));
        assertEquals(800, ((Map<String, Object>) map.get("NONE")).get("ROUTE_TO_ALTERNATE"));
    }

    @Test()
    public void noArguments()
    {
        String query = "select sum() from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'SUM' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void twoArguments()
    {
        String query = "select sum(queueDepthMessages, queueDepthBytes) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'SUM' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void subqueryInProjections()
    {
        String query = "select name, (select sum(queueDepthMessages) from queue) as s from broker";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1600, result.get(0).get("s"));

        query = "select name, (select sum(queueDepthBytes) from queue) as s from broker";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.get(0).get("s"));
    }

    @Test()
    public void subqueryInWhere()
    {
        String query = "select name, bindingCount from queue where bindingCount = (select sum(bindingCount) from queue)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_1", result.get(0).get("name"));
    }

    @Test()
    public void sumArithmeticExpressions()
    {
        String query = "select round(sum(queueDepthMessages) + 10) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1610, result.get(0).get("round(sum(queueDepthMessages)+10)"));

        query = "select round(sum(queueDepthMessages) - 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1590, result.get(0).get("round(sum(queueDepthMessages)-10)"));

        query = "select round(sum(queueDepthMessages) * 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(16000, result.get(0).get("round(sum(queueDepthMessages)*10)"));

        query = "select round(sum(queueDepthMessages) / 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(160, result.get(0).get("round(sum(queueDepthMessages)/10)"));

        query = "select round(sum(queueDepthMessages) % 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.get(0).get("round(sum(queueDepthMessages)%10)"));
    }
}
