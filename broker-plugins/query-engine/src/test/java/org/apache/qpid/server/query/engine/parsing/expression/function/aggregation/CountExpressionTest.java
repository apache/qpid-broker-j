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

/**
 * Tests designed to verify the public class {@link CountExpression} functionality
 */
public class CountExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void count()
    {
        String query = "select count(*) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.get(0).get("count(*)"));

        query = "select count(*) from queue where overflowPolicy = 'RING'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.get(0).get("count(*)"));

        query = "select count(description) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(30, result.get(0).get("count(description)"));

        query = "select count(distinct description) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.get(0).get("count(distinct description)"));

        query = "select count(distinct overflowPolicy) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(5, result.get(0).get("count(distinct overflowPolicy)"));

        query = "select count(distinct description), count(distinct overflowPolicy) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.get(0).get("count(distinct description)"));
        assertEquals(5, result.get(0).get("count(distinct overflowPolicy)"));

        query = "select count(*) + count(*) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(140, result.get(0).get("count(*)+count(*)"));

    }

    @Test()
    @SuppressWarnings("unchecked")
    public void countGroupBy()
    {
        String query = "select count(*), overflowPolicy from queue group by overflowPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(10, map.get("FLOW_TO_DISK"));
        assertEquals(10, map.get("RING"));
        assertEquals(10, map.get("REJECT"));
        assertEquals(10, map.get("PRODUCER_FLOW_CONTROL"));
        assertEquals(30, map.get("NONE"));

        query = "select count(description), overflowPolicy from queue group by overflowPolicy";
        result = _queryEvaluator.execute(query).getResults();

        map = (Map<String, Object>) result.get(0).get("count(description)");
        assertEquals(10, map.get("FLOW_TO_DISK"));
        assertEquals(10, map.get("RING"));
        assertEquals(10, map.get("REJECT"));
        assertEquals(0, map.get("PRODUCER_FLOW_CONTROL"));
        assertEquals(0, map.get("NONE"));

        query = "select count(*), count(description), overflowPolicy from queue group by overflowPolicy";
        result = _queryEvaluator.execute(query).getResults();

        map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(10, map.get("FLOW_TO_DISK"));
        assertEquals(10, map.get("RING"));
        assertEquals(10, map.get("REJECT"));
        assertEquals(10, map.get("PRODUCER_FLOW_CONTROL"));
        assertEquals(30, map.get("NONE"));

        map = (Map<String, Object>) result.get(0).get("count(description)");
        assertEquals(10, map.get("FLOW_TO_DISK"));
        assertEquals(10, map.get("RING"));
        assertEquals(10, map.get("REJECT"));
        assertEquals(0, map.get("PRODUCER_FLOW_CONTROL"));
        assertEquals(0, map.get("NONE"));

        query = "select count(*), count(description), overflowPolicy from queue group by overflowPolicy, expiryPolicy";
        result = _queryEvaluator.execute(query).getResults();

        map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(5, ((Map<String, Object>)map.get("FLOW_TO_DISK")).get("DELETE"));
        assertEquals(5, ((Map<String, Object>)map.get("FLOW_TO_DISK")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(5, ((Map<String, Object>)map.get("RING")).get("DELETE"));
        assertEquals(5, ((Map<String, Object>)map.get("RING")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(5, ((Map<String, Object>)map.get("REJECT")).get("DELETE"));
        assertEquals(5, ((Map<String, Object>)map.get("REJECT")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(5, ((Map<String, Object>)map.get("PRODUCER_FLOW_CONTROL")).get("DELETE"));
        assertEquals(5, ((Map<String, Object>)map.get("PRODUCER_FLOW_CONTROL")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(15, ((Map<String, Object>)map.get("NONE")).get("DELETE"));
        assertEquals(15, ((Map<String, Object>)map.get("NONE")).get("ROUTE_TO_ALTERNATE"));

        map = (Map<String, Object>) result.get(0).get("count(description)");
        assertEquals(5, ((Map<String, Object>)map.get("FLOW_TO_DISK")).get("DELETE"));
        assertEquals(5, ((Map<String, Object>)map.get("FLOW_TO_DISK")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(5, ((Map<String, Object>)map.get("RING")).get("DELETE"));
        assertEquals(5, ((Map<String, Object>)map.get("RING")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(5, ((Map<String, Object>)map.get("REJECT")).get("DELETE"));
        assertEquals(5, ((Map<String, Object>)map.get("REJECT")).get("ROUTE_TO_ALTERNATE"));
    }

    @Test()
    public void noArguments()
    {
        String query = "select count() from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'COUNT' requires at least 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void twoArguments()
    {
        String query = "select count(queueDepthMessages, queueDepthBytes) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function COUNT first argument must be 'distinct'", e.getMessage());
        }
    }

    @Test()
    public void threeArguments()
    {
        String query = "select count(id, queueDepthMessages, queueDepthBytes) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'COUNT' requires maximum 2 parameters", e.getMessage());
        }
    }

    @Test()
    public void subqueryInProjections()
    {
        String query = "select name, (select count(overflowPolicy) from queue) as cnt from broker";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.get(0).get("cnt"));

        query = "select name, (select count(distinct overflowPolicy) from queue) as cnt from broker";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(5, result.get(0).get("cnt"));
    }

    @Test()
    public void subqueryInWhere()
    {
        String query = "select min(name), max(name) from queue where queueDepthMessages > (select count(queueDepthMessages) from queue)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_60", result.get(0).get("min(name)"));
        assertEquals("QUEUE_69", result.get(0).get("max(name)"));
    }
}
