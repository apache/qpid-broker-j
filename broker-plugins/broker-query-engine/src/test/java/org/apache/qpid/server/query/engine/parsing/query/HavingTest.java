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
package org.apache.qpid.server.query.engine.parsing.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;

/**
 * Tests designed to verify the {@link HavingExpression} functionality
 */
@SuppressWarnings("unchecked")
public class HavingTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void havingNoneAggregation()
    {
        String query = "select overflowPolicy, count(*) from queue group by overflowPolicy having overflowPolicy = 'NONE'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(1, map.size());
        assertEquals(30, map.get("NONE"));

        query = "select overflowPolicy, round(avg(queueDepthMessages)) from queue group by overflowPolicy having lifetimePolicy = 'IN_USE'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        map = (Map<String, Object>) result.get(0).get("round(avg(queueDepthMessages))");
        assertEquals(1, map.size());
        assertEquals(32.5, map.get("NONE"));
    }

    @Test()
    public void havingSameAggregation()
    {
        String query = "select overflowPolicy, count(*) from queue group by overflowPolicy having count(queueDepthMessages) > 10";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(1, map.size());
        assertEquals(30, map.get("NONE"));

        query = "select overflowPolicy, round(avg(queueDepthMessages)) from queue group by overflowPolicy having round(avg(queueDepthMessages)) > 10";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        map = (Map<String, Object>) result.get(0).get("round(avg(queueDepthMessages))");
        assertEquals(1, map.size());
        assertEquals(53.33, map.get("NONE"));
    }

    @Test()
    public void havingSameAggregationGroupByTwoFields()
    {
        String query = "select overflowPolicy, expiryPolicy, count(*) "
            + "from queue "
            + "group by overflowPolicy, expiryPolicy "
            + "having count(queueDepthMessages) > 10";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(1, map.size());
        assertEquals(15, ((Map<String, Object>)map.get("NONE")).get("DELETE"));
        assertEquals(15, ((Map<String, Object>)map.get("NONE")).get("ROUTE_TO_ALTERNATE"));
    }

    @Test()
    public void havingSameAggregationAndCondition()
    {
        String query = "select overflowPolicy, count(*) from queue group by overflowPolicy having count(queueDepthMessages) > 10 and lifetimePolicy = 'IN_USE'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(1, map.size());
        assertEquals(10, map.get("NONE"));

        query = "select overflowPolicy, count(*) from queue group by overflowPolicy having count(queueDepthMessages) > 10 or lifetimePolicy = 'IN_USE'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(1, map.size());
        assertEquals(30, map.get("NONE"));
    }

    @Test()
    public void havingSameAggregationGroupByThreeFields()
    {
        String query = "select overflowPolicy, expiryPolicy, lifetimePolicy, count(*) "
            + "from queue "
            + "group by overflowPolicy, expiryPolicy, lifetimePolicy "
            + "having count(queueDepthMessages) >= 10";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(1, map.size());
        assertEquals(10, ((Map<String, Object>)((Map<String, Object>)map.get("NONE")).get("DELETE")).get("IN_USE"));
        assertEquals(15, ((Map<String, Object>)((Map<String, Object>)map.get("NONE")).get("ROUTE_TO_ALTERNATE")).get("PERMANENT"));
    }

    @Test()
    public void havingAnotherAggregation()
    {
        String query = "select overflowPolicy, count(*) from queue group by overflowPolicy having sum(queueDepthMessages) > 1000";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(1, map.size());
        assertEquals(30, map.get("NONE"));
    }

    @Test()
    public void havingIsNull()
    {
        String query = "select count(*) as result from queue having description is null";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(40, result.get(0).get("result"));

        query = "select count(*) as result from queue having description is not null";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(30, result.get(0).get("result"));
    }

    @Test()
    public void havingWithoutAggregation()
    {
        try
        {
            String query = "select id, name from queue having queueDepthMessages > 100";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("HAVING clause is allowed when using aggregation", e.getMessage());
        }
    }
}
