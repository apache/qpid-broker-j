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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.query.engine.QueryEngine;
import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.DefaultQuerySettings;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the ordering functionality
 */
public class OrderByTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void orderQueuesByDefault()
    {
        String query = "select * from queue limit 10 offset 0";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        assertEquals(10, result.size());
        assertEquals("QUEUE_0", result.get(0).get("name"));
        assertEquals("QUEUE_1", result.get(1).get("name"));
        assertEquals("QUEUE_10", result.get(2).get("name"));
        assertEquals("QUEUE_11", result.get(3).get("name"));
        assertEquals("QUEUE_12", result.get(4).get("name"));
        assertEquals("QUEUE_13", result.get(5).get("name"));
        assertEquals("QUEUE_14", result.get(6).get("name"));
        assertEquals("QUEUE_15", result.get(7).get("name"));
        assertEquals("QUEUE_16", result.get(8).get("name"));
        assertEquals("QUEUE_17", result.get(9).get("name"));
    }

    @Test()
    public void orderExchangesByDefault()
    {
        String query = "select * from exchange limit 10 offset 0";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        assertEquals("EXCHANGE_0", result.get(0).get("name"));
        assertEquals("EXCHANGE_1", result.get(1).get("name"));
        assertEquals("EXCHANGE_2", result.get(2).get("name"));
        assertEquals("EXCHANGE_3", result.get(3).get("name"));
        assertEquals("EXCHANGE_4", result.get(4).get("name"));
        assertEquals("EXCHANGE_5", result.get(5).get("name"));
        assertEquals("EXCHANGE_6", result.get(6).get("name"));
        assertEquals("EXCHANGE_7", result.get(7).get("name"));
        assertEquals("EXCHANGE_8", result.get(8).get("name"));
        assertEquals("EXCHANGE_9", result.get(9).get("name"));
    }

    @Test()
    public void orderAscBySingleFieldName()
    {
        String query = "select * from queue order by name limit 10 offset 0";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_0", result.get(0).get("name"));
        assertEquals("QUEUE_1", result.get(1).get("name"));
        assertEquals("QUEUE_10", result.get(2).get("name"));
        assertEquals("QUEUE_11", result.get(3).get("name"));
        assertEquals("QUEUE_12", result.get(4).get("name"));
        assertEquals("QUEUE_13", result.get(5).get("name"));
        assertEquals("QUEUE_14", result.get(6).get("name"));
        assertEquals("QUEUE_15", result.get(7).get("name"));
        assertEquals("QUEUE_16", result.get(8).get("name"));
        assertEquals("QUEUE_17", result.get(9).get("name"));

        query = "select * from queue order by name limit 10 offset 10";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_18", result.get(0).get("name"));
        assertEquals("QUEUE_26", result.get(9).get("name"));

        query = "select * from queue order by name limit 10 offset 20";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_27", result.get(0).get("name"));
        assertEquals("QUEUE_35", result.get(9).get("name"));

        query = "select * from queue order by name limit 10 offset 60";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_63", result.get(0).get("name"));
        assertEquals("QUEUE_9", result.get(9).get("name"));

        query = "select * from queue order by name limit 10 offset 70";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());
    }

    @Test()
    public void orderAscBySingleFieldOrdinal()
    {
        String query = "select id, name from queue order by 2 limit 10 offset 0";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_0", result.get(0).get("name"));
        assertEquals("QUEUE_1", result.get(1).get("name"));
        assertEquals("QUEUE_10", result.get(2).get("name"));
        assertEquals("QUEUE_11", result.get(3).get("name"));
        assertEquals("QUEUE_12", result.get(4).get("name"));
        assertEquals("QUEUE_13", result.get(5).get("name"));
        assertEquals("QUEUE_14", result.get(6).get("name"));
        assertEquals("QUEUE_15", result.get(7).get("name"));
        assertEquals("QUEUE_16", result.get(8).get("name"));
        assertEquals("QUEUE_17", result.get(9).get("name"));

        query = "select id, name from queue order by 2 limit 10 offset 10";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_18", result.get(0).get("name"));
        assertEquals("QUEUE_26", result.get(9).get("name"));

        query = "select * from queue order by name limit 10 offset 20";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_27", result.get(0).get("name"));
        assertEquals("QUEUE_35", result.get(9).get("name"));

        query = "select id, name from queue order by 2 limit 10 offset 60";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_63", result.get(0).get("name"));
        assertEquals("QUEUE_9", result.get(9).get("name"));

        query = "select id, name from queue order by 2 limit 10 offset 70";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());
    }

    @Test()
    public void orderDescBySingleFieldName()
    {
        String query = "select * from queue order by name desc limit 10 offset 0";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_9", result.get(0).get("name"));
        assertEquals("QUEUE_8", result.get(1).get("name"));
        assertEquals("QUEUE_7", result.get(2).get("name"));
        assertEquals("QUEUE_69", result.get(3).get("name"));
        assertEquals("QUEUE_68", result.get(4).get("name"));
        assertEquals("QUEUE_67", result.get(5).get("name"));
        assertEquals("QUEUE_66", result.get(6).get("name"));
        assertEquals("QUEUE_65", result.get(7).get("name"));
        assertEquals("QUEUE_64", result.get(8).get("name"));
        assertEquals("QUEUE_63", result.get(9).get("name"));
    }

    @Test()
    public void orderDescBySingleFieldOrdinal()
    {
        String query = "select id, name from queue order by 2 desc limit 10 offset 0";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_9", result.get(0).get("name"));
        assertEquals("QUEUE_8", result.get(1).get("name"));
        assertEquals("QUEUE_7", result.get(2).get("name"));
        assertEquals("QUEUE_69", result.get(3).get("name"));
        assertEquals("QUEUE_68", result.get(4).get("name"));
        assertEquals("QUEUE_67", result.get(5).get("name"));
        assertEquals("QUEUE_66", result.get(6).get("name"));
        assertEquals("QUEUE_65", result.get(7).get("name"));
        assertEquals("QUEUE_64", result.get(8).get("name"));
        assertEquals("QUEUE_63", result.get(9).get("name"));
    }

    @Test()
    public void orderAscByTwoFieldNames()
    {
        String query = "select * from queue order by overflowPolicy, expiryPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        for (int i = 0; i < 5; i ++)
        {
            assertEquals("FLOW_TO_DISK", result.get(i).get("overflowPolicy"));
            assertEquals("DELETE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 6; i < 10; i ++)
        {
            assertEquals("FLOW_TO_DISK", result.get(i).get("overflowPolicy"));
            assertEquals("ROUTE_TO_ALTERNATE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 11; i < 25; i ++)
        {
            assertEquals("NONE", result.get(i).get("overflowPolicy"));
            assertEquals("DELETE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 25; i < 40; i ++)
        {
            assertEquals("NONE", result.get(i).get("overflowPolicy"));
            assertEquals("ROUTE_TO_ALTERNATE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 40; i < 45; i ++)
        {
            assertEquals("PRODUCER_FLOW_CONTROL", result.get(i).get("overflowPolicy"));
            assertEquals("DELETE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 45; i < 50; i ++)
        {
            assertEquals("PRODUCER_FLOW_CONTROL", result.get(i).get("overflowPolicy"));
            assertEquals("ROUTE_TO_ALTERNATE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 50; i < 55; i ++)
        {
            assertEquals("REJECT", result.get(i).get("overflowPolicy"));
            assertEquals("DELETE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 55; i < 60; i ++)
        {
            assertEquals("REJECT", result.get(i).get("overflowPolicy"));
            assertEquals("ROUTE_TO_ALTERNATE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 60; i < 65; i ++)
        {
            assertEquals("RING", result.get(i).get("overflowPolicy"));
            assertEquals("DELETE", result.get(i).get("expiryPolicy"));
        }
        for (int i = 65; i < 70; i ++)
        {
            assertEquals("RING", result.get(i).get("overflowPolicy"));
            assertEquals("ROUTE_TO_ALTERNATE", result.get(i).get("expiryPolicy"));
        }
    }

    @Test()
    public void orderByFieldMissingInProjections()
    {
        String query = "select id, name from queue order by overflowPolicy limit 10 offset 0";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_20", result.get(0).get("name"));
        assertEquals("QUEUE_21", result.get(1).get("name"));
        assertEquals("QUEUE_22", result.get(2).get("name"));
        assertEquals("QUEUE_23", result.get(3).get("name"));
        assertEquals("QUEUE_24", result.get(4).get("name"));
        assertEquals("QUEUE_25", result.get(5).get("name"));
        assertEquals("QUEUE_26", result.get(6).get("name"));
        assertEquals("QUEUE_27", result.get(7).get("name"));
        assertEquals("QUEUE_28", result.get(8).get("name"));
        assertEquals("QUEUE_29", result.get(9).get("name"));

        query = "select id, name from queue order by queueDepthMessages desc limit 3 offset 0";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.size());
        assertEquals("QUEUE_60", result.get(0).get("name"));
        assertEquals("QUEUE_61", result.get(1).get("name"));
        assertEquals("QUEUE_62", result.get(2).get("name"));
    }

    @Test()
    public void orderByIndexOutOfBoundsOrdinal()
    {
        try
        {
            String query = "select id, name from queue order by 3";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Order by item must be the number of a select list expression", e.getMessage());
        }
    }

    @Test()
    public void orderByZeroOrdinal()
    {
        try
        {
            String query = "select id, name from queue order by 0";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Order by item must be the number of a select list expression", e.getMessage());
        }
    }

    @Test()
    public void orderByNegativeOrdinal()
    {
        try
        {
            String query = "select id, name from queue order by -1";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Order by item must be the number of a select list expression", e.getMessage());
        }
    }

    @Test()
    public void orderByNonExistingField()
    {
        try
        {
            String query = "select id, name from queue order by nonExistingField";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Domain 'queue' does not contain field 'nonExistingField'", e.getMessage());
        }
    }

    @Test()
    public void orderByNonComparableField()
    {
        try
        {
            String query = "select id, name from queue order by statistics";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Sorting by field 'statistics' not supported", e.getMessage());
        }
    }

    @Test()
    public void orderByStatisticsField()
    {
        String query = "select * from queue order by queueDepthMessages desc limit 1";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_60", result.get(0).get("name"));

        query = "select * from queue order by statistics.queueDepthMessages desc limit 1";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_60", result.get(0).get("name"));

        query = "select * from queue order by statistics['queueDepthMessages'] desc limit 1";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_60", result.get(0).get("name"));

        query = "select * from queue q order by q.statistics['queueDepthMessages'] desc limit 1";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_60", result.get(0).get("name"));
    }

    @Test()
    public void orderByNullableField()
    {
        String query = "select * from queue order by description limit 10";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("QUEUE_30", result.get(0).get("name"));
        assertEquals("QUEUE_31", result.get(1).get("name"));
        assertEquals("QUEUE_32", result.get(2).get("name"));
        assertEquals("QUEUE_33", result.get(3).get("name"));
        assertEquals("QUEUE_34", result.get(4).get("name"));
        assertEquals("QUEUE_35", result.get(5).get("name"));
        assertEquals("QUEUE_36", result.get(6).get("name"));
        assertEquals("QUEUE_37", result.get(7).get("name"));
        assertEquals("QUEUE_38", result.get(8).get("name"));
        assertEquals("QUEUE_39", result.get(9).get("name"));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationAscBySingleField()
    {
        String query = "SELECT count(*), overflowPolicy from queue group by overflowPolicy order by overflowPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(5, keys.size());
        assertEquals("FLOW_TO_DISK", keys.get(0));
        assertEquals("NONE", keys.get(1));
        assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
        assertEquals("REJECT", keys.get(3));
        assertEquals("RING", keys.get(4));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationDescBySingleField()
    {
        String query = "SELECT count(*), overflowPolicy from queue group by overflowPolicy order by overflowPolicy desc";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(5, keys.size());
        assertEquals("RING", keys.get(0));
        assertEquals("REJECT", keys.get(1));
        assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
        assertEquals("NONE", keys.get(3));
        assertEquals("FLOW_TO_DISK", keys.get(4));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationAscByCountAndSingleField()
    {
        String query = "SELECT count(*), overflowPolicy from queue group by overflowPolicy order by count(*), overflowPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(5, keys.size());
        assertEquals("FLOW_TO_DISK", keys.get(0));
        assertEquals("PRODUCER_FLOW_CONTROL", keys.get(1));
        assertEquals("REJECT", keys.get(2));
        assertEquals("RING", keys.get(3));
        assertEquals("NONE", keys.get(4));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationAscBySingleFieldAndCount()
    {
        String query = "SELECT count(*), overflowPolicy from queue group by overflowPolicy order by overflowPolicy, count(*)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(5, keys.size());
        assertEquals("FLOW_TO_DISK", keys.get(0));
        assertEquals("NONE", keys.get(1));
        assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
        assertEquals("REJECT", keys.get(3));
        assertEquals("RING", keys.get(4));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationByDefaultSingleField()
    {
        String query = "SELECT count(*), overflowPolicy from queue group by overflowPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(5, keys.size());
        assertEquals("FLOW_TO_DISK", keys.get(0));
        assertEquals("NONE", keys.get(1));
        assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
        assertEquals("REJECT", keys.get(3));
        assertEquals("RING", keys.get(4));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationByDefaultTwoFields()
    {
        String query = "SELECT count(*), overflowPolicy from queue group by overflowPolicy, expiryPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals("FLOW_TO_DISK", keys.get(0));
        assertEquals("NONE", keys.get(1));
        assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
        assertEquals("REJECT", keys.get(3));
        assertEquals("RING", keys.get(4));

        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            keys = new ArrayList<>(((Map<String, Object>)entry.getValue()).keySet());
            assertEquals("DELETE", keys.get(0));
            assertEquals("ROUTE_TO_ALTERNATE", keys.get(1));
        }

        query = "SELECT count(*), overflowPolicy from queue group by expiryPolicy, overflowPolicy";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(2, keys.size());
        keys = new ArrayList<>(map.keySet());
        assertEquals("DELETE", keys.get(0));
        assertEquals("ROUTE_TO_ALTERNATE", keys.get(1));

        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            keys = new ArrayList<>(((Map<String, Object>)entry.getValue()).keySet());
            assertEquals(5, keys.size());
            assertEquals("FLOW_TO_DISK", keys.get(0));
            assertEquals("NONE", keys.get(1));
            assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
            assertEquals("REJECT", keys.get(3));
            assertEquals("RING", keys.get(4));
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationAscByCount()
    {
        String query = "SELECT count(*), overflowPolicy from queue group by expiryPolicy, overflowPolicy order by 1";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(2, keys.size());
        assertEquals("DELETE", keys.get(0));
        assertEquals("ROUTE_TO_ALTERNATE", keys.get(1));

        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            keys = new ArrayList<>(((Map<String, Object>)entry.getValue()).keySet());
            assertEquals(5, keys.size());
            assertEquals("FLOW_TO_DISK", keys.get(0));
            assertEquals("PRODUCER_FLOW_CONTROL", keys.get(1));
            assertEquals("REJECT", keys.get(2));
            assertEquals("RING", keys.get(3));
            assertEquals("NONE", keys.get(4));
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationDescByCount()
    {
        String query = "SELECT count(*), overflowPolicy, expiryPolicy from queue group by expiryPolicy, overflowPolicy order by 1 desc";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(2, keys.size());
        assertEquals("DELETE", keys.get(0));
        assertEquals("ROUTE_TO_ALTERNATE", keys.get(1));

        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            keys = new ArrayList<>(((Map<String, Object>)entry.getValue()).keySet());
            assertEquals(5, keys.size());
            assertEquals("NONE", keys.get(0));
            assertEquals("FLOW_TO_DISK", keys.get(1));
            assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
            assertEquals("REJECT", keys.get(3));
            assertEquals("RING", keys.get(4));
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationAscByCountAndTwoFields()
    {
        String query = "SELECT count(*), expiryPolicy, overflowPolicy from queue group by expiryPolicy, overflowPolicy order by 1, 2, 3";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(2, keys.size());
        assertEquals("DELETE", keys.get(0));
        assertEquals("ROUTE_TO_ALTERNATE", keys.get(1));

        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            keys = new ArrayList<>(((Map<String, Object>)entry.getValue()).keySet());
            assertEquals(5, keys.size());
            assertEquals("FLOW_TO_DISK", keys.get(0));
            assertEquals("PRODUCER_FLOW_CONTROL", keys.get(1));
            assertEquals("REJECT", keys.get(2));
            assertEquals("RING", keys.get(3));
            assertEquals("NONE", keys.get(4));
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationDescByCountAndTwoFields()
    {
        String query = "SELECT count(*), expiryPolicy, overflowPolicy from queue group by expiryPolicy, overflowPolicy order by 1 desc, 2, 3";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(2, keys.size());
        assertEquals("DELETE", keys.get(0));
        assertEquals("ROUTE_TO_ALTERNATE", keys.get(1));

        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            keys = new ArrayList<>(((Map<String, Object>)entry.getValue()).keySet());
            assertEquals(5, keys.size());
            assertEquals("NONE", keys.get(0));
            assertEquals("FLOW_TO_DISK", keys.get(1));
            assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
            assertEquals("REJECT", keys.get(3));
            assertEquals("RING", keys.get(4));
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationAscByCountAndDescTwoFields()
    {
        String query = "SELECT count(*), expiryPolicy, overflowPolicy from queue group by expiryPolicy, overflowPolicy order by 1 asc, 2 desc, 3 desc";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(2, keys.size());
        assertEquals("ROUTE_TO_ALTERNATE", keys.get(0));
        assertEquals("DELETE", keys.get(1));

        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            keys = new ArrayList<>(((Map<String, Object>)entry.getValue()).keySet());
            assertEquals(5, keys.size());
            assertEquals("RING", keys.get(0));
            assertEquals("REJECT", keys.get(1));
            assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
            assertEquals("FLOW_TO_DISK", keys.get(3));
            assertEquals("NONE", keys.get(4));
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void orderAggregationByDefault()
    {
        String query = "SELECT min(queueDepthMessages), max(queueDepthMessages), count(*), avg(queueDepthMessages), sum(queueDepthBytes) from queue group by overflowPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        Map<String, Object> map = result.get(0);
        List<String> keys = new ArrayList<>(map.keySet());
        assertEquals(5, keys.size());
        assertEquals("avg(queueDepthMessages)", keys.get(0));
        assertEquals("count(*)", keys.get(1));
        assertEquals("max(queueDepthMessages)", keys.get(2));
        assertEquals("min(queueDepthMessages)", keys.get(3));
        assertEquals("sum(queueDepthBytes)", keys.get(4));

        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            keys = new ArrayList<>(((Map<String, Object>)entry.getValue()).keySet());
            assertEquals(5, keys.size());
            assertEquals("FLOW_TO_DISK", keys.get(0));
            assertEquals("NONE", keys.get(1));
            assertEquals("PRODUCER_FLOW_CONTROL", keys.get(2));
            assertEquals("REJECT", keys.get(3));
            assertEquals("RING", keys.get(4));
        }
    }

    @Test()
    public void orderCachedQuery()
    {
        final Broker<?> broker = TestBroker.createBroker();
        final QueryEngine queryEngine = new QueryEngine(broker);
        queryEngine.setMaxQueryCacheSize(10);
        queryEngine.setMaxQueryDepth(DefaultQuerySettings.MAX_QUERY_DEPTH);
        final QuerySettings querySettings = new QuerySettings();
        final QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        String query = "select id, name from queue order by overflowPolicy";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(70, result.size());
        assertEquals(2, result.get(0).keySet().size());

        query = "select id, name from queue order by overflowPolicy";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(70, result.size());
        assertEquals(2, result.get(0).keySet().size());
    }
}
