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
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the public class grouping functionality
 */
public class GroupByTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void groupByNegativeOrdinal()
    {
        String query = "select count(*), expiryPolicy from queue group by -1";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Group by item must be the number of a select list expression", e.getMessage());
        }
    }

    @Test()
    public void groupByZeroOrdinal()
    {
        String query = "select count(*), expiryPolicy from queue group by 0";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Group by item must be the number of a select list expression", e.getMessage());
        }
    }

    @Test()
    public void groupByOutOfBoundsOrdinal()
    {
        String query = "select count(*), expiryPolicy from queue group by 3";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Group by item must be the number of a select list expression", e.getMessage());
        }
    }

    @Test()
    public void groupByNoneExistingField()
    {
        String query = "select count(*), nonExistingField from queue group by nonExistingField";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Domain 'queue' does not contain field 'nonExistingField'", e.getMessage());
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void groupByOrdinals()
    {
        String query = "select count(*), expiryPolicy, overflowPolicy from queue group by 2, 3";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("count(*)", result.get(0).keySet().iterator().next());

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("count(*)");
        assertEquals(5, ((Map<String, Object>)map.get("DELETE")).get("REJECT"));
        assertEquals(5, ((Map<String, Object>)map.get("DELETE")).get("PRODUCER_FLOW_CONTROL"));
        assertEquals(5, ((Map<String, Object>)map.get("DELETE")).get("RING"));
        assertEquals(5, ((Map<String, Object>)map.get("DELETE")).get("FLOW_TO_DISK"));
        assertEquals(15, ((Map<String, Object>)map.get("DELETE")).get("NONE"));
        assertEquals(5, ((Map<String, Object>)map.get("ROUTE_TO_ALTERNATE")).get("REJECT"));
        assertEquals(5, ((Map<String, Object>)map.get("ROUTE_TO_ALTERNATE")).get("PRODUCER_FLOW_CONTROL"));
        assertEquals(5, ((Map<String, Object>)map.get("ROUTE_TO_ALTERNATE")).get("RING"));
        assertEquals(5, ((Map<String, Object>)map.get("ROUTE_TO_ALTERNATE")).get("FLOW_TO_DISK"));
        assertEquals(15, ((Map<String, Object>)map.get("ROUTE_TO_ALTERNATE")).get("NONE"));

        query = "select count(*), expiryPolicy, overflowPolicy from queue group by 3, 2";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("count(*)", result.get(0).keySet().iterator().next());

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
    }
}
