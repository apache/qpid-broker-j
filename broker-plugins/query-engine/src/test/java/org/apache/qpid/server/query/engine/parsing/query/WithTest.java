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
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the WITH clause functionality
 */
public class WithTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void selectFromWith()
    {
        String query = "with digest (id, name) as (select id, name from queue) select * from digest";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals(2, result.get(0).keySet().size());
        assertEquals("id", result.get(0).keySet().iterator().next());
        assertEquals("name", result.get(0).keySet().stream().skip(1).findFirst().orElse(null));
    }

    @Test()
    public void withClauseWithoutColumnNames()
    {
        String query = "with digest as (select id, name from queue) select * from digest";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals(2, result.get(0).keySet().size());
        assertEquals("id", result.get(0).keySet().iterator().next());
        assertEquals("name", result.get(0).keySet().stream().skip(1).findFirst().orElse(null));

        query = "with digest as (select id, name, description from queue) select * from digest";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals(3, result.get(0).keySet().size());
        assertEquals("id", result.get(0).keySet().iterator().next());
        assertEquals("name", result.get(0).keySet().stream().skip(1).findFirst().orElse(null));
        assertEquals("description", result.get(0).keySet().stream().skip(2).findFirst().orElse(null));
    }

    @Test()
    public void numberOfColumnNamesDoesNotMatch()
    {
        String query = "with digest (id, name, depth) as (select id, name from queue) select * from digest";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Number of WITH clause column names does not match number of elements in select list", e.getMessage());
        }

        query = "with digest (id, name) as (select id, name, description from queue) select * from digest";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Number of WITH clause column names does not match number of elements in select list", e.getMessage());
        }
    }

    @Test()
    public void selectListOfItemsFromWithSubquery()
    {
        String query = "with important_queues (name) as ("
            + "select 'QUEUE_1' "
            + "union "
            + "select 'QUEUE_2' "
            + "union "
            + "select 'QUEUE_3') "
            + "select * from important_queues";
        List<Map<String, Object>>result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.size());
        assertEquals("QUEUE_1", result.get(0).get("name"));
        assertEquals("QUEUE_2", result.get(1).get("name"));
        assertEquals("QUEUE_3", result.get(2).get("name"));
    }
}
