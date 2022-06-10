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
package org.apache.qpid.server.query.engine.parsing.expression.set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

public class UnionExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void unionAllFields()
    {
        String query = "select * from queue where name = 'QUEUE_0' "
            + "union "
            + "select * from queue where name = 'QUEUE_10'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(2, result.size());
        assertEquals("QUEUE_0", result.get(0).get("name"));
        assertEquals("QUEUE_10", result.get(1).get("name"));
    }

    @Test()
    public void unionAll()
    {
        String query = "select id, name from queue where name in ('QUEUE_0', 'QUEUE_1', 'QUEUE_2') "
                       + "union all "
                       + "select id, name from queue where name in ('QUEUE_2', 'QUEUE_3', 'QUEUE_4')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(6, result.size());
        assertEquals("QUEUE_0", result.get(0).get("name"));
        assertEquals("QUEUE_1", result.get(1).get("name"));
        assertEquals("QUEUE_2", result.get(2).get("name"));
        assertEquals("QUEUE_2", result.get(3).get("name"));
        assertEquals("QUEUE_3", result.get(4).get("name"));
        assertEquals("QUEUE_4", result.get(5).get("name"));
    }

    @Test()
    public void unionDistinct()
    {
        String query = "select id, name from queue where name in ('QUEUE_0', 'QUEUE_1', 'QUEUE_2') "
                       + "union distinct "
                       + "select id, name from queue where name in ('QUEUE_2', 'QUEUE_3', 'QUEUE_4')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(5, result.size());
        assertEquals("QUEUE_0", result.get(0).get("name"));
        assertEquals("QUEUE_1", result.get(1).get("name"));
        assertEquals("QUEUE_2", result.get(2).get("name"));
        assertEquals("QUEUE_3", result.get(3).get("name"));
        assertEquals("QUEUE_4", result.get(4).get("name"));
    }

    @Test()
    public void unionDistinctOrderBy()
    {
        String query = "select id, name from queue where name in ('QUEUE_0', 'QUEUE_1', 'QUEUE_2') "
                       + "union distinct "
                       + "select id, name from queue where name in ('QUEUE_2', 'QUEUE_3', 'QUEUE_4') "
                       + "order by name desc";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(5, result.size());
        assertEquals("QUEUE_4", result.get(0).get("name"));
        assertEquals("QUEUE_3", result.get(1).get("name"));
        assertEquals("QUEUE_2", result.get(2).get("name"));
        assertEquals("QUEUE_1", result.get(3).get("name"));
        assertEquals("QUEUE_0", result.get(4).get("name"));
    }

    @Test()
    public void unionEmptySet()
    {
        String query = "select id, name from queue where name = 'QUEUE_999' "
                       + "union "
                       + "select id, name from queue where name = 'QUEUE_10'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_10", result.get(0).get("name"));

        query = "select id, name from queue where name = 'QUEUE_10' "
                + "union "
                + "select id, name from queue where name = 'QUEUE_999'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_10", result.get(0).get("name"));

        query = "select id, name from queue where name = 'QUEUE_777' "
                + "union "
                + "select id, name from queue where name = 'QUEUE_999'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());
    }

    @Test()
    public void unionAggregations()
    {
        String query = "select count(*) as result from queue "
                       + "union "
                       + "select sum(queueDepthMessages) from queue "
                       + "union "
                       + "select max(queueDepthMessages) from queue "
                       + "union "
                       + "select min(queueDepthMessages) from queue "
                       + "union "
                       + "select round(avg(queueDepthMessages)) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(5, result.size());
        assertEquals(70, result.get(0).get("result"));
        assertEquals(1600, result.get(1).get("result"));
        assertEquals(95, result.get(2).get("result"));
        assertEquals(0, result.get(3).get("result"));
        assertEquals(22.86, result.get(4).get("result"));
    }

    @Test()
    public void unionDifferentSetLength()
    {
        String query = "select id, name from queue where name = 'QUEUE_0' "
            + "union "
            + "select name from queue where name = 'QUEUE_10'";
        try
        {
            _queryEvaluator.execute(query).getResults();
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Products of 'union' operation have different length", e.getMessage());
        }
    }

    @Test()
    public void optionalBrackets()
    {
        String query = "(select * from queue where name = 'QUEUE_0') "
                       + "union "
                       + "(select * from queue where name = 'QUEUE_10')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(2, result.size());
        assertEquals("QUEUE_0", result.get(0).get("name"));
        assertEquals("QUEUE_10", result.get(1).get("name"));
    }
}
