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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the {@link IntersectExpression} functionality
 */
public class IntersectExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void notIntersectingSets()
    {
        String query = "select * from queue where name in ('QUEUE_1', 'QUEUE_2', 'QUEUE_3') "
                       + "intersect "
                       + "select * from queue where name in ('QUEUE_11', 'QUEUE_12', 'QUEUE_13')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());
    }

    @Test()
    public void fullyIntersectingSets()
    {
        String query = "select * from queue where name in ('QUEUE_1', 'QUEUE_2', 'QUEUE_3') "
                       + "intersect "
                       + "select * from queue where name in ('QUEUE_1', 'QUEUE_2', 'QUEUE_3')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.size());
        assertEquals("QUEUE_1", result.get(0).get("name"));
        assertEquals("QUEUE_2", result.get(1).get("name"));
        assertEquals("QUEUE_3", result.get(2).get("name"));
    }

    @Test()
    public void partiallyIntersectingSets()
    {
        String query = "select * from queue where name in ('QUEUE_1', 'QUEUE_2', 'QUEUE_3') "
                       + "intersect "
                       + "select * from queue where name in ('QUEUE_11', 'QUEUE_2', 'QUEUE_13')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_2", result.get(0).get("name"));
    }

    @Test()
    public void intersectWithAnEmptySet()
    {
        String query = "select * from queue where name in ('QUEUE_1', 'QUEUE_2', 'QUEUE_3') "
                       + "intersect "
                       + "select * from queue where name in ('QUEUE_111', 'QUEUE_222', 'QUEUE_333')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());
    }

    @Test()
    public void intersectDifferentSetLength()
    {
        String query = "select id, name from queue where name = 'QUEUE_0' "
                       + "intersect "
                       + "select name from queue where name = 'QUEUE_10'";
        try
        {
            _queryEvaluator.execute(query).getResults();
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Products of 'intersect' operation have different length", e.getMessage());
        }
    }

    @Test()
    public void optionalBrackets()
    {
        String query = "(select * from queue where name in ('QUEUE_1', 'QUEUE_2', 'QUEUE_3')) "
                       + "intersect "
                       + "(select * from queue where name in ('QUEUE_11', 'QUEUE_2', 'QUEUE_13'))";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_2", result.get(0).get("name"));
    }

}
