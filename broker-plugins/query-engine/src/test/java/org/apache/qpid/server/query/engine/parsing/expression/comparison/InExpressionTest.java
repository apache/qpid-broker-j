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
package org.apache.qpid.server.query.engine.parsing.expression.comparison;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link InExpression} functionality
 */
public class InExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void inIntegers()
    {
        String query = "select 1 in (1, 2, 3) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1 in (2, 3, 4) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 1 in (1) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void inLongs()
    {
        String query = "select 1L in (1L, 2L, 3L) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1L in (2L, 3L, 4L) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 1L in (1L) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void inDoubles()
    {
        String query = "select 1.0 in (1.0, 2.0, 3.0) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1.0 in (2.0, 3.0, 4.0) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 1.0 in (1.0) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void inBigDecimals()
    {
        String query = "select " + BigDecimal.ONE + " in (" + BigDecimal.ONE + ", " + BigDecimal.valueOf(2L) + ", " + BigDecimal.valueOf(3L) + ") as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select " + BigDecimal.ONE + " in (" + BigDecimal.valueOf(2L) + ", " + BigDecimal.valueOf(3L) + ", " + BigDecimal.valueOf(4L) + ") as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select " + BigDecimal.ONE + " in (" + BigDecimal.valueOf(1L) + ") as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void inStrings()
    {
        String query = "select 'test' in ('test123', 'test234', 'test') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' not in ('test123', 'test234', 'test') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test' in ('test123', 'test234', 'test345') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test' not in ('test123', 'test234', 'test345') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' in ('test') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' not in ('test') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));
    }

    @Test()
    public void inMixedTypes()
    {
        String query = "select 1 in ('1', 'test', 1.0) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1 not in ('1', 'test', 1.0) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test' in (100L, 5/6, 'test') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' not in (100L, 5/6, 'test') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 1/3 in ('test', '1/3', 0) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 1/3 not in ('test', '1/3', 0) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void inSelect()
    {
        String query = "select 'FLOW_TO_DISK' in (select distinct overflowPolicy from queue) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'RING' in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'REJECT' in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'PRODUCER_FLOW_CONTROL' in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'NONE' in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'UNKNOWN' in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));
    }

    @Test()
    public void notInSelect()
    {
        String query = "select 'FLOW_TO_DISK' not in (select distinct overflowPolicy from queue) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'RING' not in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'REJECT' not  in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'PRODUCER_FLOW_CONTROL' not in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'NONE' not in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'UNKNOWN' not in (select distinct overflowPolicy from queue) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void inList()
    {
        String query = "select lower(name) as name from queue where lower(name) in ('queue_10', 'queue_20', 'queue_30')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.size());
        assertEquals("queue_10", result.get(0).get("name"));
        assertEquals("queue_20", result.get(1).get("name"));
        assertEquals("queue_30", result.get(2).get("name"));

        query = "select distinct overflowPolicy from queue where overflowPolicy in ('RING', 'REJECT', 'NONE')";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.size());
        assertEquals("NONE", result.get(0).get("overflowPolicy"));
        assertEquals("REJECT", result.get(1).get("overflowPolicy"));
        assertEquals("RING", result.get(2).get("overflowPolicy"));
    }

    @Test()
    public void notInList()
    {
        String query = "select lower(name) as name from queue where lower(name) not in ('queue_10', 'queue_20', 'queue_30')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(67, result.size());

        query = "select distinct overflowPolicy from queue where overflowPolicy not in ('RING', 'REJECT', 'NONE')";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(2, result.size());
        assertEquals("FLOW_TO_DISK", result.get(0).get("overflowPolicy"));
        assertEquals("PRODUCER_FLOW_CONTROL", result.get(1).get("overflowPolicy"));
    }

}
