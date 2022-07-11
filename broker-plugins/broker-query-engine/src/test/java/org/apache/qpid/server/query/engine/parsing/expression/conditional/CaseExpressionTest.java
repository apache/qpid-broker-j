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
package org.apache.qpid.server.query.engine.parsing.expression.conditional;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link CaseExpression} functionality
 */
public class CaseExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void constants()
    {
        String query = "select case when 1 > 2 then 1 else 2 end as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(2, result.get(0).get("result"));

        query = "select case when true then 1 when false then 2 else 3 end as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.get(0).get("result"));

        query = "select case when false then 1 when true then 2 else 3 end as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(2, result.get(0).get("result"));

        query = "select case when false then 1 when false then 2 else 3 end as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.get(0).get("result"));

        query = "select (case when true then 1 when false then 2 else 3 end) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.get(0).get("result"));

        query = "select (case when false then 1 when true then 2 else 3 end) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(2, result.get(0).get("result"));

        query = "select (case when false then 1 when false then 2 else 3 end) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(3, result.get(0).get("result"));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void queueMessageDepth()
    {
        String query = "select "
            + "case "
            + "when (maximumQueueDepthMessages = -1 and maximumQueueDepthBytes = -1) or queueDepthMessages < maximumQueueDepthMessages * 0.6 then 'good' "
            + "when queueDepthMessages < maximumQueueDepthMessages * 0.9 then 'bad' "
            + "else 'critical' "
            + "end as queue_state, "
            + "count(*) "
            + "from queue group by queue_state";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(50, ((Map<String, Object>)result.get(0).get("count(*)")).get("good"));
        assertEquals(10, ((Map<String, Object>)result.get(0).get("count(*)")).get("bad"));
        assertEquals(10, ((Map<String, Object>)result.get(0).get("count(*)")).get("critical"));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void queueDescription()
    {
        String query = "select case when description is null then 'empty' else description end as description from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        for (int i = 0; i < 40; i ++)
        {
            assertEquals("empty", result.get(i).get("description"));
        }
        for (int i = 40; i < 50; i ++)
        {
            assertEquals("test description 1", result.get(i).get("description"));
        }
        for (int i = 50; i < 60; i ++)
        {
            assertEquals("test description 2", result.get(i).get("description"));
        }
        for (int i = 60; i < 70; i ++)
        {
            assertEquals("test description 3", result.get(i).get("description"));
        }

        query = "select count(distinct case when description is null then 'empty' else description end) as description from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("description"));

        query = "select "
                + "count(distinct case when description is null then 'empty' else description end) as cnt, "
                + "coalesce(description, 'empty') as description "
                + "from queue "
                + "group by description";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(10, ((Map<String,Object>)result.get(0).get("cnt")).get("test description 1"));
        assertEquals(10, ((Map<String,Object>)result.get(0).get("cnt")).get("test description 2"));
        assertEquals(10, ((Map<String,Object>)result.get(0).get("cnt")).get("test description 3"));
        assertEquals(40, ((Map<String,Object>)result.get(0).get("cnt")).get("empty"));

        query = "select "
                + "count(case when description is null then 'empty' else description end) cnt, "
                + "coalesce(description, 'empty') description "
                + "from queue "
                + "group by 2";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(10, ((Map<String,Object>)result.get(0).get("cnt")).get("test description 1"));
        assertEquals(10, ((Map<String,Object>)result.get(0).get("cnt")).get("test description 2"));
        assertEquals(10, ((Map<String,Object>)result.get(0).get("cnt")).get("test description 3"));
        assertEquals(40, ((Map<String,Object>)result.get(0).get("cnt")).get("empty"));
    }
}
