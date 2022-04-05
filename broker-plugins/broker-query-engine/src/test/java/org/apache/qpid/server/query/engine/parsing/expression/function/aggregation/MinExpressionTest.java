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

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

public class MinExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void minimizingLongs()
    {
        String query = "select min(queueDepthMessages) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.get(0).get("min(queueDepthMessages)"));
    }

    @Test()
    public void minimizingStrings()
    {
        String query = "select min(lower(name)) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("queue_0", result.get(0).get("min(lower(name))"));
    }

    @Test()
    public void minimizingDatetime()
    {
        EvaluationContextHolder.getEvaluationContext().put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
        Broker<?> broker = TestBroker.createBroker();
        Date maxDate = broker.getVirtualHostNodes().stream().map(VirtualHostNode::getVirtualHost).filter(Objects::nonNull)
            .flatMap(vh -> vh.getChildren(Queue.class).stream()).map(ConfiguredObject::getCreatedTime)
            .min(Comparator.naturalOrder()).orElse(null);

        String query = "select min(createdTime) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(maxDate, result.get(0).get("min(createdTime)"));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void minGroupBy()
    {
        String query = "select min(queueDepthMessages), overflowPolicy from queue group by overflowPolicy";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();

        Map<String, Object> map = (Map<String, Object>) result.get(0).get("min(queueDepthMessages)");
        assertEquals(0, map.get("FLOW_TO_DISK"));
        assertEquals(0, map.get("RING"));
        assertEquals(0, map.get("REJECT"));
        assertEquals(0, map.get("PRODUCER_FLOW_CONTROL"));
        assertEquals(0, map.get("NONE"));

        query = "select min(queueDepthMessages), overflowPolicy, expiryPolicy from queue group by overflowPolicy, expiryPolicy";
        result = _queryEvaluator.execute(query).getResults();

        map = (Map<String, Object>) result.get(0).get("min(queueDepthMessages)");
        assertEquals(0, ((Map<String, Object>)map.get("FLOW_TO_DISK")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>)map.get("FLOW_TO_DISK")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(0, ((Map<String, Object>)map.get("RING")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>)map.get("RING")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(0, ((Map<String, Object>)map.get("REJECT")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>)map.get("REJECT")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(0, ((Map<String, Object>)map.get("PRODUCER_FLOW_CONTROL")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>)map.get("PRODUCER_FLOW_CONTROL")).get("ROUTE_TO_ALTERNATE"));
        assertEquals(0, ((Map<String, Object>)map.get("NONE")).get("DELETE"));
        assertEquals(0, ((Map<String, Object>)map.get("NONE")).get("ROUTE_TO_ALTERNATE"));
    }

    @Test()
    public void noArguments()
    {
        String query = "select min() from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'MIN' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void twoArguments()
    {
        String query = "select min(queueDepthMessages, queueDepthBytes) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'MIN' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void invalidArgumentType()
    {
        String query = "select min(statistics) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameters of function 'MIN' invalid (invalid types: [HashMap])", e.getMessage());
        }
    }

    @Test()
    public void subqueryInProjections()
    {
        String query = "select name, (select min(queueDepthMessages) from queue) as cnt from broker";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.get(0).get("cnt"));

        query = "select name, (select lower(min(name)) from queue) as cnt from broker";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("queue_0", result.get(0).get("cnt"));
    }

    @Test()
    public void subqueryInWhere()
    {
        String query = "select min(name) from queue where queueDepthMessages = (select min(queueDepthMessages) from queue)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_0", result.get(0).get("min(name)"));
    }

    @Test()
    public void minArithmeticExpressions()
    {
        String query = "select round(min(queueDepthMessages) + 10) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.get(0).get("round(min(queueDepthMessages)+10)"));

        query = "select round(min(queueDepthMessages) - 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(-10, result.get(0).get("round(min(queueDepthMessages)-10)"));

        query = "select round(min(queueDepthMessages) * 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.get(0).get("round(min(queueDepthMessages)*10)"));

        query = "select round(min(queueDepthMessages) / 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.get(0).get("round(min(queueDepthMessages)/10)"));

        query = "select round(min(queueDepthMessages) % 10) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.get(0).get("round(min(queueDepthMessages)%10)"));
    }
}
