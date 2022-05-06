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
package org.apache.qpid.server.query.engine.broker;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.query.engine.QueryEngine;
import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;

public class QueueQueryTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void allQueues()
    {
        String query = "select * from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
    }

    @Test()
    public void queuesByName()
    {
        String query = "select * from queue where name = 'QUEUE_1'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        query = "select * from queue where name = 'QUEUE_1' or name = 'QUEUE_10'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(2, result.size());

        query = "select * from queue where lower(name) = 'queue_1'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());

        query = "select * from queue where lower(name) like 'queue_1%'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(11, result.size());

        query = "select * from queue where lower(name) like 'queue_1?'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
    }

    @Test()
    public void queuesByOverflowPolicy()
    {
        String query = "select * from queue where overflowPolicy = 'RING'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        query = "select * from queue where overflowPolicy = 'REJECT'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        query = "select * from queue where overflowPolicy = 'FLOW_TO_DISK'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        query = "select * from queue where overflowPolicy = 'PRODUCER_FLOW_CONTROL'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        query = "select * from queue where overflowPolicy = 'NONE'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(30, result.size());
    }

    @Test()
    public void queuesByMessageDepth()
    {
        String query = "select * from queue "
        + "where maximumQueueDepthMessages != -1 "
        + "and maximumQueueDepthBytes != -1 "
        + "and ((queueDepthMessages > maximumQueueDepthMessages * 0.6 and queueDepthMessages < maximumQueueDepthMessages * 0.9)"
        + "or (queueDepthBytes > maximumQueueDepthBytes * 0.6 and queueDepthBytes < maximumQueueDepthBytes * 0.9))";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        for (int i = 50; i < 60; i++)
        {
            assertEquals("QUEUE_" + i, result.get(i - 50).get("name"));
        }

        query = "select * from queue "
                + "where maximumQueueDepthMessages != -1 "
                + "and maximumQueueDepthBytes != -1 "
                + "and (queueDepthMessages > maximumQueueDepthMessages * 0.9"
                + "or queueDepthBytes > maximumQueueDepthBytes * 0.9)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        for (int i = 60; i < 70; i++)
        {
            assertEquals("QUEUE_" + i, result.get(i - 60).get("name"));
        }

        query = "select * from queue "
                + "where maximumQueueDepthMessages != -1 " 
                + "and maximumQueueDepthBytes != -1 ";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(60, result.size());
        for (int i = 10; i < 70; i++)
        {
            assertEquals("QUEUE_" + i, result.get(i - 10).get("name"));
        }
    }

    @Test()
    public void criticalQueues()
    {
        String query = "select "
            + "name, "
            + "queueDepthMessages, "
            + "round(queueDepthMessages / maximumQueueDepthMessages * 100, 2) as queueDepthMessagesPercentage, "
            + "queueDepthBytes, "
            + "round(queueDepthBytes / maximumQueueDepthBytes * 100, 2) as queueDepthBytesPercentage "
            + "from queue "
            + "where maximumQueueDepthMessages != -1 and maximumQueueDepthBytes != -1 "
            + "and overflowPolicy != 'RING' "
            + "and (queueDepthMessages > maximumQueueDepthMessages * 0.9 "
            + "or queueDepthBytes > maximumQueueDepthBytes * 0.9)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
    }

    @Test()
    public void badQueues()
    {
        String query = "select "
            + "name, "
            + "queueDepthMessages, "
            + "round(queueDepthMessages / maximumQueueDepthMessages * 100, 2) as queueDepthMessagesPercentage, "
            + "queueDepthBytes, "
            + "round(queueDepthBytes / maximumQueueDepthBytes * 100, 2) as queueDepthBytesPercentage "
            + "from queue "
            + "where maximumQueueDepthMessages != -1 and maximumQueueDepthBytes != -1 "
            + "and overflowPolicy != 'RING' "
            + "and (queueDepthMessages between (maximumQueueDepthMessages * 0.6, maximumQueueDepthMessages * 0.9) "
            + "or queueDepthBytes between maximumQueueDepthBytes * 0.6 and maximumQueueDepthBytes * 0.9)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
    }

    @Test()
    public void countingInSubquery()
    {
        String query = "select "
            + "name, "
            + "(select count(*) from queue "
            + "where maximumQueueDepthMessages = -1 and maximumQueueDepthBytes = -1 "
            + "or ((maximumQueueDepthMessages != -1 and (queueDepthMessages < maximumQueueDepthMessages * 0.6)) "
            + "and (maximumQueueDepthBytes != -1 and (queueDepthBytes < maximumQueueDepthBytes * 0.6)))) as goodQueuesCount, "
            + "(select count(*) from queue "
            + "where maximumQueueDepthMessages != -1 "
            + "and maximumQueueDepthBytes != -1 "
            + "and (queueDepthMessages between (maximumQueueDepthMessages * 0.6 and maximumQueueDepthMessages * 0.9)"
            + "or (queueDepthBytes between (maximumQueueDepthBytes * 0.6 and maximumQueueDepthBytes * 0.9)))) as badQueuesCount, "
            + "(select count(*) from queue "
            + "where maximumQueueDepthMessages != -1 "
            + "and maximumQueueDepthBytes != -1 "
            + "and (queueDepthMessages > maximumQueueDepthMessages * 0.9 "
            + "or queueDepthBytes > maximumQueueDepthBytes * 0.9)) as criticalQueuesCount "
            + "from broker";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("mock", result.get(0).get("name"));
        assertEquals(50, result.get(0).get("goodQueuesCount"));
        assertEquals(10, result.get(0).get("badQueuesCount"));
        assertEquals(10, result.get(0).get("criticalQueuesCount"));
    }

    @Test()
    public void withStatement()
    {
        String query = "with ringQueues (queueName) as (select name from queue where overflowPolicy = 'RING') "
            + "select * from ringQueues";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
    }

    @Test()
    public void queueWithMaxMessages()
    {
        final Broker<?> broker = TestBroker.createBroker();
        final QueryEngine queryEngine = new QueryEngine(broker);
        queryEngine.setMaxQueryCacheSize(10);
        queryEngine.setMaxQueryDepth(4096);
        final QuerySettings querySettings = new QuerySettings();
        final QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        String query = "select * from queue where queueDepthMessages = (select max(queueDepthMessages) from queue)";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(10, result.size());

        query = "select * from queue where queueDepthMessages = (select max(queueDepthMessages) from queue)";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(10, result.size());

    }
}