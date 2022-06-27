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

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the broker digests retrieval
 */
public class BrokerDigestQueryTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void selectBrokerDigest()
    {
        String query = "with important_queues as "
            + "(select name from queue where name in ("
            + "'QUEUE_55','QUEUE_56', 'QUEUE_57',"
            + "'QUEUE_60','QUEUE_61')) "
            + "select "
            + "name as label, "
            + "null as objectsState, "
            + "usedHeapMemorySize as heapMemoryUsage, "
            + "maximumHeapMemorySize as maximumHeapMemorySize, "
            + "usedDirectMemorySize as directMemoryUsage, "
            + "maximumDirectMemorySize as maximumDirectMemorySize, "
            + "(statistics.processCpuLoad * 100) as cpuUsage, "
            + "(select count(*) from certificate) as certificatesCount, "
            + "(select count(*) from connection) as connectionCount, "
            + "(select count(*) from exchange) as exchangesCount, "
            + "(select count(*) from binding) as bindingsCount, "
            + "(select count(*) from queue) as queuesCount, "
            + "(select count(*) from user) as usersCount, "
            // good queues have depth < 60%
            + "(select count(*) "
            + "from queue "
            + "where name not in (select name from important_queues) "
            + "or maximumQueueDepthMessages = -1 or maximumQueueDepthBytes = -1 "
            + "or overflowPolicy = 'RING' "
            + "and (queueDepthMessages < maximumQueueDepthMessages * 0.6 "
            + "or queueDepthBytes < maximumQueueDepthBytes * 0.6)) as goodQueuesCount, "
            // bad queues have depth > 60% and < 90%
            + "(select count(*) "
            + "from queue "
            + "where name in (select name from important_queues) "
            + "and maximumQueueDepthMessages != -1 and maximumQueueDepthBytes != -1 "
            + "and overflowPolicy != 'RING' "
            + "and (queueDepthMessages between (maximumQueueDepthMessages * 0.6, maximumQueueDepthMessages * 0.9) "
            + "or queueDepthBytes between (maximumQueueDepthBytes * 0.6, maximumQueueDepthBytes * 0.9))) as badQueuesCount, "
            // critical queues have depth > 90%
            + "(select count(*) "
            + "from queue "
            + "where name in (select name from important_queues) "
            + "and maximumQueueDepthMessages != -1 and maximumQueueDepthBytes != -1 "
            + "and overflowPolicy != 'RING' "
            + "and (queueDepthMessages > maximumQueueDepthMessages * 0.9 "
            + "or queueDepthBytes > maximumQueueDepthBytes * 0.9)) as criticalQueuesCount "
            + "from broker";

        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("mock", result.get(0).get("label"));
        assertEquals(6_000_000_000L, result.get(0).get("heapMemoryUsage"));
        assertEquals(10_000_000_000L, result.get(0).get("maximumHeapMemorySize"));
        assertEquals(500_000_000L, result.get(0).get("directMemoryUsage"));
        assertEquals(1_500_000_000L, result.get(0).get("maximumDirectMemorySize"));
        assertEquals(5.2, result.get(0).get("cpuUsage"));
        assertEquals(10, result.get(0).get("certificatesCount"));
        assertEquals(30, result.get(0).get("connectionCount"));
        assertEquals(10, result.get(0).get("exchangesCount"));
        assertEquals(10, result.get(0).get("bindingsCount"));
        assertEquals(70, result.get(0).get("queuesCount"));
        assertEquals(0, result.get(0).get("usersCount"));
        assertEquals(65, result.get(0).get("goodQueuesCount"));
        assertEquals(3, result.get(0).get("badQueuesCount"));
        assertEquals(2, result.get(0).get("criticalQueuesCount"));
    }
}
