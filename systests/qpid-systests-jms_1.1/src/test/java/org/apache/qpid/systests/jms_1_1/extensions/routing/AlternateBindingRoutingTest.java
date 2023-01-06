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
package org.apache.qpid.systests.jms_1_1.extensions.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.Session;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class AlternateBindingRoutingTest extends JmsTestBase
{
    @Test
    public void testFanoutExchangeAsAlternateBinding() throws Exception
    {
        String queueName = getTestName();
        String deadLetterQueueName = queueName + "_DeadLetter";
        String deadLetterExchangeName = "deadLetterExchange";

        createEntityUsingAmqpManagement(deadLetterQueueName,
                                        "org.apache.qpid.StandardQueue",
                                        Collections.emptyMap());
        createEntityUsingAmqpManagement(deadLetterExchangeName,
                                        "org.apache.qpid.FanoutExchange",
                                        Collections.emptyMap());

        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", deadLetterQueueName);
        arguments.put("bindingKey", queueName);
        performOperationUsingAmqpManagement(deadLetterExchangeName,
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            arguments);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Queue.NAME, queueName);
        attributes.put(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING,
                       new ObjectMapper().writeValueAsString(Collections.singletonMap(AlternateBinding.DESTINATION,
                                                                                      deadLetterExchangeName)));
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.StandardQueue", attributes);

        Queue testQueue = createQueue(queueName);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            Utils.sendMessages(session, testQueue, 1);

            assertEquals(1, getQueueDepth(queueName), "Unexpected number of messages on queue");
            assertEquals(0, getQueueDepth(deadLetterQueueName), "Unexpected number of messages on DLQ");

            performOperationUsingAmqpManagement(queueName,
                                                "DELETE",
                                                "org.apache.qpid.Queue",
                                                Collections.emptyMap());

            assertEquals(1, getQueueDepth(deadLetterQueueName), "Unexpected number of messages on DLQ after deletion");
        }
        finally
        {
            connection.close();
        }
    }

    private int getQueueDepth(final String queueName) throws Exception
    {
        Map<String, Object> arguments =
                Collections.singletonMap("statistics", Collections.singletonList("queueDepthMessages"));
        Object statistics = performOperationUsingAmqpManagement(queueName,
                                                                "getStatistics",
                                                                "org.apache.qpid.Queue",
                                                                arguments);
        assertNotNull(statistics, "Statistics is null");
        assertTrue(statistics instanceof Map, "Statistics is not map");
        @SuppressWarnings("unchecked")
        Map<String, Object> statisticsMap = (Map<String, Object>) statistics;
        assertTrue(statisticsMap.get("queueDepthMessages") instanceof Number, "queueDepthMessages is not present");
        return ((Number) statisticsMap.get("queueDepthMessages")).intValue();
    }
}
