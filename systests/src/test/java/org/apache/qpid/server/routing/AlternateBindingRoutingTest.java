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
package org.apache.qpid.server.routing;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.Session;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class AlternateBindingRoutingTest extends QpidBrokerTestCase
{
    public void testFanoutExchangeAsAlternateBinding() throws Exception
    {
        Connection connection = getConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        String queueName = getTestQueueName();
        String deadLetterQueueName = queueName + "_DeadLetter";
        String deadLetterExchangeName = "deadLetterExchange";

        Queue deadLetterQueue = createTestQueue(session, deadLetterQueueName);

        createEntityUsingAmqpManagement(deadLetterExchangeName,
                                        session,
                                        "org.apache.qpid.FanoutExchange");

        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", deadLetterQueueName);
        arguments.put("bindingKey", queueName);
        performOperationUsingAmqpManagement(deadLetterExchangeName,
                                            "bind",
                                            session,
                                            "org.apache.qpid.Exchange",
                                            arguments);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Queue.NAME, queueName);
        attributes.put(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING,
                       new ObjectMapper().writeValueAsString(Collections.singletonMap(AlternateBinding.DESTINATION,
                                                                                      deadLetterExchangeName)));
        createEntityUsingAmqpManagement(queueName,
                                        session,
                                        "org.apache.qpid.StandardQueue",
                                        attributes);
        Queue testQueue = getQueueFromName(session, queueName);

        sendMessage(session, testQueue, 1);
        assertEquals("Unexpected number of messages on queueName", 1, getQueueDepth(connection, testQueue));

        assertEquals("Unexpected number of messages on DLQ queueName", 0, getQueueDepth(connection, deadLetterQueue));

        performOperationUsingAmqpManagement(queueName,
                                            "DELETE",
                                            session,
                                            "org.apache.qpid.Queue",
                                            Collections.emptyMap());

        assertEquals("Unexpected number of messages on DLQ queueName", 1, getQueueDepth(connection, deadLetterQueue));
    }

}
