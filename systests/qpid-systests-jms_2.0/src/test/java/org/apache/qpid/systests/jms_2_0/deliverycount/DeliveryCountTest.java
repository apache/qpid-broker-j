package org.apache.qpid.systests.jms_2_0.deliverycount;/*
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

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class DeliveryCountTest extends QpidBrokerTestCase
{
    private static final int MAX_DELIVERY_ATTEMPTS = 3;
    private static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";
    private Queue _queue;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        try (Connection connection = getConnectionWithPrefetch(0))
        {
            String testQueueName = getTestQueueName();
            connection.start();
            Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
            final Map<String, Object> attributes = new HashMap<>();
            attributes.put(org.apache.qpid.server.model.Queue.NAME, testQueueName);
            attributes.put(org.apache.qpid.server.model.Queue.MAXIMUM_DELIVERY_ATTEMPTS, MAX_DELIVERY_ATTEMPTS);
            createEntityUsingAmqpManagement(testQueueName,
                                            session,
                                            "org.apache.qpid.StandardQueue",
                                            attributes);
           _queue = session.createQueue(testQueueName);
            sendMessage(session, _queue, 1);
        }
    }


    public void testDeliveryCountChangedOnRollback() throws Exception
    {
        try (Connection connection = getConnectionWithPrefetch(0))
        {
            Session session = connection.createSession(JMSContext.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(_queue);
            connection.start();
            for (int i = 0; i < MAX_DELIVERY_ATTEMPTS; i++)
            {
                Message message = consumer.receive(getReceiveTimeout());
                session.rollback();
                assertDeliveryCountHeaders(message, i);
            }
            Message message = consumer.receive(getReceiveTimeout());
            assertNull("Message should be discarded", message);
        }
    }

    public void testDeliveryCountChangedOnSessionClose() throws Exception
    {
        try (Connection connection = getConnectionWithPrefetch(0))
        {
            connection.start();
            for (int i = 0; i < MAX_DELIVERY_ATTEMPTS; i++)
            {
                Session consumingSession = connection.createSession(JMSContext.SESSION_TRANSACTED);
                MessageConsumer consumer = consumingSession.createConsumer(_queue);
                Message message = consumer.receive(getReceiveTimeout());
                assertDeliveryCountHeaders(message, i);
                consumingSession.close();
            }

            Session session = connection.createSession(JMSContext.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(_queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertNull("Message should be discarded", message);
        }
    }

    private void assertDeliveryCountHeaders(final Message message, final int deliveryAttempt) throws JMSException
    {
        assertNotNull(String.format("Message is not received on attempt %d", deliveryAttempt), message);
        assertEquals(String.format("Unexpected redelivered flag on attempt %d", deliveryAttempt),
                     deliveryAttempt > 0,
                     message.getJMSRedelivered());
        assertEquals(String.format("Unexpected message delivery count on attempt %d", deliveryAttempt + 1),
                     deliveryAttempt + 1,
                     message.getIntProperty(JMSX_DELIVERY_COUNT));
    }
}

