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
package org.apache.qpid.systests.jms_1_1.extensions.filters;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.JmsTestBase;

public class ArrivalTimeFilterTest extends JmsTestBase
{
    private static final String LEGACY_BINDING_URL = "direct://amq.direct/%s/%s?x-qpid-replay-period='%d'";

    @Test
    public void testQueueDefaultFilterArrivalTime0() throws Exception
    {
        final String queueName = getTestName();
        createDestinationWithFilter(0, queueName);
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = getQueue(queueName);
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            producer.send(session.createTextMessage("B"));

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message should be received");
            assertTrue(receivedMessage instanceof TextMessage, "Unexpected message type");
            assertEquals("B", ((TextMessage) receivedMessage).getText(), "Unexpected message");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testQueueDefaultFilterArrivalTime1000() throws Exception
    {
        final String queueName = getTestName();
        final long period = getReceiveTimeout() / 1000;
        createDestinationWithFilter(period, queueName);
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = getQueue(queueName);
            connection.start();
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            Thread.sleep(getReceiveTimeout() / 4);

            final MessageConsumer consumer = session.createConsumer(queue);

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message A should be received");
            assertTrue(receivedMessage instanceof TextMessage, "Unexpected message type");
            assertEquals("A", ((TextMessage) receivedMessage).getText(), "Unexpected message");

            producer.send(session.createTextMessage("B"));

            final Message secondMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(secondMessage, "Message B should be received");
            assertTrue(secondMessage instanceof TextMessage, "Unexpected message type");
            assertEquals("B", ((TextMessage) secondMessage).getText(), "Unexpected message");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testConsumerFilterArrivalTime0() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "Only legacy client implements this feature");

        final String queueName = getTestName();
        createQueue(queueName);
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, 0));
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            producer.send(session.createTextMessage("B"));

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message should be received");
            assertTrue(receivedMessage instanceof TextMessage, "Unexpected message type");
            assertEquals("B", ((TextMessage) receivedMessage).getText(), "Unexpected message");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testConsumerFilterArrivalTime1000() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "Only legacy client implements this feature");

        final String queueName = getTestName();
        createQueue(queueName);
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue =
                    session.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, getReceiveTimeout()));
            connection.start();
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            Thread.sleep(getReceiveTimeout() / 4);

            final MessageConsumer consumer = session.createConsumer(queue);

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message A should be received");
            assertTrue(receivedMessage instanceof TextMessage, "Unexpected message type");
            assertEquals("A", ((TextMessage) receivedMessage).getText(), "Unexpected message");

            producer.send(session.createTextMessage("B"));

            final Message secondMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(secondMessage, "Message B should be received");
            assertTrue(secondMessage instanceof TextMessage, "Unexpected message type");
            assertEquals("B", ((TextMessage) secondMessage).getText(), "Unexpected message");
        }
        finally
        {
            connection.close();
        }
    }

    private void createDestinationWithFilter(final long period, final String queueName) throws Exception
    {
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue",
                                        Collections.singletonMap("defaultFilters",
                                                                 "{ \"x-qpid-replay-period\" : { \"x-qpid-replay-period\" : [ \""
                                                                 + period
                                                                 + "\" ] } }"));
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", queueName);
        arguments.put("bindingKey", queueName);
        performOperationUsingAmqpManagement("amq.direct",
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            arguments);
    }
}
