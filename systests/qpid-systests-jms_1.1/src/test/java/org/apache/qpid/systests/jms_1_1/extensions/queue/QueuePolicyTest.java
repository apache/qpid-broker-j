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
package org.apache.qpid.systests.jms_1_1.extensions.queue;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.systests.JmsTestBase;

public class QueuePolicyTest extends JmsTestBase
{
    @Test
    public void testRejectPolicyMessageDepth() throws Exception
    {
        Destination destination = createQueue(getTestName(), OverflowPolicy.REJECT, 5);
        Connection connection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(destination);

            for (int i = 0; i < 5; i++)
            {
                producer.send(session.createMessage());
                session.commit();
            }

            try
            {
                producer.send(session.createMessage());
                session.commit();
                fail("The client did not receive an exception after exceeding the queue limit");
            }
            catch (JMSException e)
            {
                // pass
            }
        }
        finally
        {
            connection.close();
        }

        Connection secondConnection = getConnection();
        try
        {
            secondConnection.start();

            Session secondSession = secondConnection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = secondSession.createConsumer(destination);
            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message  is not received");
            secondSession.commit();

            MessageProducer secondProducer = secondSession.createProducer(destination);
            secondProducer.send(secondSession.createMessage());
            secondSession.commit();
        }
        finally
        {
            secondConnection.close();
        }
    }

    @Test
    public void testRingPolicy() throws Exception
    {
        Destination destination = createQueue(getTestName(), OverflowPolicy.RING, 2);
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(destination);
            producer.send(session.createTextMessage("Test1"));
            producer.send(session.createTextMessage("Test2"));
            producer.send(session.createTextMessage("Test3"));

            MessageConsumer consumer = session.createConsumer(destination);
            connection.start();

            TextMessage receivedMessage = (TextMessage) consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "The consumer should receive the receivedMessage with body='Test2'");
            assertEquals("Test2", receivedMessage.getText(), "Unexpected first message");

            receivedMessage = (TextMessage) consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "The consumer should receive the receivedMessage with body='Test3'");
            assertEquals("Test3", receivedMessage.getText(), "Unexpected second message");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testRoundtripWithFlowToDisk() throws Exception
    {
        assumeTrue(getBrokerAdmin().supportsRestart(), "Test requires persistent store");

        String queueName = getTestName();
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.OVERFLOW_POLICY, OverflowPolicy.FLOW_TO_DISK.name());
        arguments.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 0L);
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", arguments);
        Queue queue = createQueue(queueName);

        Map<String, Object> statistics = getVirtualHostStatistics("bytesEvacuatedFromMemory");
        Long originalBytesEvacuatedFromMemory = (Long) statistics.get("bytesEvacuatedFromMemory");

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            TextMessage message = session.createTextMessage("testMessage");
            MessageProducer producer = session.createProducer(queue);
            producer.send(message);
            session.commit();

            // make sure we are flowing to disk
            Map<String, Object> statistics2 = getVirtualHostStatistics("bytesEvacuatedFromMemory");
            Long bytesEvacuatedFromMemory = (Long) statistics2.get("bytesEvacuatedFromMemory");
            assertTrue(bytesEvacuatedFromMemory > originalBytesEvacuatedFromMemory,
                    "Message was not evacuated from memory");

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Did not receive message");
            assertThat("Unexpected message type", receivedMessage, is(instanceOf(TextMessage.class)));
            assertEquals(message.getText(), ((TextMessage) receivedMessage).getText(),
                    "Unexpected message content");
        }
        finally
        {
            connection.close();
        }
    }

    private Destination createQueue(final String queueName, OverflowPolicy overflowPolicy, int messageLimit)
            throws Exception
    {
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.OVERFLOW_POLICY, overflowPolicy.name());
        arguments.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, messageLimit);
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", arguments);
        return createQueue(queueName);
    }
}
