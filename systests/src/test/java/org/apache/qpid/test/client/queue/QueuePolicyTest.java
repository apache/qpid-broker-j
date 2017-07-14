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
package org.apache.qpid.test.client.queue;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class QueuePolicyTest extends QpidBrokerTestCase
{
    private Connection _connection;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
    }

    public void testRejectPolicyMessageDepth() throws Exception
    {
        Session session = _connection.createSession(true, Session.SESSION_TRANSACTED);

        Destination destination = createQueue(session, OverflowPolicy.REJECT, 5);
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
            if (isJavaBroker())
            {
                assertTrue("Unexpected exception: " + e.getMessage(),
                           e.getMessage().contains("Maximum depth exceeded"));
            }
        }

        Connection secondConnection = getConnection();
        secondConnection.start();

        Session secondSession = secondConnection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = secondSession.createConsumer(destination);
        Message receivedMessage = consumer.receive(getReceiveTimeout());
        assertNotNull("Message  is not received", receivedMessage);
        secondSession.commit();

        MessageProducer secondProducer = secondSession.createProducer(destination);
        secondProducer.send(secondSession.createMessage());
        secondSession.commit();
    }

    public void testRingPolicy() throws Exception
    {
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = createQueue(session, OverflowPolicy.RING, 2);
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createTextMessage("Test1"));
        producer.send(session.createTextMessage("Test2"));
        producer.send(session.createTextMessage("Test3"));

        MessageConsumer consumer = session.createConsumer(destination);
        _connection.start();

        TextMessage receivedMessage = (TextMessage) consumer.receive(getReceiveTimeout());
        assertNotNull("The consumer should receive the receivedMessage with body='Test2'", receivedMessage);
        assertEquals("Unexpected first message", "Test2", receivedMessage.getText());

        receivedMessage = (TextMessage) consumer.receive(getReceiveTimeout());
        assertNotNull("The consumer should receive the receivedMessage with body='Test3'", receivedMessage);
        assertEquals("Unexpected second message", "Test3", receivedMessage.getText());
    }


    private Destination createQueue(Session session, OverflowPolicy overflowPolicy, int msgLimit)
            throws Exception
    {
        Destination destination;
        String testQueueName = getTestQueueName();
        if (isBroker10())
        {
            final Map<String, Object> arguments = new HashMap<>();
            arguments.put(org.apache.qpid.server.model.Queue.OVERFLOW_POLICY, overflowPolicy.name());
            arguments.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, msgLimit);
            createEntityUsingAmqpManagement(testQueueName, session, "org.apache.qpid.Queue", arguments);
            destination = getQueueFromName(session, testQueueName);
        }
        else
        {
            String address = String.format("ADDR: %s; {create: always, node: {"
                                           + "x-bindings: [{exchange : 'amq.direct', key : %s}],"
                                           + "x-declare:{arguments : {'qpid.policy_type': %s, 'qpid.max_count': %d}}"
                                           + "}}",
                                           testQueueName,
                                           testQueueName, overflowPolicy.name().toLowerCase(), msgLimit);
            destination = session.createQueue(address);
            session.createConsumer(destination).close();
        }
        return destination;
    }
}
