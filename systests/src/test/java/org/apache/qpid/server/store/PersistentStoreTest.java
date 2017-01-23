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

package org.apache.qpid.server.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class PersistentStoreTest extends QpidBrokerTestCase
{
    private static final int NUM_MESSAGES = 100;
    private Connection _con;
    private Session _session;
    private Destination _destination;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _con = getConnection();
    }

    public void testCommittedMessagesSurviveBrokerNormalShutdown() throws Exception
    {
        sendAndCommitMessages();
        stopDefaultBroker();
        startDefaultBroker();
        confirmBrokerStillHasCommittedMessages();
    }

    public void testCommittedMessagesSurviveBrokerAbnormalShutdown() throws Exception
    {
        if (isInternalBroker())
        {
            return;
        }

        sendAndCommitMessages();
        killDefaultBroker();
        startDefaultBroker();
        confirmBrokerStillHasCommittedMessages();
    }

    public void testCommittedMessagesSurviveBrokerNormalShutdownMidTransaction() throws Exception
    {
        sendAndCommitMessages();
        sendMoreMessagesWithoutCommitting();
        stopDefaultBroker();
        startDefaultBroker();
        confirmBrokerStillHasCommittedMessages();
    }

    public void testCommittedMessagesSurviveBrokerAbnormalShutdownMidTransaction() throws Exception
    {
        if (isInternalBroker())
        {
            return;
        }
        sendAndCommitMessages();
        sendMoreMessagesWithoutCommitting();
        killDefaultBroker();
        startDefaultBroker();
        confirmBrokerStillHasCommittedMessages();
    }

    public void testHeaderPersistence() throws Exception
    {
        String testQueueName = getTestQueueName();
        String replyToQueue = testQueueName + "_reply";
        _con.start();
        _session = _con.createSession(true, Session.SESSION_TRANSACTED);
        _destination = createTestQueue(_session, testQueueName);
        Destination replyTo = createTestQueue(_session, replyToQueue);
        MessageConsumer consumer = _session.createConsumer(_destination);
        MessageProducer producer = _session.createProducer(_destination);

        final long expiration = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
        final int priority = 3;
        final String propertyKey = "mystring";
        final String propertyValue = "string";

        Message msg = _session.createMessage();
        msg.setStringProperty(propertyKey, propertyValue);
        msg.setJMSExpiration(expiration);
        msg.setJMSReplyTo(replyTo);

        producer.send(msg, DeliveryMode.PERSISTENT, priority, expiration);
        _session.commit();

        final String sentMessageId = msg.getJMSMessageID();

        Message receivedMessage = consumer.receive(getReceiveTimeout());
        long receivedJmsExpiration = receivedMessage.getJMSExpiration();
        assertEquals("Unexpected JMS message id", sentMessageId, receivedMessage.getJMSMessageID());
        assertEquals("Unexpected JMS replyto", replyTo, receivedMessage.getJMSReplyTo());
        assertEquals("Unexpected JMS priority", priority, receivedMessage.getJMSPriority());
        assertTrue("Expecting expiration to be in the future", receivedJmsExpiration > 0);
        assertTrue("Expecting user property to be present", receivedMessage.propertyExists(propertyKey));
        assertEquals("Unexpected user property", propertyValue, receivedMessage.getStringProperty(propertyKey));
        // Do not commit message so we can re-receive after Broker restart

        stopDefaultBroker();
        startDefaultBroker();

        _con = getConnection();
        _con.start();
        _session = _con.createSession(true, Session.SESSION_TRANSACTED);
        consumer = _session.createConsumer(_destination);

        Message rereceivedMessage = consumer.receive(getReceiveTimeout());
        assertEquals("Unexpected JMS message id", sentMessageId, rereceivedMessage.getJMSMessageID());
        assertEquals("Unexpected JMS replyto", replyTo, rereceivedMessage.getJMSReplyTo());
        assertEquals("Unexpected JMS priority", priority, rereceivedMessage.getJMSPriority());
        assertEquals("Expecting expiration to be unchanged", receivedJmsExpiration, rereceivedMessage.getJMSExpiration());
        assertTrue("Expecting user property to be present", rereceivedMessage.propertyExists(propertyKey));
        assertEquals("Unexpected user property", propertyValue, rereceivedMessage.getStringProperty(propertyKey));
        _session.commit();
    }

    private void sendAndCommitMessages() throws Exception
    {
        _session = _con.createSession(true, Session.SESSION_TRANSACTED);
        _destination = createTestQueue(_session);

        sendMessage(_session, _destination, NUM_MESSAGES);
        _session.commit();
    }

    private void sendMoreMessagesWithoutCommitting() throws Exception
    {
        sendMessage(_session, _destination, NUM_MESSAGES);
    }

    private void confirmBrokerStillHasCommittedMessages() throws Exception
    {
        Connection con = getConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        con.start();
        Destination destination = session.createQueue(getTestQueueName());
        MessageConsumer consumer = session.createConsumer(destination);
        for (int i = 1; i <= NUM_MESSAGES; i++)
        {
            Message msg = consumer.receive(getReceiveTimeout());
            assertNotNull("Message " + i + " not received", msg);
            assertEquals("Did not receive the expected message", i, msg.getIntProperty(INDEX));
        }

        Message msg = consumer.receive(getShortReceiveTimeout());
        if(msg != null)
        {
            fail("No more messages should be received, but received additional message with index: " + msg.getIntProperty(INDEX));
        }
    }

    /**
     * This test requires that we can send messages without committing.
     * QTC always commits the messages sent via sendMessages.
     *
     * @param session the session to use for sending
     * @param destination where to send them to
     * @param count no. of messages to send
     *
     * @return the sent messages
     *
     * @throws Exception
     */
    @Override
    public List<Message> sendMessage(Session session, Destination destination,
                                     int count) throws Exception
    {
        List<Message> messages = new ArrayList<>(count);

        MessageProducer producer = session.createProducer(destination);

        for (int i = 1; i <= count; i++)
        {
            Message next = createNextMessage(session, i);

            producer.send(next);

            messages.add(next);
        }

        return messages;
    }

}
