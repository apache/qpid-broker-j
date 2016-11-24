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

package org.apache.qpid.test.unit.client.temporaryqueue;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 * Tests the behaviour of {@link TemporaryQueue}.
 */
public class TemporaryQueueTest extends QpidBrokerTestCase
{
    /**
     * Tests the basic produce/consume behaviour of a temporary queue.
     */
    public void testMessageDeliveryUsingTemporaryQueue() throws Exception
    {
        final Connection conn = getConnection();
        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TemporaryQueue queue = session.createTemporaryQueue();
        assertNotNull(queue);
        final MessageProducer producer = session.createProducer(queue);
        final MessageConsumer consumer = session.createConsumer(queue);
        conn.start();
        producer.send(session.createTextMessage("hello"));
        TextMessage tm = (TextMessage) consumer.receive(RECEIVE_TIMEOUT);
        assertNotNull("Message not received", tm);
        assertEquals("hello", tm.getText());
    }

    /**
     * Tests that a temporary queue cannot be used by a MessageConsumer on another Connection.
     */
    public void testConsumeFromAnotherConnectionProhibited() throws Exception
    {
        final Connection conn = getConnection();
        final Connection conn2 = getConnection();
        final Session session1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TemporaryQueue queue = session1.createTemporaryQueue();
        assertNotNull(queue);

        try
        {
            session2.createConsumer(queue);
            fail("Expected a JMSException when subscribing to a temporary queue created on a different session");
        }
        catch (JMSException je)
        {
            //pass
            assertEquals(isBroker10() ? "Can't consume from a temporary destination created using another connection" : "Cannot consume from a temporary destination created on another connection", je.getMessage());
        }
    }

    /**
     * Tests that a temporary queue can be used by a MessageProducer on another Connection.
     */
    public void testPublishFromAnotherConnectionAllowed() throws Exception
    {
        final Connection conn = getConnection();
        final Connection conn2 = getConnection();
        final Session session1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TemporaryQueue queue = session1.createTemporaryQueue();
        assertNotNull(queue);

        MessageProducer producer = session2.createProducer(queue);
        producer.send(session2.createMessage());

        conn.start();
        MessageConsumer consumer = session1.createConsumer(queue);
        Message message = consumer.receive(RECEIVE_TIMEOUT);
        assertNotNull("Message not received", message);
    }


    public void testClosingConsumerDoesNotDeleteQueue() throws Exception
    {
        final Connection conn = getConnection();
        final Session session1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TemporaryQueue queue = session1.createTemporaryQueue();
        assertNotNull(queue);

        MessageConsumer consumer1 = session1.createConsumer(queue);

        MessageProducer producer = session1.createProducer(queue);

        producer.send(session1.createTextMessage("Hello World!"));

        consumer1.close();

        conn.start();

        MessageConsumer consumer2 = session1.createConsumer(queue);

        Message message = consumer2.receive(RECEIVE_TIMEOUT);

        assertNotNull("Message should have been received", message);
        assertTrue("Received message not a text message", message instanceof TextMessage);
        assertEquals("Incorrect message text", "Hello World!",  ((TextMessage)message).getText());
    }


    public void testClosingSessionDoesNotDeleteQueue() throws Exception
    {
        final Connection conn = getConnection();
        final Session session1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final TemporaryQueue queue = session1.createTemporaryQueue();
        assertNotNull(queue);

        MessageConsumer consumer1 = session1.createConsumer(queue);

        MessageProducer producer = session1.createProducer(queue);

        producer.send(session1.createTextMessage("Hello World!"));

        session1.close();

        conn.start();

        MessageConsumer consumer2 = session2.createConsumer(queue);

        Message message = consumer2.receive(RECEIVE_TIMEOUT);

        assertNotNull("Message should have been received", message);
        assertTrue("Received message not a text message", message instanceof TextMessage);
        assertEquals("Incorrect message text", "Hello World!",  ((TextMessage)message).getText());
    }


    /**
     * Tests that the client is able to explicitly delete a temporary queue using
     * {@link TemporaryQueue#delete()} and is prevented from deleting one that
     * still has consumers.
     *
     */
    public void testExplictTemporaryQueueDeletion() throws Exception
    {
        final Connection conn = getConnection();
        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final AMQSession<?, ?> amqSession = (AMQSession<?, ?>)session; // Required to observe the queue binding on the Broker
        final TemporaryQueue queue = session.createTemporaryQueue();
        assertNotNull(queue);
        final MessageConsumer consumer = session.createConsumer(queue);
        conn.start();

        assertTrue("Queue should be bound", amqSession.isQueueBound((AMQDestination)queue));

        try
        {
            queue.delete();
            fail("Expected JMSException : should not be able to delete while there are active consumers");
        }
        catch (JMSException je)
        {
            //pass
            if(isBroker10())
            {
                assertEquals("A consumer is consuming from the temporary destination", je.getMessage());
            }
            else
            {
                assertEquals("Temporary Queue has consumers so cannot be deleted", je.getMessage());
            }
        }
        consumer.close();

        // Now deletion should succeed.
        queue.delete();

        try
        {
            session.createConsumer(queue);
            fail("Exception not thrown");
        }
        catch (JMSException je)
        {
            //pass
            assertEquals("Cannot consume from a deleted destination", je.getMessage());
        }

        assertFalse("Queue should no longer be bound", amqSession.isQueueBound((AMQDestination)queue));

    }
}
