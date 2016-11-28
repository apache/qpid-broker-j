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
package org.apache.qpid.test.unit.close;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class MessageConsumerCloseTest  extends QpidBrokerTestCase
{
    private volatile Exception _exception;

    /**
     * JMS Session says "The content of a transaction's input and output units is simply those messages that have
     * been produced and consumed within the session's current transaction.".  Closing a consumer must not therefore
     * prevent previously received messages from being committed.
     */
    public void testConsumerCloseAndSessionCommit() throws Exception
    {
        Connection connection = getConnection();
        connection.start();
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = createTestQueue(session);
        MessageConsumer consumer1 = session.createConsumer(destination);
        sendMessage(session, destination, 2);


        Message message = consumer1.receive(RECEIVE_TIMEOUT);
        assertNotNull("First message is not received", message);
        assertEquals("First message unexpected has unexpected property", 0, message.getIntProperty(INDEX));
        consumer1.close();

        session.commit();

        MessageConsumer consumer2 = session.createConsumer(destination);
        message = consumer2.receive(RECEIVE_TIMEOUT);
        assertNotNull("Second message is not received", message);
        assertEquals("Second message unexpected has unexpected property", 1, message.getIntProperty(INDEX));

        message = consumer2.receive(100l);
        assertNull("Unexpected third message", message);
    }


    public void testConsumerCloseAndSessionRollback() throws Exception
    {
        Connection connection = getConnection();
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = createTestQueue(session);
        MessageConsumer consumer = session.createConsumer(destination);
        sendMessage(session, destination, 2);
        connection.start();
        consumer.setMessageListener(new MessageListener()
        {
            @Override
            public void onMessage(Message message)
            {
                try
                {
                    receiveLatch.countDown();
                    session.rollback();
                }
                catch (JMSException e)
                {
                    _exception = e;
                }
            }
        });
        boolean messageReceived = receiveLatch.await(1l, TimeUnit.SECONDS);
        consumer.close();

        assertNull("Exception occurred on rollback:" + _exception, _exception);
        assertTrue("Message is not received", messageReceived);

        consumer = session.createConsumer(destination);
        final CountDownLatch receiveLatch2 = new CountDownLatch(2);
        consumer.setMessageListener(new MessageListener()
        {
            @Override
            public void onMessage(Message message)
            {
                receiveLatch2.countDown();
            }
        });
        assertTrue( receiveLatch2.await(1l, TimeUnit.SECONDS));
    }

    public void testPrefetchedMessagesReleasedOnConsumerClose() throws Exception
    {
        Connection connection = getConnection();
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        Destination destination = createTestQueue(session);
        MessageConsumer consumer = session.createConsumer(destination);

        sendMessage(session, destination, 3);

        connection.start();

        Message msg1 = consumer.receive(1000);
        assertNotNull("Message one was null", msg1);
        assertEquals("Message one has unexpected content", 0, msg1.getIntProperty(INDEX));
        session.commit();

        // Messages two and three will have been prefetched by the consumer.
        // Closing the consumer must make the available for delivery elsewhere

        consumer.close();

        MessageConsumer consumer2 = session.createConsumer(destination);

        Message msg2 = consumer2.receive(1000);
        Message msg3 = consumer2.receive(1000);
        assertNotNull("Message two was null", msg2);
        assertEquals("Message two has unexpected content", 1, msg2.getIntProperty(INDEX));

        assertNotNull("Message three was null", msg3);
        assertEquals("Message three has unexpected content", 2, msg3.getIntProperty(INDEX));
        session.commit();
    }

    public void testMessagesReceivedBeforeConsumerCloseAreRedeliveredAfterRollback() throws Exception
    {
        Connection connection = getConnection();
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        Destination destination = createTestQueue(session);
        MessageConsumer consumer = session.createConsumer(destination);

        int messageNumber = 4;
        connection.start();
        sendMessage(session, destination, messageNumber);

        for(int i = 0; i < messageNumber/2 ; i++)
        {
            Message message = consumer.receive(RECEIVE_TIMEOUT);
            assertNotNull("Message [" + i +"] was null", message);
            assertEquals("Message [" + i +"] has unexpected content", i, message.getIntProperty(INDEX));
        }

        consumer.close();

        session.rollback();

        MessageConsumer consumer2 = session.createConsumer(destination);

        for(int i = 0; i < messageNumber ; i++)
        {
            Message message = consumer2.receive(RECEIVE_TIMEOUT);
            assertNotNull("Message [" + i +"] was null", message);
            assertEquals("Message [" + i +"] has unexpected content", i, message.getIntProperty(INDEX));
        }

        session.commit();
    }
}
