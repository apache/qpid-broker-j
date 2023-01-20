/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.systests.jms_1_1.transaction;

import static org.apache.qpid.systests.Utils.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class CommitRollbackTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitRollbackTest.class);

    @Test
    public void produceMessageAndAbortTransactionByClosingConnection() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("A"));
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            connection2.start();
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("B"));

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            TextMessage textMessage = (TextMessage) message;
            assertEquals("B", textMessage.getText(), "Unexpected message received");
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void produceMessageAndAbortTransactionByClosingSession() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer transactedProducer = transactedSession.createProducer(queue);
            transactedProducer.send(transactedSession.createTextMessage("A"));
            transactedSession.close();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("B"));

            connection.start();
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            assertEquals("B", ((TextMessage) message).getText(), "Unexpected message received");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void produceMessageAndRollbackTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("A"));
            session.rollback();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("B"));

            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            TextMessage textMessage = (TextMessage) message;
            assertEquals("B", textMessage.getText(), "Unexpected message received");
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void produceMessageAndCommitTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("A"));
            session.commit();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            TextMessage textMessage = (TextMessage) message;
            assertEquals("A", textMessage.getText(), "Unexpected message received");
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageAndAbortTransactionByClosingConnection() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendTextMessage(connection, queue, "A");

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            TextMessage textMessage = (TextMessage) message;
            assertEquals("A", textMessage.getText(), "Unexpected message received");
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            connection2.start();
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            TextMessage textMessage = (TextMessage) message;
            assertEquals("A", textMessage.getText(), "Unexpected message received");
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageAndAbortTransactionByClosingSession() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendTextMessage(connection, queue, "A");

            connection.start();
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer transactedConsumer = transactedSession.createConsumer(queue);
            Message message = transactedConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            TextMessage textMessage = (TextMessage) message;
            assertEquals("A", textMessage.getText(), "Unexpected message received");

            transactedSession.close();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);

            Message message2 = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message2 instanceof TextMessage, "Text message should be received");
            assertEquals("A", ((TextMessage) message2).getText(), "Unexpected message received");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void receiveMessageAndRollbackTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendTextMessage(connection, queue, "A");

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            TextMessage textMessage = (TextMessage) message;
            assertEquals("A", textMessage.getText(), "Unexpected message received");
            session.rollback();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "Text message should be received");
            TextMessage textMessage = (TextMessage) message;
            assertEquals("A", textMessage.getText(), "Unexpected message received");
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageAndCommitTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 2);

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(message, "Message not received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message received");
            session.commit();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(message, "Message not received");
            assertEquals(1, message.getIntProperty(INDEX), "Unexpected message received");
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageCloseConsumerAndCommitTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 2);

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(message, "Message not received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message received");
            messageConsumer.close();
            session.commit();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(message, "Message not received");
            assertEquals(1, message.getIntProperty(INDEX), "Unexpected message received");
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageCloseConsumerAndRollbackTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 2);

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(message, "Message not received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message received");
            messageConsumer.close();
            session.rollback();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(message, "Message not received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message received");
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void transactionSharedByConsumers() throws Exception
    {
        final Queue queue1 = createQueue("Q1");
        final Queue queue2 = createQueue("Q2");
        Connection connection = getConnection();
        try
        {
            Utils.sendTextMessage(connection, queue1, "queue1Message1");
            Utils.sendTextMessage(connection, queue1, "queue1Message2");
            Utils.sendTextMessage(connection, queue2, "queue2Message1");
            Utils.sendTextMessage(connection, queue2, "queue2Message2");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer1 = session.createConsumer(queue1);
            MessageConsumer messageConsumer2 = session.createConsumer(queue2);
            connection.start();

            Message message1 = messageConsumer1.receive(getReceiveTimeout());
            assertTrue(message1 instanceof TextMessage, "Text message not received from first queue");
            assertEquals("queue1Message1", ((TextMessage) message1).getText(),
                    "Unexpected message received from first queue");

            Message message2 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue(message2 instanceof TextMessage, "Text message not received from second queue");
            assertEquals("queue2Message1", ((TextMessage) message2).getText(),
                    "Unexpected message received from second queue");

            session.rollback();

            message1 = messageConsumer1.receive(getReceiveTimeout());
            assertTrue(message1 instanceof TextMessage, "Text message not received from first queue");
            assertEquals("queue1Message1", ((TextMessage) message1).getText(),
                    "Unexpected message received from first queue");

            message2 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue(message2 instanceof TextMessage, "Text message not received from second queue");
            assertEquals("queue2Message1", ((TextMessage) message2).getText(),
                    "Unexpected message received from second queue");

            session.commit();

            Message message3 = messageConsumer1.receive(getReceiveTimeout());
            assertTrue(message3 instanceof TextMessage, "Text message not received from first queue");
            assertEquals("queue1Message2", ((TextMessage) message3).getText(),
                    "Unexpected message received from first queue");

            Message message4 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue(message4 instanceof TextMessage, "Text message not received from second queue");
            assertEquals("queue2Message2", ((TextMessage) message4).getText(),
                    "Unexpected message received from second queue");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void commitWithinMessageListener() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        int messageNumber = 2;
        try
        {
            Utils.sendMessages(connection, queue, messageNumber);
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final CountDownLatch receiveLatch = new CountDownLatch(messageNumber);
            final AtomicInteger commitCounter = new AtomicInteger();
            final AtomicReference<Throwable> messageConsumerThrowable = new AtomicReference<>();
            MessageConsumer messageConsumer = session.createConsumer(queue);

            messageConsumer.setMessageListener(message ->
            {
                try
                {
                    LOGGER.info("received message " + message);
                    assertEquals(commitCounter.get(), message.getIntProperty(INDEX), "Unexpected message received");
                    LOGGER.info("commit session");
                    session.commit();
                    commitCounter.incrementAndGet();
                }
                catch (Throwable e)
                {
                    messageConsumerThrowable.set(e);
                    LOGGER.error("Unexpected exception", e);
                }
                finally
                {
                    receiveLatch.countDown();
                }
            });
            connection.start();

            assertTrue(receiveLatch.await(getReceiveTimeout() * messageNumber, TimeUnit.MILLISECONDS),
                    "Messages not received in expected time");
            assertNull(messageConsumerThrowable.get(), "Unexpected exception: " + messageConsumerThrowable.get());
            assertEquals(messageNumber, commitCounter.get(), "Unexpected number of commits");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void rollbackWithinMessageListener() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(session, queue, 2);
            connection.start();
            final CountDownLatch receiveLatch = new CountDownLatch(2);
            final AtomicInteger receiveCounter = new AtomicInteger();
            final AtomicReference<Throwable> messageListenerThrowable = new AtomicReference<>();
            consumer.setMessageListener(message -> {
                try
                {
                    if (receiveCounter.incrementAndGet()<3)
                    {
                        session.rollback();
                    }
                    else
                    {
                        session.commit();
                        receiveLatch.countDown();
                    }
                }
                catch (Throwable e)
                {
                    messageListenerThrowable.set(e);
                }
            });

            assertTrue(receiveLatch.await(getReceiveTimeout() * 4, TimeUnit.MILLISECONDS),
                                  "Timeout waiting for messages");
            assertNull(messageListenerThrowable.get(),
                                  "Exception occurred: " + messageListenerThrowable.get());
            assertEquals(4, receiveCounter.get(), "Unexpected number of received messages");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void exhaustedPrefetchInTransaction() throws Exception
    {
        final int maxPrefetch = 2;
        final int messageNumber = maxPrefetch + 1;

        final Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(maxPrefetch).build();
        try
        {
            Utils.sendMessages(connection, queue, messageNumber);
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection.start();

            for (int i = 0; i < maxPrefetch; i++)
            {
                final Message message = messageConsumer.receive(getReceiveTimeout());
                assertNotNull(message, String.format("Message %d not received", i));
                assertEquals(i, message.getIntProperty(INDEX), "Unexpected message received");
            }

            session.rollback();

            for (int i = 0; i < maxPrefetch; i++)
            {
                final Message message = messageConsumer.receive(getReceiveTimeout());
                assertNotNull(message, String.format("Message %d not received after rollback", i));
                assertEquals(i, message.getIntProperty(INDEX), "Unexpected message received after rollback");
            }

            session.commit();

            final Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(message, String.format("Message %d not received", maxPrefetch));
            assertEquals(maxPrefetch, message.getIntProperty(INDEX), "Unexpected message received");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testMessageOrder() throws Exception
    {
        final int messageNumber = 4;
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(connection, queue, messageNumber);
            connection.start();

            int messageSeen = 0;
            int expectedIndex = 0;
            while (expectedIndex < messageNumber)
            {
                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull(message, String.format("Expected message '%d' is not received", expectedIndex));
                assertEquals(expectedIndex, message.getIntProperty(INDEX), "Received message out of order");

                //don't commit transaction for the message until we receive it 5 times
                if (messageSeen < 5)
                {
                    // receive remaining
                    for (int m = expectedIndex + 1; m < messageNumber; m++)
                    {
                        Message remaining = consumer.receive(getReceiveTimeout());
                        assertNotNull(message, String.format("Expected remaining message '%d' is not received", m));
                        assertEquals(m, remaining.getIntProperty(INDEX), "Received remaining message out of order");
                    }

                    LOGGER.debug(String.format("Rolling back transaction for message with index %d", expectedIndex));
                    session.rollback();
                    messageSeen++;
                }
                else
                {
                    LOGGER.debug(String.format("Committing transaction for message with index %d", expectedIndex));
                    messageSeen = 0;
                    expectedIndex++;
                    session.commit();
                }
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testCommitOnClosedConnection() throws Exception
    {
        Session transactedSession;
        Connection connection = getConnection();
        try
        {
            transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        }
        finally
        {
            connection.close();
        }

        assertNotNull(transactedSession, "Session cannot be null");
        try
        {
            transactedSession.commit();
            fail("Commit on closed connection should throw IllegalStateException!");
        }
        catch (IllegalStateException e)
        {
            // passed
        }
    }

    @Test
    public void testCommitOnClosedSession() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            transactedSession.close();
            try
            {
                transactedSession.commit();
                fail("Commit on closed session should throw IllegalStateException!");
            }
            catch (IllegalStateException e)
            {
                // passed
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testRollbackOnClosedSession() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            transactedSession.close();
            try
            {
                transactedSession.rollback();
                fail("Rollback on closed session should throw IllegalStateException!");
            }
            catch (IllegalStateException e)
            {
                // passed
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testGetTransactedOnClosedSession() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            transactedSession.close();
            try
            {
                transactedSession.getTransacted();
                fail("According to Sun TCK invocation of Session#getTransacted on closed session should throw IllegalStateException!");
            }
            catch (IllegalStateException e)
            {
                // passed
            }
        }
        finally
        {
            connection.close();
        }
    }
}
