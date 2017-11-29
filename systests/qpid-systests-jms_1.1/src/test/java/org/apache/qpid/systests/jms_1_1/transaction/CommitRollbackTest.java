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

import static junit.framework.TestCase.fail;
import static org.apache.qpid.systests.Utils.INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class CommitRollbackTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitRollbackTest.class);

    @Test
    public void produceMessageAndAbortTransaction() throws Exception
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
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "B", textMessage.getText());
        }
        finally
        {
            connection2.close();
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
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "B", textMessage.getText());
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
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageAndAbortTransaction() throws Exception
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
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
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
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
        }
        finally
        {
            connection2.close();
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
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
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
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
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
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 0, message.getIntProperty(INDEX));
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
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 1, message.getIntProperty(INDEX));
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
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 0, message.getIntProperty(INDEX));
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
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 1, message.getIntProperty(INDEX));
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
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 0, message.getIntProperty(INDEX));
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
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 0, message.getIntProperty(INDEX));
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
            assertTrue("Text message not received from first queue", message1 instanceof TextMessage);
            assertEquals("Unexpected message received from first queue",
                         "queue1Message1",
                         ((TextMessage) message1).getText());

            Message message2 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue("Text message not received from second queue", message2 instanceof TextMessage);
            assertEquals("Unexpected message received from second queue",
                         "queue2Message1",
                         ((TextMessage) message2).getText());

            session.rollback();

            message1 = messageConsumer1.receive(getReceiveTimeout());
            assertTrue("Text message not received from first queue", message1 instanceof TextMessage);
            assertEquals("Unexpected message received from first queue",
                         "queue1Message1",
                         ((TextMessage) message1).getText());

            message2 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue("Text message not received from second queue", message2 instanceof TextMessage);
            assertEquals("Unexpected message received from second queue",
                         "queue2Message1",
                         ((TextMessage) message2).getText());

            session.commit();

            Message message3 = messageConsumer1.receive(getReceiveTimeout());
            assertTrue("Text message not received from first queue", message3 instanceof TextMessage);
            assertEquals("Unexpected message received from first queue",
                         "queue1Message2",
                         ((TextMessage) message3).getText());

            Message message4 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue("Text message not received from second queue", message4 instanceof TextMessage);
            assertEquals("Unexpected message received from second queue",
                         "queue2Message2",
                         ((TextMessage) message4).getText());
        }
        finally
        {
            connection.close();
        }
    }

    public void testCommitWithinOnMessage() throws Exception
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

            messageConsumer.setMessageListener(message -> {
                try
                {
                    LOGGER.info("received message " + message);
                    assertEquals("Wrong message received", message.getJMSCorrelationID(), "m1");
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

            assertTrue("Messages not received in expected time",
                       receiveLatch.await(getReceiveTimeout() * messageNumber, TimeUnit.MILLISECONDS));
            assertNull("Unexpected exception: " + messageConsumerThrowable.get(), messageConsumerThrowable.get());
            assertEquals("Unexpected number of commits", messageNumber, commitCounter.get());
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
                assertNotNull(String.format("Message %d not received", i), message);
                assertEquals("Unexpected message received", i, message.getIntProperty(INDEX));
            }

            session.rollback();

            for (int i = 0; i < maxPrefetch; i++)
            {
                final Message message = messageConsumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message %d not received after rollback", i), message);
                assertEquals("Unexpected message received after rollback", i, message.getIntProperty(INDEX));
            }

            session.commit();

            final Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(String.format("Message %d not received", maxPrefetch), message);
            assertEquals("Unexpected message received", maxPrefetch, message.getIntProperty(INDEX));
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
                assertNotNull(String.format("Expected message '%d' is not received", expectedIndex), message);
                assertEquals("Received message out of order", expectedIndex, message.getIntProperty(INDEX));

                //don't commit transaction for the message until we receive it 5 times
                if (messageSeen < 5)
                {
                    // receive remaining
                    for (int m = expectedIndex + 1; m < messageNumber; m++)
                    {
                        Message remaining = consumer.receive(getReceiveTimeout());
                        assertNotNull(String.format("Expected remaining message '%d' is not received", m), message);
                        assertEquals("Received remained message out of order", m, remaining.getIntProperty(INDEX));
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
    public void testRollbackSoak() throws Exception
    {
        final int messageNumber = 20;
        final int maximumRollbacks = messageNumber * 2;
        final int numberOfConsumers = 2;
        long timeout = getReceiveTimeout() * (maximumRollbacks + messageNumber);
        final AtomicInteger rollbackCounter = new AtomicInteger();
        final AtomicInteger commitCounter = new AtomicInteger();
        final AtomicBoolean shutdown = new AtomicBoolean();
        List<ListenableFuture<Void>> consumerFutures = new ArrayList<>(numberOfConsumers);
        final Queue queue = createQueue(getTestName());
        final Connection connection = getConnectionBuilder().setPrefetch(messageNumber / numberOfConsumers).build();
        try
        {
            Utils.sendMessages(connection, queue, messageNumber);
            final ListeningExecutorService threadPool =
                    MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numberOfConsumers));
            try
            {
                for (int i = 0; i < numberOfConsumers; ++i)
                {
                    final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                    final MessageConsumer consumer = session.createConsumer(queue);
                    consumerFutures.add(threadPool.submit(() -> {
                        do
                        {
                            Message m = consumer.receive(getReceiveTimeout());
                            if (m != null)
                            {
                                if (rollbackCounter.incrementAndGet() <= maximumRollbacks)
                                {
                                    session.rollback();
                                }
                                else
                                {
                                    session.commit();
                                    commitCounter.incrementAndGet();
                                }
                            }
                        }
                        while (commitCounter.get() < messageNumber
                               && !Thread.currentThread().isInterrupted()
                               && !shutdown.get());
                        return null;
                    }));
                }
                connection.start();

                final ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(consumerFutures);
                try
                {
                    combinedFuture.get(timeout, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e)
                {
                    fail(String.format(
                            "Test took more than %.1f seconds. All consumers probably starved."
                            + " Performed %d rollbacks, consumed %d/%d messages",
                            timeout / 1000.,
                            rollbackCounter.get(),
                            commitCounter.get(),
                            messageNumber));
                }
                finally
                {
                    shutdown.set(true);
                }
            }
            finally
            {
                threadPool.shutdown();
                threadPool.awaitTermination(timeout, TimeUnit.MILLISECONDS);
            }
        }
        finally
        {
            connection.close();
        }
    }
}
