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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.NamingException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.queue.LastValueQueue;
import org.apache.qpid.systests.JmsTestBase;

public class LastValueQueueTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LastValueQueueTest.class);

    private static final String MESSAGE_SEQUENCE_NUMBER_PROPERTY = "msg";
    private static final String KEY_PROPERTY = "key";

    private static final int MSG_COUNT = 400;

    @Test
    public void testConflation() throws Exception
    {
        String queueName = getTestName();
        createConflationQueue(queueName, KEY_PROPERTY, false);
        Queue queue = createQueue(queueName);

        sendMessages(queue, 0, MSG_COUNT);

        Connection consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            List<Message> messages = new ArrayList<>();
            Message received;
            while ((received = consumer.receive(getReceiveTimeout())) != null)
            {
                messages.add(received);
            }

            assertEquals("Unexpected number of messages received", 10, messages.size());

            for (int i = 0; i < 10; i++)
            {
                Message msg = messages.get(i);
                assertEquals("Unexpected message number received",
                             MSG_COUNT - 10 + i,
                             msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testConflationWithRelease() throws Exception
    {
        String queueName = getTestName();
        createConflationQueue(queueName, KEY_PROPERTY, false);
        Queue queue = createQueue(queueName);

        sendMessages(queue, 0, MSG_COUNT / 2);

        Connection consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message received;
            List<Message> messages = new ArrayList<>();
            while ((received = consumer.receive(getReceiveTimeout())) != null)
            {
                messages.add(received);
            }

            assertEquals("Unexpected number of messages received", 10, messages.size());

            for (int i = 0; i < 10; i++)
            {
                Message msg = messages.get(i);
                assertEquals("Unexpected message number received",
                             MSG_COUNT / 2 - 10 + i,
                             msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }

        sendMessages(queue, MSG_COUNT / 2, MSG_COUNT);

        consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message received;
            List<Message> messages = new ArrayList<>();
            while ((received = consumer.receive(getReceiveTimeout())) != null)
            {
                messages.add(received);
            }

            assertEquals("Unexpected number of messages received", 10, messages.size());

            for (int i = 0; i < 10; i++)
            {
                Message msg = messages.get(i);
                assertEquals("Unexpected message number received",
                             MSG_COUNT - 10 + i,
                             msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testConflationWithReleaseAfterNewPublish() throws Exception
    {
        String queueName = getTestName();
        createConflationQueue(queueName, KEY_PROPERTY, false);
        Queue queue = createQueue(queueName);

        sendMessages(queue, 0, MSG_COUNT / 2);

        Connection consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message received;
            List<Message> messages = new ArrayList<>();
            while ((received = consumer.receive(getReceiveTimeout())) != null)
            {
                messages.add(received);
            }

            assertEquals("Unexpected number of messages received", 10, messages.size());

            for (int i = 0; i < 10; i++)
            {
                Message msg = messages.get(i);
                assertEquals("Unexpected message number received",
                             MSG_COUNT / 2 - 10 + i,
                             msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }

            consumer.close();

            sendMessages(queue, MSG_COUNT / 2, MSG_COUNT);

            consumerSession.close();
        }
        finally
        {
            consumerConnection.close();
        }

        consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message received;
            List<Message> messages = new ArrayList<>();
            while ((received = consumer.receive(getReceiveTimeout())) != null)
            {
                messages.add(received);
            }

            assertEquals("Unexpected number of messages received", 10, messages.size());

            for (int i = 0; i < 10; i++)
            {
                Message msg = messages.get(i);
                assertEquals("Unexpected message number received",
                             MSG_COUNT - 10 + i,
                             msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testConflatedQueueDepth() throws Exception
    {
        String queueName = getTestName();
        createConflationQueue(queueName, KEY_PROPERTY, false);
        Queue queue = createQueue(queueName);

        sendMessages(queue, 0, MSG_COUNT);

        final long queueDepth = getTotalDepthOfQueuesMessages();

        assertEquals(10, queueDepth);
    }

    @Test
    public void testConflationBrowser() throws Exception
    {
        String queueName = getTestName();
        createConflationQueue(queueName, KEY_PROPERTY, true);
        Queue queue = createQueue(queueName);

        sendMessages(queue, 0, MSG_COUNT);

        Connection consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();
            Message received;
            List<Message> messages = new ArrayList<>();
            while ((received = consumer.receive(getReceiveTimeout())) != null)
            {
                messages.add(received);
            }

            assertEquals("Unexpected number of messages received", 10, messages.size());

            for (int i = 0; i < 10; i++)
            {
                Message msg = messages.get(i);
                assertEquals("Unexpected message number received",
                             MSG_COUNT - 10 + i,
                             msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }

            messages.clear();

            sendMessages(queue, MSG_COUNT, MSG_COUNT + 1);

            while ((received = consumer.receive(getReceiveTimeout())) != null)
            {
                messages.add(received);
            }
            assertEquals("Unexpected number of messages received", 1, messages.size());
            assertEquals("Unexpected message number received",
                         MSG_COUNT,
                         messages.get(0).getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testConflation2Browsers() throws Exception
    {
        String queueName = getTestName();
        createConflationQueue(queueName, KEY_PROPERTY, true);
        Queue queue = createQueue(queueName);

        sendMessages(queue, 0, MSG_COUNT);

        Connection consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            MessageConsumer consumer2 = consumerSession.createConsumer(queue);

            consumerConnection.start();
            List<Message> messages = new ArrayList<>();
            List<Message> messages2 = new ArrayList<>();
            Message received = consumer.receive(getReceiveTimeout());
            Message received2 = consumer2.receive(getReceiveTimeout());

            while (received != null || received2 != null)
            {
                if (received != null)
                {
                    messages.add(received);
                }
                if (received2 != null)
                {
                    messages2.add(received2);
                }

                received = consumer.receive(getReceiveTimeout());
                received2 = consumer2.receive(getReceiveTimeout());
            }

            assertEquals("Unexpected number of messages received on first browser", 10, messages.size());
            assertEquals("Unexpected number of messages received on second browser", 10, messages2.size());

            for (int i = 0; i < 10; i++)
            {
                Message msg = messages.get(i);
                assertEquals("Unexpected message number received on first browser",
                             MSG_COUNT - 10 + i,
                             msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
                msg = messages2.get(i);
                assertEquals("Unexpected message number received on second browser",
                             MSG_COUNT - 10 + i,
                             msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testParallelProductionAndConsumption() throws Exception
    {
        String queueName = getTestName();
        createConflationQueue(queueName, KEY_PROPERTY, true);
        Queue queue = createQueue(queueName);

        // Start producing threads that send messages
        BackgroundMessageProducer messageProducer1 = new BackgroundMessageProducer("Message sender1", queue);
        messageProducer1.startSendingMessages();
        BackgroundMessageProducer messageProducer2 = new BackgroundMessageProducer("Message sender2", queue);
        messageProducer2.startSendingMessages();

        Map<String, Integer> lastReceivedMessages = receiveMessages(messageProducer1, queue);

        messageProducer1.join();
        messageProducer2.join();

        final Map<String, Integer> lastSentMessages1 = messageProducer1.getMessageSequenceNumbersByKey();
        assertEquals("Unexpected number of last sent messages sent by producer1", 2, lastSentMessages1.size());
        final Map<String, Integer> lastSentMessages2 = messageProducer2.getMessageSequenceNumbersByKey();
        assertEquals(lastSentMessages1, lastSentMessages2);

        assertEquals("The last message sent for each key should match the last message received for that key",
                     lastSentMessages1, lastReceivedMessages);

        assertNull("Unexpected exception from background producer thread", messageProducer1.getException());
    }

    private Map<String, Integer> receiveMessages(BackgroundMessageProducer producer, final Queue queue) throws Exception
    {
        producer.waitUntilQuarterOfMessagesSentToEncourageConflation();

        Map<String, Integer> messageSequenceNumbersByKey = new HashMap<>();
        Connection consumerConnection = getConnectionBuilder().setPrefetch(1).build();
        try
        {

            Session _consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            LOGGER.info("Starting to receive");

            MessageConsumer _consumer = _consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message message;
            int numberOfShutdownsReceived = 0;
            int numberOfMessagesReceived = 0;
            while (numberOfShutdownsReceived < 2)
            {
                message = _consumer.receive(getReceiveTimeout());
                assertNotNull("null received after "
                              + numberOfMessagesReceived
                              + " messages and "
                              + numberOfShutdownsReceived
                              + " shutdowns", message);

                if (message.propertyExists(BackgroundMessageProducer.SHUTDOWN))
                {
                    numberOfShutdownsReceived++;
                }
                else
                {
                    numberOfMessagesReceived++;
                    putMessageInMap(message, messageSequenceNumbersByKey);
                }
            }

            LOGGER.info("Finished receiving.  Received " + numberOfMessagesReceived + " message(s) in total");
        }
        finally
        {
            consumerConnection.close();
        }
        return messageSequenceNumbersByKey;
    }

    private void putMessageInMap(Message message, Map<String, Integer> messageSequenceNumbersByKey) throws JMSException
    {
        String keyValue = message.getStringProperty(KEY_PROPERTY);
        Integer messageSequenceNumber = message.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY);
        messageSequenceNumbersByKey.put(keyValue, messageSequenceNumber);
    }

    private class BackgroundMessageProducer
    {
        static final String SHUTDOWN = "SHUTDOWN";

        private final String _threadName;
        private final Queue _queue;

        private volatile Exception _exception;

        private Thread _thread;
        private Map<String, Integer> _messageSequenceNumbersByKey = new HashMap<>();
        private CountDownLatch _quarterOfMessagesSentLatch = new CountDownLatch(MSG_COUNT / 4);

        BackgroundMessageProducer(String threadName, Queue queue)
        {
            _threadName = threadName;
            _queue = queue;
        }

        void waitUntilQuarterOfMessagesSentToEncourageConflation() throws InterruptedException
        {
            final long latchTimeout = 60000;
            boolean success = _quarterOfMessagesSentLatch.await(latchTimeout, TimeUnit.MILLISECONDS);
            assertTrue("Failed to be notified that 1/4 of the messages have been sent within " + latchTimeout + " ms.",
                       success);
            LOGGER.info("Quarter of messages sent");
        }

        public Exception getException()
        {
            return _exception;
        }

        Map<String, Integer> getMessageSequenceNumbersByKey()
        {
            return Collections.unmodifiableMap(_messageSequenceNumbersByKey);
        }

        void startSendingMessages()
        {
            Runnable messageSender = () -> {
                try
                {
                    LOGGER.info("Starting to send in background thread");
                    Connection producerConnection = getConnection();
                    try
                    {
                        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                        MessageProducer backgroundProducer = producerSession.createProducer(_queue);
                        for (int messageNumber = 0; messageNumber < MSG_COUNT; messageNumber++)
                        {
                            Message message = nextMessage(messageNumber, producerSession, 2);
                            backgroundProducer.send(message);

                            putMessageInMap(message, _messageSequenceNumbersByKey);
                            _quarterOfMessagesSentLatch.countDown();
                        }

                        Message shutdownMessage = producerSession.createMessage();
                        shutdownMessage.setBooleanProperty(SHUTDOWN, true);
                        // make sure the shutdown messages have distinct keys because the Qpid Cpp Broker will
                        // otherwise consider them to have the same key.
                        shutdownMessage.setStringProperty(KEY_PROPERTY, _threadName);

                        backgroundProducer.send(shutdownMessage);

                        // make sure that all in-flight messages reach the Broker
                        // before closing the connection
                        producerSession.createTemporaryQueue().delete();
                    }
                    finally
                    {
                        producerConnection.close();
                    }

                    LOGGER.info("Finished sending in background thread");
                }
                catch (Exception e)
                {
                    _exception = e;
                    LOGGER.warn("Unexpected exception in publisher", e);
                }
            };

            _thread = new Thread(messageSender);
            _thread.setName(_threadName);
            _thread.start();
        }

        void join() throws InterruptedException
        {
            final int timeoutInMillis = 120000;
            _thread.join(timeoutInMillis);
            assertFalse("Expected producer thread to finish within " + timeoutInMillis + "ms", _thread.isAlive());
        }
    }

    private void createConflationQueue(final String queueName,
                                       final String keyProperty, final boolean enforceBrowseOnly) throws Exception
    {
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put(LastValueQueue.LVQ_KEY, keyProperty);
        if (enforceBrowseOnly)
        {
            arguments.put("ensureNondestructiveConsumers", true);
        }
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.LastValueQueue", arguments);
    }

    private Message nextMessage(int msg, Session producerSession) throws JMSException
    {
        return nextMessage(msg, producerSession, 10);
    }

    private Message nextMessage(int msg, Session producerSession, int numberOfUniqueKeyValues) throws JMSException
    {
        Message send = producerSession.createTextMessage("Message: " + msg);

        final String keyValue = String.valueOf(msg % numberOfUniqueKeyValues);
        send.setStringProperty(KEY_PROPERTY, keyValue);
        send.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, msg);

        return send;
    }

    private void sendMessages(final Queue queue, final int fromIndex, final int toIndex)
            throws JMSException, NamingException
    {
        Connection producerConnection = getConnection();
        try
        {
            Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = producerSession.createProducer(queue);

            for (int msg = fromIndex; msg < toIndex; msg++)
            {
                producer.send(nextMessage(msg, producerSession));
                producerSession.commit();
            }

            producer.close();
            producerSession.close();
        }
        finally
        {
            producerConnection.close();
        }
    }
}
