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

package org.apache.qpid.server.queue;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class LastValueQueueTest extends QpidBrokerTestCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LastValueQueueTest.class);

    private static final String MESSAGE_SEQUENCE_NUMBER_PROPERTY = "msg";
    private static final String KEY_PROPERTY = "key";

    private static final int MSG_COUNT = 400;

    private String _queueName;
    private Queue _queue;
    private Connection _producerConnection;
    private MessageProducer _producer;
    private Session _producerSession;
    private Connection _consumerConnection;
    private Session _consumerSession;
    private MessageConsumer _consumer;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _queueName = getTestQueueName();
        _producerConnection = getConnection();
        _producerSession = _producerConnection.createSession(true, Session.SESSION_TRANSACTED);
    }

    public void testConflation() throws Exception
    {
        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        createConflationQueue(_producerSession, false);
        _producer = _producerSession.createProducer(_queue);

        for (int msg = 0; msg < MSG_COUNT; msg++)
        {
            _producer.send(nextMessage(msg, _producerSession));
            _producerSession.commit();
        }

        _producer.close();
        _producerSession.close();
        _producerConnection.close();

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();
        Message received;

        List<Message> messages = new ArrayList<>();
        while((received = _consumer.receive(getReceiveTimeout())) != null)
        {
            messages.add(received);
        }

        assertEquals("Unexpected number of messages received",10,messages.size());

        for(int i = 0 ; i < 10; i++)
        {
            Message msg = messages.get(i);
            assertEquals("Unexpected message number received", MSG_COUNT - 10 + i, msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }
    }

    public void testConflationWithRelease() throws Exception
    {
        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);


        createConflationQueue(_producerSession, false);
        _producer = _producerSession.createProducer(_queue);

        for (int msg = 0; msg < MSG_COUNT/2; msg++)
        {
            _producer.send(nextMessage(msg, _producerSession));
            _producerSession.commit();
        }

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();
        Message received;
        List<Message> messages = new ArrayList<>();
        while((received = _consumer.receive(getReceiveTimeout())) != null)
        {
            messages.add(received);
        }

        assertEquals("Unexpected number of messages received",10,messages.size());

        for(int i = 0 ; i < 10; i++)
        {
            Message msg = messages.get(i);
            assertEquals("Unexpected message number received", MSG_COUNT/2 - 10 + i, msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }

        _consumerSession.close();
        _consumerConnection.close();


        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);


        for (int msg = MSG_COUNT/2; msg < MSG_COUNT; msg++)
        {
            _producer.send(nextMessage(msg, _producerSession));
            _producerSession.commit();
        }

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();

        messages = new ArrayList<>();
        while((received = _consumer.receive(getReceiveTimeout())) != null)
        {
            messages.add(received);
        }

        assertEquals("Unexpected number of messages received",10,messages.size());

        for(int i = 0 ; i < 10; i++)
        {
            Message msg = messages.get(i);
            assertEquals("Unexpected message number received", MSG_COUNT - 10 + i, msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }

    }


    public void testConflationWithReleaseAfterNewPublish() throws Exception
    {
        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);


        createConflationQueue(_producerSession, false);
        _producer = _producerSession.createProducer(_queue);

        for (int msg = 0; msg < MSG_COUNT/2; msg++)
        {
            _producer.send(nextMessage(msg, _producerSession));
            _producerSession.commit();
        }

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();
        Message received;
        List<Message> messages = new ArrayList<>();
        while((received = _consumer.receive(getReceiveTimeout())) != null)
        {
            messages.add(received);
        }

        assertEquals("Unexpected number of messages received",10,messages.size());

        for(int i = 0 ; i < 10; i++)
        {
            Message msg = messages.get(i);
            assertEquals("Unexpected message number received", MSG_COUNT/2 - 10 + i, msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }

        _consumer.close();

        for (int msg = MSG_COUNT/2; msg < MSG_COUNT; msg++)
        {
            _producer.send(nextMessage(msg, _producerSession));
        }
        _producerSession.commit();

        // this causes the "old" messages to be released
        _consumerSession.close();
        _consumerConnection.close();


        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);



        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();

        messages = new ArrayList<>();
        while((received = _consumer.receive(getReceiveTimeout())) != null)
        {
            messages.add(received);
        }

        assertEquals("Unexpected number of messages received",10,messages.size());

        for(int i = 0 ; i < 10; i++)
        {
            Message msg = messages.get(i);
            assertEquals("Unexpected message number received", MSG_COUNT - 10 + i, msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }

    }

    public void testConflatedQueueDepth() throws Exception
    {
        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        createConflationQueue(_producerSession, false);
        _producer = _producerSession.createProducer(_queue);

        for (int msg = 0; msg < MSG_COUNT; msg++)
        {
            _producer.send(nextMessage(msg, _producerSession));
            _producerSession.commit();
        }
        _producerConnection.start();
        final long queueDepth = getQueueDepth(_producerConnection,_queue);

        assertEquals(10, queueDepth);
    }

    public void testConflationBrowser() throws Exception
    {
        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);


        createConflationQueue(_producerSession, true);
        _producer = _producerSession.createProducer(_queue);

        for (int msg = 0; msg < MSG_COUNT; msg++)
        {
            _producer.send(nextMessage(msg, _producerSession));
            _producerSession.commit();
        }

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();
        Message received;
        List<Message> messages = new ArrayList<>();
        while((received = _consumer.receive(getReceiveTimeout())) != null)
        {
            messages.add(received);
        }

        assertEquals("Unexpected number of messages received",10,messages.size());

        for(int i = 0 ; i < 10; i++)
        {
            Message msg = messages.get(i);
            assertEquals("Unexpected message number received", MSG_COUNT - 10 + i, msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }

        messages.clear();

        _producer.send(nextMessage(MSG_COUNT, _producerSession));
        _producerSession.commit();

        while((received = _consumer.receive(getReceiveTimeout())) != null)
        {
            messages.add(received);
        }
        assertEquals("Unexpected number of messages received",1,messages.size());
        assertEquals("Unexpected message number received", MSG_COUNT, messages.get(0).getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));


        _producer.close();
        _producerSession.close();
        _producerConnection.close();
    }

    public void testConflation2Browsers() throws Exception
    {
        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        createConflationQueue(_producerSession, true);
        _producer = _producerSession.createProducer(_queue);

        for (int msg = 0; msg < MSG_COUNT; msg++)
        {
            _producer.send(nextMessage(msg, _producerSession));
            _producerSession.commit();
        }

        _consumer = _consumerSession.createConsumer(_queue);
        MessageConsumer consumer2 = _consumerSession.createConsumer(_queue);

        _consumerConnection.start();
        List<Message> messages = new ArrayList<>();
        List<Message> messages2 = new ArrayList<>();
        Message received  = _consumer.receive(getReceiveTimeout());
        Message received2  = consumer2.receive(getReceiveTimeout());

        while(received!=null || received2!=null)
        {
            if(received != null)
            {
                messages.add(received);
            }
            if(received2 != null)
            {
                messages2.add(received2);
            }


            received  = _consumer.receive(getReceiveTimeout());
            received2  = consumer2.receive(getReceiveTimeout());

        }

        assertEquals("Unexpected number of messages received on first browser",10,messages.size());
        assertEquals("Unexpected number of messages received on second browser",10,messages2.size());

        for(int i = 0 ; i < 10; i++)
        {
            Message msg = messages.get(i);
            assertEquals("Unexpected message number received on first browser", MSG_COUNT - 10 + i, msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            msg = messages2.get(i);
            assertEquals("Unexpected message number received on second browser", MSG_COUNT - 10 + i, msg.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }


        _producer.close();
        _producerSession.close();
        _producerConnection.close();
    }

    public void testParallelProductionAndConsumption() throws Exception
    {
        createConflationQueue(_producerSession, false);

        // Start producing threads that send messages
        BackgroundMessageProducer messageProducer1 = new BackgroundMessageProducer("Message sender1");
        messageProducer1.startSendingMessages();
        BackgroundMessageProducer messageProducer2 = new BackgroundMessageProducer("Message sender2");
        messageProducer2.startSendingMessages();

        Map<String, Integer> lastReceivedMessages = receiveMessages(messageProducer1);

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

    private Map<String, Integer> receiveMessages(BackgroundMessageProducer producer) throws Exception
    {
        producer.waitUntilQuarterOfMessagesSentToEncourageConflation();

        _consumerConnection = getConnectionWithPrefetch(1);

        _consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        LOGGER.info("Starting to receive");

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();

        Map<String, Integer> messageSequenceNumbersByKey = new HashMap<>();

        Message message;
        int numberOfShutdownsReceived = 0;
        int numberOfMessagesReceived = 0;
        while(numberOfShutdownsReceived < 2)
        {
            message = _consumer.receive(getReceiveTimeout());
            assertNotNull("null received after " + numberOfMessagesReceived + " messages and " + numberOfShutdownsReceived + " shutdowns", message);

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

        private volatile Exception _exception;

        private Thread _thread;
        private Map<String, Integer> _messageSequenceNumbersByKey = new HashMap<>();
        private CountDownLatch _quarterOfMessagesSentLatch = new CountDownLatch(MSG_COUNT/4);

        public BackgroundMessageProducer(String threadName)
        {
            _threadName = threadName;
        }

        public void waitUntilQuarterOfMessagesSentToEncourageConflation() throws InterruptedException
        {
            final long latchTimeout = 60000;
            boolean success = _quarterOfMessagesSentLatch.await(latchTimeout, TimeUnit.MILLISECONDS);
            assertTrue("Failed to be notified that 1/4 of the messages have been sent within " + latchTimeout + " ms.", success);
            LOGGER.info("Quarter of messages sent");
        }

        public Exception getException()
        {
            return _exception;
        }

        public Map<String, Integer> getMessageSequenceNumbersByKey()
        {
            return Collections.unmodifiableMap(_messageSequenceNumbersByKey);
        }

        public void startSendingMessages()
        {
            Runnable messageSender = new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        LOGGER.info("Starting to send in background thread");
                        Connection producerConnection = getConnection();
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

                        LOGGER.info("Finished sending in background thread");
                    }
                    catch (Exception e)
                    {
                        _exception = e;
                        throw new RuntimeException(e);
                    }
                }
            };

            _thread = new Thread(messageSender);
            _thread.setName(_threadName);
            _thread.start();
        }

        public void join() throws InterruptedException
        {
            final int timeoutInMillis = 120000;
            _thread.join(timeoutInMillis);
            assertFalse("Expected producer thread to finish within " + timeoutInMillis + "ms", _thread.isAlive());
        }
    }

    private void createConflationQueue(Session session, final boolean enforceBrowseOnly) throws QpidException, JMSException
    {
        if(isBroker10())
        {
            final Map<String, Object> arguments = new HashMap<>();
            arguments.put(LastValueQueue.LVQ_KEY, KEY_PROPERTY);
            if(enforceBrowseOnly)
            {
                arguments.put("ensureNondestructiveConsumers", true);
            }
            createEntityUsingAmqpManagement(_queueName, session, "org.apache.qpid.LastValueQueue", arguments);
            _queue = session.createQueue(_queueName);
        }
        else
        {
            String browserOnly = enforceBrowseOnly ? "mode: browse," : "";
            String addr = String.format("ADDR:%s; {create: always, %s" +
                                        "node: {x-declare:{arguments : {'qpid.last_value_queue_key':'%s'}}}}",
                                        _queueName, browserOnly, KEY_PROPERTY);

            _queue = session.createQueue(addr);
            session.createConsumer(_queue).close();
        }
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
}
