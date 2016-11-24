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
package org.apache.qpid.test.unit.transacted;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.RejectBehaviour;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 * This class tests a number of commits and roll back scenarios
 *
 * Assumptions; - Assumes empty Queue
 *
 * @see org.apache.qpid.test.client.RollbackOrderTest
 */
public class CommitRollbackTest extends QpidBrokerTestCase
{
    private static final Logger _logger = LoggerFactory.getLogger(CommitRollbackTest.class);
    private long _positiveTimeout;
    private long _negativeTimeout;

    private Connection _conn;
    private Session _session;
    private MessageProducer _publisher;
    private Session _pubSession;
    private MessageConsumer _consumer;
    private Queue _jmsQueue;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _positiveTimeout = getReceiveTimeout();
        _negativeTimeout = getShortReceiveTimeout();
    }

    private void newConnection() throws Exception
    {
        _logger.debug("calling newConnection()");
        _conn = getConnection();

        _session = _conn.createSession(true, Session.SESSION_TRANSACTED);

        final String queueName = getTestQueueName();
        _jmsQueue = createTestQueue(_session);
        _session.commit();
        _consumer = _session.createConsumer(_jmsQueue);

        _pubSession = _conn.createSession(true, Session.SESSION_TRANSACTED);

        _publisher = _pubSession.createProducer(_pubSession.createQueue(queueName));

        _conn.start();
    }

    /**
     * PUT a text message, disconnect before commit, confirm it is gone.
     *
     * @throws Exception On error
     */
    public void testPutThenDisconnect() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending test message");
        String MESSAGE_TEXT = "testPutThenDisconnect";
        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _logger.info("reconnecting without commit");
        _conn.close();

        newConnection();

        _logger.info("receiving result");
        Message result = _consumer.receive(_negativeTimeout);

        // commit to ensure message is removed from queue
        _session.commit();

        assertNull("test message was put and disconnected before commit, but is still present", result);
    }


    /**
     * PUT a text message, rollback, confirm message is gone. The consumer is on the same connection but different
     * session as producer
     *
     * @throws Exception On error
     */
    public void testPutThenRollback() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending test message");
        String MESSAGE_TEXT = "testPutThenRollback";
        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _logger.info("rolling back");
        _pubSession.rollback();

        _logger.info("receiving result");
        Message result = _consumer.receive(_negativeTimeout);

        assertNull("test message was put and rolled back, but is still present", result);
    }

    /**
     * GET a text message, disconnect before commit, confirm it is still there. The consumer is on a new connection
     *
     * @throws Exception On error
     */
    public void testGetThenDisconnect() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending test message");
        String MESSAGE_TEXT = "testGetThenDisconnect";
        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _pubSession.commit();

        _logger.info("getting test message");

        Message msg = _consumer.receive(_positiveTimeout);
        assertNotNull("retrieved message is null", msg);

        _logger.info("closing connection");
        _conn.close();

        newConnection();

        _logger.info("receiving result");
        Message result = _consumer.receive(_negativeTimeout);

        _session.commit();

        assertNotNull("test message was consumed and disconnected before commit, but is gone", result);
        assertEquals("test message was correct message", MESSAGE_TEXT, ((TextMessage) result).getText());
    }

    /**
     * GET a text message, close consumer, disconnect before commit, confirm it is still there. The consumer is on the
     * same connection but different session as producer
     *
     * @throws Exception On error
     */
    public void testGetThenCloseDisconnect() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending test message");
        String MESSAGE_TEXT = "testGetThenCloseDisconnect";
        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _pubSession.commit();

        _logger.info("getting test message");

        Message msg = _consumer.receive(_positiveTimeout);
        assertNotNull("retrieved message is null", msg);
        assertEquals("test message was correct message", MESSAGE_TEXT, ((TextMessage) msg).getText());

        _logger.info("reconnecting without commit");
        _consumer.close();
        _conn.close();

        newConnection();

        _logger.info("receiving result");
        Message result = _consumer.receive(_positiveTimeout);

        _session.commit();

        assertNotNull("test message was consumed and disconnected before commit, but is gone", result);
        assertEquals("test message was correct message", MESSAGE_TEXT, ((TextMessage) result).getText());
    }

    /**
     * GET a text message, rollback, confirm it is still there. The consumer is on the same connection but differnt
     * session to the producer
     *
     * @throws Exception On error
     */
    public void testGetThenRollback() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending test message");
        String MESSAGE_TEXT = "testGetThenRollback";
        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _pubSession.commit();

        _logger.info("getting test message");

        Message msg = _consumer.receive(_positiveTimeout);

        assertNotNull("retrieved message is null", msg);
        assertEquals("test message was correct message", MESSAGE_TEXT, ((TextMessage) msg).getText());

        _logger.info("rolling back");

        _session.rollback();

        _logger.info("receiving result");

        Message result = _consumer.receive(_positiveTimeout);

        _session.commit();
        assertNotNull("test message was consumed and rolled back, but is gone", result);
        assertEquals("test message was correct message", MESSAGE_TEXT, ((TextMessage) result).getText());
        assertTrue("Message is not marked as redelivered", result.getJMSRedelivered());
    }

    /**
     * GET a text message, close message producer, rollback, confirm it is still there. The consumer is on the same
     * connection but different session as producer
     *
     * @throws Exception On error
     */
    public void testGetThenCloseRollback() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending test message");
        String MESSAGE_TEXT = "testGetThenCloseRollback";
        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _pubSession.commit();

        _logger.info("getting test message");

        Message msg = _consumer.receive(_positiveTimeout);

        assertNotNull("retrieved message is null", msg);
        assertEquals("test message was correct message", MESSAGE_TEXT, ((TextMessage) msg).getText());

        _logger.info("Closing consumer");
        _consumer.close();

        _logger.info("rolling back");
        _session.rollback();

        _logger.info("receiving result");

        _consumer = _session.createConsumer(_jmsQueue);

        Message result = _consumer.receive(_positiveTimeout);

        _session.commit();
        assertNotNull("test message was consumed and rolled back, but is gone", result);
        assertEquals("test message was correct message", MESSAGE_TEXT, ((TextMessage) result).getText());
        assertTrue("Message is not marked as redelivered", result.getJMSRedelivered());
    }

    /**
     * This test sends two messages receives one of them but doesn't ack it.
     * The consumer is then closed
     * the first message should be returned as redelivered.
     *  the second message should be delivered normally.
     * @throws Exception
     */
    public void testSend2ThenCloseAfter1andTryAgain() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending two test messages");
        _publisher.send(_pubSession.createTextMessage("1"));
        _publisher.send(_pubSession.createTextMessage("2"));
        _pubSession.commit();

        _logger.info("getting test message");
        Message result = _consumer.receive(_positiveTimeout);

        assertNotNull("Message received should not be null", result);
        assertEquals("1", ((TextMessage) result).getText());
        assertTrue("Message is marked as redelivered" + result, !result.getJMSRedelivered());

        _logger.info("Closing Consumer");
        
        _consumer.close();

        _logger.info("Creating New consumer");
        _consumer = _session.createConsumer(_jmsQueue);

        _logger.info("receiving result");


        // Message 2 may be marked as redelivered if it was prefetched.
        result = _consumer.receive(_positiveTimeout);
        assertNotNull("Second message was not consumed, but is gone", result);

        // The first message back will be 2, message 1 has been received but not committed
        // Closing the consumer does not commit the session.

        // if this is message 1 then it should be marked as redelivered
        if("1".equals(((TextMessage) result).getText()))
        {
            fail("First message was received again");
        }

        result = _consumer.receive(_negativeTimeout);
        assertNull("test message should be null:" + result, result);

        _session.commit();
    }

    public void testPutThenRollbackThenGet() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending test message");
        String MESSAGE_TEXT = "testPutThenRollbackThenGet";

        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));
        _pubSession.commit();

        assertNotNull(_consumer.receive(_positiveTimeout));

        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _logger.info("rolling back");
        _pubSession.rollback();

        _logger.info("receiving result");
        Message result = _consumer.receive(_negativeTimeout);
        assertNull("test message was put and rolled back, but is still present", result);

        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _pubSession.commit();

        assertNotNull(_consumer.receive(_positiveTimeout));

        _session.commit();
    }

    /**
     * Qpid-1163
     * Check that when commit is called inside onMessage then
     * the last message is nor redelivered. 
     *
     * @throws Exception
     */
    public void testCommitWithinOnMessage() throws Exception
    {
        newConnection();

        Queue queue = createTestQueue(_session,"example.queue");
        _session.commit();

        // create a consumer
        MessageConsumer cons = _session.createConsumer(queue);
        MessageProducer prod = _session.createProducer(queue);
        Message message =  _session.createTextMessage("Message");
        message.setJMSCorrelationID("m1");
        prod.send(message);
        _session.commit();
        _logger.info("Sent message to queue");
        CountDownLatch cd = new CountDownLatch(1);
        cons.setMessageListener(new CommitWithinOnMessageListener(cd));
        _conn.start();
        cd.await(30, TimeUnit.SECONDS);
        if( cd.getCount() > 0 )
        {
           fail("Did not received message");
        }
        // Check that the message has been dequeued
        _session.close();
        _conn.close();
        _conn = getConnection();
        _conn.start();
        Session session = _conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        cons = session.createConsumer(queue);        
        message = cons.receiveNoWait();
        if(message != null)
        {
            if(message.getJMSCorrelationID().equals("m1"))
            {
                fail("received message twice");
            }
            else
            {
                fail("queue should have been empty, received message: " + message);
            }
        }
    }

    /**
     * This test ensures that after exhausting credit (prefetch), a {@link Session#rollback()} successfully
     * restores credit and allows the same messages to be re-received.
     */
    public void testRollbackSessionAfterCreditExhausted() throws Exception
    {
        final int maxPrefetch= 5;

        // We send more messages than prefetch size.  This ensure that if the 0-10 client were to
        // complete the message commands before the rollback command is sent, the broker would
        // send additional messages utilising the release credit.  This problem would manifest itself
        // as an incorrect message (or no message at all) being received at the end of the test.

        final int numMessages = maxPrefetch * 2;

        setTestClientSystemProperty(ClientProperties.MAX_PREFETCH_PROP_NAME, String.valueOf(maxPrefetch));

        newConnection();

        assertEquals("Prefetch not reset", maxPrefetch, ((AMQSession<?, ?>)_session).getDefaultPrefetch());

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        sendMessage(_pubSession, _publisher.getDestination(), numMessages);
        _pubSession.commit();

        for (int i=0 ;i< maxPrefetch; i++)
        {
            final Message message = _consumer.receive(_positiveTimeout);
            assertNotNull("Received:" + i, message);
            assertEquals("Unexpected message received", i, message.getIntProperty(INDEX));
        }

        _logger.info("Rolling back");
        _session.rollback();

        _logger.info("Receiving messages");

        Message result = _consumer.receive(_positiveTimeout);
        assertNotNull("Message expected", result);
        // Expect the first message
        assertEquals("Unexpected message received", 0, result.getIntProperty(INDEX));
    }

    private class CommitWithinOnMessageListener implements MessageListener
    {
        private CountDownLatch _cd;
        private CommitWithinOnMessageListener(CountDownLatch cd)
        {
            _cd = cd;
        }
        public void onMessage(Message message)
        {
            try
            {
                _logger.info("received message " + message);
                assertEquals("Wrong message received", message.getJMSCorrelationID(), "m1");
                _logger.info("commit session");
                _session.commit();
                _cd.countDown();
            }
            catch (JMSException e)
            {
                _logger.error("OnMessage error",e);
            }
        }
    }

    public void testRollbackSoak() throws Exception
    {
        newConnection();
        final int rollbackTime = 2000;
        final int numberOfMessages = 1000;
        final int numberOfConsumers = 2;
        final long testTimeout = 60*1000L;
        sendMessage(_pubSession, _jmsQueue, numberOfMessages);

        List<ListenableFuture<Void >> consumerFutures = new ArrayList<>(numberOfConsumers);
        final ListeningExecutorService threadPool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numberOfConsumers));

        try
        {
            final CountDownLatch modeFlippedLatch = new CountDownLatch(1);
            final AtomicInteger counter = new AtomicInteger();
            final AtomicInteger rollbackCounter = new AtomicInteger();
            final long flipModeTime = System.currentTimeMillis() + rollbackTime;
            final AtomicBoolean shutdown = new AtomicBoolean();

            for (int i = 0; i < numberOfConsumers; ++i)
            {
                consumerFutures.add(threadPool.submit(new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        Session session = _conn.createSession(true, Session.SESSION_TRANSACTED);
                        final MessageConsumer consumer = session.createConsumer(_jmsQueue);

                        while(!shutdown.get())
                        {
                            Message m = consumer.receive(_positiveTimeout);
                            if (m != null)
                            {
                                long currentTime = System.currentTimeMillis();
                                if (currentTime < flipModeTime)
                                {
                                    session.rollback();
                                    rollbackCounter.incrementAndGet();
                                }
                                else
                                {
                                    modeFlippedLatch.countDown();
                                    counter.incrementAndGet();
                                    session.commit();
                                }
                            }

                            if (counter.get() == numberOfMessages)
                            {
                                break;
                            }

                            if (Thread.currentThread().isInterrupted())
                            {
                                break;
                            }
                        }

                        return null;
                    }
                }));
            }

            final ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(consumerFutures);
            modeFlippedLatch.await(rollbackTime * 2, TimeUnit.MILLISECONDS);
            try
            {
                combinedFuture.get(testTimeout, TimeUnit.MILLISECONDS);
                _logger.debug("Performed {} rollbacks, consumed {}/{} messages",
                              rollbackCounter.get(),
                              counter.get(),
                              numberOfMessages);
            }
            catch (TimeoutException e)
            {
                fail(String.format(
                        "Test took more than %.1f seconds. All consumers probably starved. Performed %d rollbacks, consumed %d/%d messages",
                        testTimeout / 1000.,
                        rollbackCounter.get(),
                        counter.get(),
                        numberOfMessages));
            }
            finally
            {
                shutdown.set(true);
            }
            assertEquals(String.format(
                    "Unexpected number of messages received. Performed %d rollbacks, consumed %d/%d messages",
                    rollbackCounter.get(),
                    counter.get(),
                    numberOfMessages), numberOfMessages, counter.get());
        }
        finally
        {
            threadPool.shutdownNow();
            threadPool.awaitTermination(2 * _positiveTimeout, TimeUnit.SECONDS);
        }
    }

    public void testResendUnseenMessagesAfterRollback() throws Exception
    {
        resendAfterRollback();
    }

    public void testResendUnseenMessagesAfterRollbackWithServerReject() throws Exception
    {
        setTestSystemProperty(ClientProperties.REJECT_BEHAVIOUR_PROP_NAME, RejectBehaviour.SERVER.toString());
        resendAfterRollback();
    }

    private void resendAfterRollback() throws Exception
    {
        newConnection();

        assertTrue("session is not transacted", _session.getTransacted());
        assertTrue("session is not transacted", _pubSession.getTransacted());

        _logger.info("sending test message");
        String MESSAGE_TEXT = "message text";

        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));
        _publisher.send(_pubSession.createTextMessage(MESSAGE_TEXT));

        _pubSession.commit();

        assertNotNull("two messages were sent, but none has been received", _consumer.receive(_positiveTimeout));

        _session.rollback();

        _logger.info("receiving result");

        assertNotNull("two messages were sent, but none has been received", _consumer.receive(_positiveTimeout));
        assertNotNull("two messages were sent, but only one has been received", _consumer.receive(_positiveTimeout));
        assertNull("Only two messages were sent, but more have been received", _consumer.receive(_negativeTimeout));

        _session.commit();
    }
}
