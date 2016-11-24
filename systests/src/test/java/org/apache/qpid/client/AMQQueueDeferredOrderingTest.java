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
package org.apache.qpid.client;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class AMQQueueDeferredOrderingTest extends QpidBrokerTestCase
{
    private static final Logger _logger = LoggerFactory.getLogger(AMQQueueDeferredOrderingTest.class);
    private Connection con;
    private Session session;
    private Queue queue;
    private MessageConsumer consumer;
    private int _numMessages;
    private ExecutorService _executor;
    private volatile boolean _shutdownThreads;

    private class ASyncProducer implements Callable<Exception>
    {
        private MessageProducer producer;
        private final Logger _logger = LoggerFactory.getLogger(ASyncProducer.class);
        private Session session;
        private int start;
        private int end;

        public ASyncProducer(Queue q, int start, int end) throws JMSException
        {
            this.session = con.createSession(true, Session.SESSION_TRANSACTED);
            this._logger.info("Create Consumer of Q1");
            this.producer = this.session.createProducer(q);
            this.start = start;
            this.end = end;
        }

        public Exception call()
        {
            try
            {
                this._logger.info("Starting to send messages");
                for (int i = start; i < end && !_shutdownThreads; i++)
                {
                    producer.send(session.createTextMessage(Integer.toString(i)));
                    session.commit();
                }
                this._logger.info("Sent " + (end - start) + " messages");
            }
            catch (Exception e)
            {
                return e;
            }
            return null;
        }
    }

    protected void setUp() throws Exception
    {
        super.setUp();

        _numMessages = isBrokerStorePersistent() ? 300 : 1000;

        _logger.info("Create Connection");
        con = getConnection();
        _logger.info("Create Session");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _logger.info("Create Q");
        queue = createTestQueue(session);
        _logger.info("Create Consumer of Q");
        consumer = session.createConsumer(queue);
        _logger.info("Start Connection");
        con.start();
        _executor = Executors.newSingleThreadExecutor();
    }

    public void testMessagesSentByTwoThreadsAreDeliveredInOrder() throws Exception
    {
        Future<Exception> f;
        Exception publisherException;

        // Setup initial messages
        _logger.info("Creating first producer thread");
        f = _executor.submit(new ASyncProducer(queue, 0, _numMessages / 2));
        publisherException = f.get(1, TimeUnit.MINUTES);
        assertNull("Publishing first batch failed: " + publisherException, publisherException);

        // Setup second set of messages to produce while we consume
        _logger.info("Creating second producer thread");
        f = _executor.submit(new ASyncProducer(queue, _numMessages / 2, _numMessages));

        // Start consuming and checking they're in order
        _logger.info("Consuming messages");
        for (int i = 0; i < _numMessages; i++)
        {
            Message msg = consumer.receive(3000);

            assertNotNull("Message " + i + " should not be null", msg);
            assertTrue("Message " + i + " should be a text message", msg instanceof TextMessage);
            assertEquals("Message content " + i + " does not match expected", Integer.toString(i), ((TextMessage) msg).getText());
        }
        publisherException = f.get(10, TimeUnit.SECONDS);
        assertNull("Publishing second batch failed: " + publisherException, publisherException);
    }

    protected void tearDown() throws Exception
    {
        try
        {
            _logger.info("Interrupting producer thread");
            _shutdownThreads = true;
            _executor.shutdown();
            assertTrue("Executor service failed to shutdown", _executor.awaitTermination(1, TimeUnit.MINUTES));
        }
        finally
        {
            try
            {
                _logger.info("Closing connection");
                con.close();
            }
            finally
            {
                super.tearDown();
            }
        }
    }

    public static junit.framework.Test suite()
    {
        return new junit.framework.TestSuite(AMQQueueDeferredOrderingTest.class);
    }

}
