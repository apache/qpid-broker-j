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
package org.apache.qpid.test.unit.client.connection;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import org.apache.qpid.AMQConnectionClosedException;
import org.apache.qpid.AMQDisconnectedException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.BasicMessageConsumer;
import org.apache.qpid.client.BasicMessageProducer;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.transport.ConnectionException;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * Tests the behaviour of the client when the Broker terminates client connection
 * by the Broker being shutdown gracefully or otherwise.
 */
public class BrokerClosesClientConnectionTest extends QpidBrokerTestCase
{
    private Connection _connection;
    private boolean _isExternalBroker;
    private final RecordingExceptionListener _recordingExceptionListener = new RecordingExceptionListener();
    private Session _session;
    private MessageConsumer _consumer;
    private MessageProducer _producer;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();

        _connection = getConnection();
        _connection.start();
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _connection.setExceptionListener(_recordingExceptionListener);
        Queue queue = _session.createTemporaryQueue();
        _consumer = _session.createConsumer(queue);
        _producer = _session.createProducer(queue);

        _isExternalBroker = isExternalBroker();
    }

    public void testClientCloseOnNormalBrokerShutdown() throws Exception
    {
        final Class<? extends Exception> expectedLinkedException = isBroker010() ? ConnectionException.class : AMQConnectionClosedException.class;

        assertJmsObjectsOpen();

        stopBroker();

        JMSException exception = _recordingExceptionListener.awaitException(10000);
        assertConnectionCloseWasReported(exception, expectedLinkedException);
        assertJmsObjectsClosed();

        ensureCanCloseWithoutException();
    }

    public void testClientCloseOnBrokerKill() throws Exception
    {
        final Class<? extends Exception> expectedLinkedException = isBroker010() ? ConnectionException.class : AMQDisconnectedException.class;

        if (!_isExternalBroker)
        {
            return;
        }

        assertJmsObjectsOpen();

        killBroker();

        JMSException exception = _recordingExceptionListener.awaitException(10000);
        assertConnectionCloseWasReported(exception, expectedLinkedException);
        assertJmsObjectsClosed();

        ensureCanCloseWithoutException();
    }

    private void ensureCanCloseWithoutException()
    {
        try
        {
            _connection.close();
        }
        catch (JMSException e)
        {
            fail("Connection should close without exception" + e.getMessage());
        }
    }

    private void assertConnectionCloseWasReported(JMSException exception, Class<? extends Exception> linkedExceptionClass)
    {
        assertNotNull("Broker shutdown should be reported to the client via the ExceptionListener", exception);
        assertNotNull("JMXException should have linked exception", exception.getLinkedException());

        assertEquals("Unexpected linked exception", linkedExceptionClass, exception.getLinkedException().getClass());
    }

    private void assertJmsObjectsClosed()
    {
        assertTrue("Connection should be marked as closed", ((AMQConnection) _connection).isClosed());
        assertTrue("Session should be marked as closed", ((AMQSession) _session).isClosed());
        assertTrue("Producer should be marked as closed", ((BasicMessageProducer) _producer).isClosed());
        assertTrue("Consumer should be marked as closed", ((BasicMessageConsumer) _consumer).isClosed());
    }

    private void assertJmsObjectsOpen()
    {
        assertFalse("Connection should not be marked as closed", ((AMQConnection) _connection).isClosed());
        assertFalse("Session should not be marked as closed", ((AMQSession) _session).isClosed());
        assertFalse("Producer should not be marked as closed", ((BasicMessageProducer) _producer).isClosed());
        assertFalse("Consumer should not be marked as closed", ((BasicMessageConsumer) _consumer).isClosed());
    }

    private final class RecordingExceptionListener implements ExceptionListener
    {
        private final CountDownLatch _exceptionReceivedLatch = new CountDownLatch(1);
        private volatile JMSException _exception;

        @Override
        public void onException(JMSException exception)
        {
            _exception = exception;
        }

        public JMSException awaitException(long timeoutInMillis) throws InterruptedException
        {
            _exceptionReceivedLatch.await(timeoutInMillis, TimeUnit.MILLISECONDS);
            return _exception;
        }
    }


    private class Listener implements MessageListener
    {
        int _messageCount;

        @Override
        public synchronized void onMessage(Message message)
        {
            _messageCount++;
        }

        public synchronized int getCount()
        {
            return _messageCount;
        }
    }

    public void testNoDeliveryAfterBrokerClose() throws Exception
    {

        Listener listener = new Listener();

        Session session = _connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer1 = session.createConsumer(getTestQueue());
        consumer1.setMessageListener(listener);

        MessageProducer producer = _session.createProducer(getTestQueue());
        producer.send(_session.createTextMessage("test message"));

        _connection.start();


        synchronized (listener)
        {
            long currentTime = System.currentTimeMillis();
            long until = currentTime + 2000l;
            while(listener.getCount() == 0 && currentTime < until)
            {
                listener.wait(until - currentTime);
                currentTime = System.currentTimeMillis();
            }
        }
        assertEquals(1, listener.getCount());

        Connection connection2 = getConnection();
        Session session2 = connection2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(getTestQueue());
        consumer2.setMessageListener(listener);
        connection2.start();


        Connection connection3 = getConnection();
        Session session3 = connection3.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createConsumer(getTestQueue());
        consumer3.setMessageListener(listener);
        connection3.start();

        assertEquals(1, listener.getCount());

        stopBroker();

        assertEquals(1, listener.getCount());


    }
}
