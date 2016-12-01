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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import com.google.common.util.concurrent.SettableFuture;

import org.apache.qpid.AMQConnectionClosedException;
import org.apache.qpid.AMQConnectionFailureException;
import org.apache.qpid.AMQDisconnectedException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.BasicMessageConsumer;
import org.apache.qpid.client.BasicMessageProducer;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.transport.ConnectionException;

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
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
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

        stopDefaultBroker();

        JMSException exception = _recordingExceptionListener.awaitException(10000);
        assertConnectionCloseWasReported(exception, expectedLinkedException);
        assertJmsObjectsClosed();

        ensureCanCloseWithoutException();
    }

    public void testClientCloseOnVirtualHostStop() throws Exception
    {
        final String virtualHostName = TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST;
        RestTestHelper restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());

        final CountDownLatch connectionCreatorStarted = new CountDownLatch(1);
        final AtomicBoolean shutdown = new AtomicBoolean(false);
        final SettableFuture<Exception> clientException = SettableFuture.create();
        Thread connectionCreator = new Thread(new Runnable(){

            @Override
            public void run()
            {
                while (!shutdown.get())
                {
                    try
                    {
                        getConnection();
                    }
                    catch (Exception e)
                    {
                        clientException.set(e);
                        shutdown.set(true);
                    }
                    connectionCreatorStarted.countDown();
                }
            }
        }, getTestName() + "_ConnectionCreatingThread");

        try
        {
            connectionCreator.start();
            assertTrue("connection creation thread did not start in time", connectionCreatorStarted.await(20, TimeUnit.SECONDS));

            String restHostUrl = "virtualhost/" + virtualHostName + "/" + virtualHostName;
            restTestHelper.submitRequest(restHostUrl, "PUT", Collections.singletonMap("desiredState", (Object) "STOPPED"), 200);
            restTestHelper.waitForAttributeChanged(restHostUrl, VirtualHost.STATE, "STOPPED");

            int connectionCount = 0;
            for (int i = 0; i < 20; ++i)
            {
                Map<String, Object> portObject = restTestHelper.getJsonAsSingletonList("port/" + TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT);
                Map<String, Object> portStatistics = (Map<String, Object>) portObject.get("statistics");
                connectionCount = (int) portStatistics.get("connectionCount");
                if (connectionCount == 0)
                {
                    break;
                }
                Thread.sleep(250);
            }
            assertEquals("unexpected number of connections after virtual host stopped", 0, connectionCount);

            assertConnectionCloseWasReported(clientException.get(20, TimeUnit.SECONDS), AMQConnectionFailureException.class);
        }
        finally
        {
            shutdown.set(true);
            connectionCreator.join(10000);
        }
    }

    public void testClientCloseOnBrokerKill() throws Exception
    {
        final Class<? extends Exception> expectedLinkedException = isBroker010() ? ConnectionException.class : AMQDisconnectedException.class;

        if (!_isExternalBroker)
        {
            return;
        }

        assertJmsObjectsOpen();

        killDefaultBroker();

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

    private void assertConnectionCloseWasReported(Exception exception, Class<? extends Exception> linkedExceptionClass)
    {
        assertNotNull("Did not receive exception", exception);
        if(!isBroker10())
        {
            assertNotNull("Exception should have a cause", exception.getCause());
            assertEquals("Unexpected exception cause", linkedExceptionClass, exception.getCause().getClass());
        }
    }

    private void assertJmsObjectsClosed()
    {
        assertTrue("Connection should be marked as closed", connectionIsClosed());
        assertTrue("Session should be marked as closed", sessionIsClosed());
        assertTrue("Producer should be marked as closed", producerIsClosed());
        assertTrue("Consumer should be marked as closed", consumerIsClosed());
    }

    private boolean producerIsClosed()
    {
        if(isBroker10())
        {
            try
            {
                _producer.setPriority(7);
                return false;
            }
            catch (JMSException e)
            {
                return true;
            }
        }
        else
        {
            return ((BasicMessageProducer) _producer).isClosed();
        }

    }

    private boolean sessionIsClosed()
    {
        if(isBroker10())
        {
            try
            {
                _session.createProducer(null);
                return false;
            }
            catch (JMSException e)
            {
                return true;
            }
        }
        else
        {
            return ((AMQSession) _session).isClosed();
        }
    }

    private boolean connectionIsClosed()
    {
        if(isBroker10())
        {
            try
            {
                _connection.stop();
                return false;
            }
            catch (JMSException e)
            {
                return true;
            }
        }
        else
        {
            return ((AMQConnection) _connection).isClosed();
        }
    }

    private void assertJmsObjectsOpen()
    {
        assertFalse("Connection should not be marked as closed", connectionIsClosed());
        assertFalse("Session should not be marked as closed", sessionIsClosed());
        assertFalse("Producer should not be marked as closed", producerIsClosed());
        assertFalse("Consumer should not be marked as closed", consumerIsClosed());
    }

    private boolean consumerIsClosed()
    {
        if(isBroker10())
        {
            try
            {
                _consumer.getMessageSelector();
                return false;
            }
            catch (JMSException e)
            {
                return true;
            }
        }
        else
        {
            return ((BasicMessageConsumer) _consumer).isClosed();
        }
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
        final Queue testQueue = createTestQueue(session);
        MessageConsumer consumer1 = session.createConsumer(testQueue);
        consumer1.setMessageListener(listener);

        MessageProducer producer = _session.createProducer(testQueue);
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
        MessageConsumer consumer2 = session2.createConsumer(testQueue);
        consumer2.setMessageListener(listener);
        connection2.start();


        Connection connection3 = getConnection();
        Session session3 = connection3.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createConsumer(testQueue);
        consumer3.setMessageListener(listener);
        connection3.start();

        assertEquals(1, listener.getCount());

        stopDefaultBroker();

        assertEquals(1, listener.getCount());


    }
}
