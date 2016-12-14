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
package org.apache.qpid.server;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TCPTunneler;

/**
 * Tests the behaviour of the Broker when the client's connection is unexpectedly
 * severed.  Test uses a TCP tunneller which is halted by the test in order to
 * simulate a sudden client failure.
 */
public class AbruptClientDisconnectTest extends QpidBrokerTestCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbruptClientDisconnectTest.class);

    private TCPTunneler _tcpTunneler;
    private Connection _tunneledConnection;
    private ExecutorService _executorService;
    private Queue _testQueue;
    private Connection _utilityConnection;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _executorService = Executors.newFixedThreadPool(3);

        _utilityConnection = getConnection();
        _utilityConnection.start();
        final Session session = _utilityConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // create queue
        _testQueue = createTestQueue(session);

        _tcpTunneler = new TCPTunneler(getFailingPort(), "localhost", getDefaultAmqpPort(), 1);
        _tcpTunneler.start();
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            if (_tunneledConnection != null)
            {
                _tunneledConnection.close();
            }
        }
        finally
        {
            try
            {
                if (_tcpTunneler != null)
                {
                    _tcpTunneler.stop();
                }
            }
            finally
            {
                if (_executorService != null)
                {
                    _executorService.shutdown();
                }
                super.tearDown();
            }
        }

    }

    public void testMessagingOnAbruptConnectivityLostWhilstPublishing() throws Exception
    {
        final ClientMonitor clientMonitor = new ClientMonitor();
        _tunneledConnection = createTunneledConnection(clientMonitor);
        Producer producer =
                new Producer(_tunneledConnection, _testQueue, Session.SESSION_TRANSACTED, 10, new Runnable()
                {
                    @Override
                    public void run()
                    {
                        _tcpTunneler.disconnect(clientMonitor.getClientAddress());
                    }
                }
                );
        _executorService.submit(producer);
        boolean disconnected = clientMonitor.awaitDisconnect(10, TimeUnit.SECONDS);
        producer.stop();
        assertTrue("Client disconnect did not happen", disconnected);
        assertTrue("Unexpected number of published messages " + producer.getNumberOfPublished(),
                   producer.getNumberOfPublished() >= 10);

        consumeIgnoringLastSeenOmission(_utilityConnection, _testQueue, 0, producer.getNumberOfPublished(), -1);
    }


    public void testMessagingOnAbruptConnectivityLostWhilstConsuming() throws Exception
    {
        int minimumNumberOfMessagesToProduce = 40;
        int minimumNumberOfMessagesToConsume = 20;

        // produce minimum required number of messages before starting consumption
        final CountDownLatch queueDataWaiter = new CountDownLatch(1);
        final Producer producer = new Producer(_utilityConnection,
                                               _testQueue,
                                               Session.SESSION_TRANSACTED,
                                               minimumNumberOfMessagesToProduce,
                                               new Runnable()
                                               {
                                                   @Override
                                                   public void run()
                                                   {
                                                       queueDataWaiter.countDown();
                                                   }
                                               });

        // create tunneled connection to consume messages
        final ClientMonitor clientMonitor = new ClientMonitor();
        _tunneledConnection = createTunneledConnection(clientMonitor);
        _tunneledConnection.start();

        // consumer will consume minimum number of messages before abrupt disconnect
        Consumer consumer = new Consumer(_tunneledConnection,
                                         _testQueue,
                                         Session.SESSION_TRANSACTED,
                                         minimumNumberOfMessagesToConsume,
                                         new Runnable()
                                         {
                                             @Override
                                             public void run()
                                             {
                                                 try
                                                 {
                                                     _tcpTunneler.disconnect(clientMonitor.getClientAddress());
                                                 }
                                                 finally
                                                 {
                                                     producer.stop();
                                                 }
                                             }
                                         }
        );

        LOGGER.debug("Waiting for producer to produce {} messages before consuming", minimumNumberOfMessagesToProduce);
        _executorService.submit(producer);

        assertTrue("Latch waiting for produced messages was not count down", queueDataWaiter.await(10, TimeUnit.SECONDS));

        LOGGER.debug("Producer sent {} messages. Starting consumption...", producer.getNumberOfPublished());

        _executorService.submit(consumer);

        boolean disconnectOccurred = clientMonitor.awaitDisconnect(10, TimeUnit.SECONDS);

        LOGGER.debug("Stopping consumer and producer");
        consumer.stop();
        producer.stop();

        LOGGER.debug("Producer sent {} messages. Consumer received {} messages",
                     producer.getNumberOfPublished(),
                     consumer.getNumberOfConsumed());

        assertTrue("Client disconnect did not happen", disconnectOccurred);
        assertTrue("Unexpected number of published messages " + producer.getNumberOfPublished(),
                   producer.getNumberOfPublished() >= minimumNumberOfMessagesToProduce);
        assertTrue("Unexpected number of consumed messages " + consumer.getNumberOfConsumed(),
                   consumer.getNumberOfConsumed() >= minimumNumberOfMessagesToConsume);

        LOGGER.debug("Remaining number to consume {}.",
                     (producer.getNumberOfPublished() - consumer.getNumberOfConsumed()));
        consumeIgnoringLastSeenOmission(_utilityConnection,
                                        _testQueue,
                                        consumer.getNumberOfConsumed(),
                                        producer.getNumberOfPublished(),
                                        consumer.getLastSeenMessageIndex());

    }


    private Connection createTunneledConnection(final ClientMonitor clientMonitor)
            throws Exception
    {
        final int localPort = _tcpTunneler.getLocalPort();
        setSystemProperty("test.port", String.valueOf(localPort));
        setSystemProperty("test.port.alt", String.valueOf(localPort));

        Connection tunneledConnection = getConnectionFactory("default").createConnection(GUEST_USERNAME, GUEST_PASSWORD);
        _tcpTunneler.addClientListener(clientMonitor);
        final AtomicReference<JMSException> _exception = new AtomicReference<>();
        tunneledConnection.setExceptionListener(new ExceptionListener()
        {
            @Override
            public void onException(final JMSException exception)
            {
                _exception.set(exception);
                _tcpTunneler.disconnect(clientMonitor.getClientAddress());
            }
        });
        return tunneledConnection;
    }

    private void consumeIgnoringLastSeenOmission(final Connection connection,
                                                 final Queue testQueue,
                                                 int fromIndex,
                                                 int toIndex,
                                                 int consumerLastSeenMessageIndex)
            throws JMSException
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(testQueue);
        int expectedIndex = fromIndex;
        while (expectedIndex < toIndex)
        {
            Message message = consumer.receive(RECEIVE_TIMEOUT);
            if (message == null && consumerLastSeenMessageIndex + 1 == toIndex)
            {
                // this is a corner case when one remaining message is expected
                // but it was already received previously, Commit was sent
                // and broker successfully committed and sent back CommitOk
                // but CommitOk did not reach client due to abrupt disconnect
                LOGGER.debug( "Broker transaction was completed for message {}"
                                + " but there was no network to notify client about its completion.",
                        consumerLastSeenMessageIndex);
            }
            else
            {
                assertNotNull("Expected message with index " + expectedIndex + " but got null", message);
                int messageIndex = message.getIntProperty(INDEX);
                LOGGER.debug("Received message with index {}, expected index is {}", messageIndex, expectedIndex);
                if (messageIndex != expectedIndex
                        && expectedIndex == fromIndex
                        && messageIndex == consumerLastSeenMessageIndex + 1)
                {
                    LOGGER.debug("Broker transaction was completed for message {}"
                                    + " but there was no network to notify client about its completion.",
                            consumerLastSeenMessageIndex);
                    expectedIndex = messageIndex;
                }
                assertEquals("Unexpected message index", expectedIndex, messageIndex);
            }
            expectedIndex++;
        }
        session.close();
    }

    private class ClientMonitor implements TCPTunneler.TunnelListener
    {
        private final CountDownLatch _closeLatch = new CountDownLatch(1);
        private final AtomicReference<InetSocketAddress> _clientAddress = new AtomicReference<>();

        @Override
        public void clientConnected(final InetSocketAddress clientAddress)
        {
            _clientAddress.set(clientAddress);
        }

        @Override
        public void clientDisconnected(final InetSocketAddress clientAddress)
        {
            if (clientAddress.equals(getClientAddress()))
            {
                _closeLatch.countDown();
            }
        }

        public boolean awaitDisconnect(int period, TimeUnit timeUnit) throws InterruptedException
        {
            return _closeLatch.await(period, timeUnit);
        }

        public InetSocketAddress getClientAddress()
        {
            return _clientAddress.get();
        }
    }

    private class Producer implements Runnable
    {
        private final Runnable _runnable;
        private final Session _session;
        private final MessageProducer _messageProducer;
        private final int _numberOfMessagesToInvokeRunnableAfter;
        private volatile int _publishedMessageCounter;
        private volatile Exception _exception;
        private volatile Thread _thread;
        private AtomicBoolean _closed = new AtomicBoolean();

        public Producer(Connection connection, Destination queue, int acknowledgeMode,
                        int numberOfMessagesToInvokeRunnableAfter, Runnable runnableToInvoke)
                throws JMSException
        {
            _session = connection.createSession(acknowledgeMode == Session.SESSION_TRANSACTED, acknowledgeMode);
            _messageProducer = _session.createProducer(queue);
            _runnable = runnableToInvoke;
            _numberOfMessagesToInvokeRunnableAfter = numberOfMessagesToInvokeRunnableAfter;
        }

        @Override
        public void run()
        {
            _thread = Thread.currentThread();
            try
            {
                Message message = _session.createMessage();
                while (!_closed.get())
                {
                    if (_publishedMessageCounter == _numberOfMessagesToInvokeRunnableAfter && _runnable != null)
                    {
                        _executorService.execute(_runnable);
                    }

                    message.setIntProperty(INDEX, _publishedMessageCounter);
                    _messageProducer.send(message);
                    if (_session.getTransacted())
                    {
                        _session.commit();
                    }
                    LOGGER.debug("Produced message with index {}", _publishedMessageCounter);
                    _publishedMessageCounter++;
                }
                LOGGER.debug("Stopping producer gracefully");
            }
            catch (Exception e)
            {
                LOGGER.debug("Stopping producer due to exception", e);
                _exception = e;
            }
        }

        public void stop()
        {
            if (_closed.compareAndSet(false, true))
            {
                if (_thread != null)
                {
                    try
                    {
                        _thread.join(2000);
                    }
                    catch (InterruptedException e)
                    {
                        _thread.interrupt();
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        public int getNumberOfPublished()
        {
            return _publishedMessageCounter;
        }

        public Exception getException()
        {
            return _exception;
        }

    }

    private class Consumer implements Runnable
    {
        private final Runnable _runnable;
        private final Session _session;
        private final MessageConsumer _messageConsumer;
        private final int _numberOfMessagesToInvokeRunnableAfter;
        private volatile int _consumedMessageCounter;
        private volatile Exception _exception;
        private volatile Thread _thread;
        private AtomicBoolean _closed = new AtomicBoolean();
        private volatile int _lastSeenMessageIndex;

        public Consumer(Connection connection,
                        Destination queue,
                        int acknowledgeMode,
                        int numberOfMessagesToInvokeRunnableAfter,
                        Runnable runnableToInvoke)
                throws JMSException
        {
            _session = connection.createSession(acknowledgeMode == Session.SESSION_TRANSACTED, acknowledgeMode);
            _messageConsumer = _session.createConsumer(queue);
            _runnable = runnableToInvoke;
            _numberOfMessagesToInvokeRunnableAfter = numberOfMessagesToInvokeRunnableAfter;
        }

        @Override
        public void run()
        {
            _thread = Thread.currentThread();
            try
            {
                while (!_closed.get())
                {
                    if (_consumedMessageCounter == _numberOfMessagesToInvokeRunnableAfter && _runnable != null)
                    {
                        _executorService.execute(_runnable);
                    }

                    Message message = _messageConsumer.receive(RECEIVE_TIMEOUT);
                    if (message != null)
                    {
                        int messageIndex = message.getIntProperty(INDEX);
                        _lastSeenMessageIndex = messageIndex;
                        LOGGER.debug("Received message with index {}, expected index {}",
                                     messageIndex,
                                     _consumedMessageCounter);
                        assertEquals("Unexpected message index",
                                     _consumedMessageCounter,
                                     messageIndex);

                        if (_session.getTransacted())
                        {
                            _session.commit();
                            LOGGER.debug("Committed message with index {}", messageIndex);
                        }
                        _consumedMessageCounter++;
                    }
                }
                LOGGER.debug("Stopping consumer gracefully");
            }
            catch (Exception e)
            {
                LOGGER.debug("Stopping consumer due to exception, number of consumed {}", _consumedMessageCounter, e);
                _exception = e;
            }
        }

        public void stop()
        {
            if (_closed.compareAndSet(false, true))
            {
                if (_thread != null)
                {
                    try
                    {
                        _thread.join(2000);
                    }
                    catch (InterruptedException e)
                    {
                        _thread.interrupt();
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        public int getNumberOfConsumed()
        {
            return _consumedMessageCounter;
        }

        public Exception getException()
        {
            return _exception;
        }

        public int getLastSeenMessageIndex()
        {
            return _lastSeenMessageIndex;
        }

    }
}
