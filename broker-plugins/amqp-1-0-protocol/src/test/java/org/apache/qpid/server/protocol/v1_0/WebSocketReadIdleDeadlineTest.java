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
package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.transport.network.Ticker;

class WebSocketReadIdleDeadlineTest
{
    private static final long READ_IDLE_MILLIS = 1000L;

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"WS", "WSS"})
    void testReadIdleLoggingDoesNotDelayManagementDeadline(final Transport transport) throws Exception
    {
        final Broker<?> broker = BrokerTestHelper.createBrokerMock();
        final Model model = broker.getModel();
        final TaskExecutor taskExecutor = broker.getTaskExecutor();
        final ServerNetworkConnection network = mock();
        when(network.getLocalAddress()).thenReturn(new InetSocketAddress(0));
        final AmqpPort<?> port = mock();
        when(port.getModel()).thenReturn(model);
        when(port.getTaskExecutor()).thenReturn(taskExecutor);
        when(port.getChildExecutor()).thenReturn(taskExecutor);
        when(port.getContext()).thenReturn(Map.of(Connection.CLOSE_RESPONSE_TIMEOUT, "1000"));

        final AggregateTicker ticker = spy(new AggregateTicker());
        final EventLogger eventLogger = mock();

        final class ReadIdleConnection extends AMQPConnection_1_0Impl
        {
            ReadIdleConnection()
            {
                super(broker, network, port, transport, 0L, ticker);
            }

            void startReadIdleTicker()
            {
                initialiseHeartbeating(0L, READ_IDLE_MILLIS);
            }

            @Override
            public EventLogger getEventLogger()
            {
                return eventLogger;
            }
        }

        final ReadIdleConnection connection = new ReadIdleConnection();
        connection.startReadIdleTicker();
        final ArgumentCaptor<Ticker> idleTickerCaptor = ArgumentCaptor.captor();
        verify(ticker).addTicker(idleTickerCaptor.capture());
        final Ticker readIdleTicker = idleTickerCaptor.getValue();

        doAnswer(invocation ->
        {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(network).closeOnReadIdle(any(Runnable.class));

        final CountDownLatch loggingIdleClose = new CountDownLatch(1);
        final CountDownLatch finishLogging = new CountDownLatch(1);
        doAnswer(invocation ->
        {
            loggingIdleClose.countDown();
            assertTrue(finishLogging.await(10L, TimeUnit.SECONDS));
            return null;
        }).when(eventLogger).message(any(LogMessage.class));

        final long idleTime = connection.getLastReadTime() + READ_IDLE_MILLIS;
        final CompletableFuture<Integer> idle = CompletableFuture.supplyAsync(() -> readIdleTicker.tick(idleTime));
        try
        {
            assertTrue(loggingIdleClose.await(5L, TimeUnit.SECONDS));
            CompletableFuture.runAsync(() -> connection.sendConnectionCloseAsync(AMQPConnection.CloseReason.MANAGEMENT,
                    "Test administrative close")).get(5L, TimeUnit.SECONDS);
            verify(network).setCloseDeadline(any(ConnectionClosingTicker.class));
        }
        finally
        {
            finishLogging.countDown();
            idle.get(5L, TimeUnit.SECONDS);
        }

        assertEquals(Integer.MAX_VALUE, readIdleTicker.tick(idleTime));
        verify(eventLogger).message(any(LogMessage.class));
        verify(network).closeOnReadIdle(any(Runnable.class));
    }
}
