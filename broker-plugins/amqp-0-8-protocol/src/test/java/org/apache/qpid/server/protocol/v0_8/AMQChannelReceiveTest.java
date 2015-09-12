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
package org.apache.qpid.server.protocol.v0_8;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.exchange.ExchangeImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.virtualhost.VirtualHostImpl;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.AggregateTicker;
import org.apache.qpid.transport.network.NetworkConnection;

public class AMQChannelReceiveTest extends QpidTestCase
{
    private VirtualHostImpl<?, AMQQueue<?>, ExchangeImpl<?>> _virtualHost;
    private AMQPConnection_0_8 _protocolSession;
    private MessageStore _messageStore;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();

        _virtualHost = mock(VirtualHostImpl.class);
        when(_virtualHost.getContextValue(Integer.class, Broker.MESSAGE_COMPRESSION_THRESHOLD_SIZE)).thenReturn(1);
        when(_virtualHost.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(1l);
        when(_virtualHost.getPrincipal()).thenReturn(mock(Principal.class));
        when(_virtualHost.getEventLogger()).thenReturn(mock(EventLogger.class));

        Broker<?> broker = mock(Broker.class);
        when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(broker.getContextValue(Long.class, Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)).thenReturn(1l);

        NetworkConnection network = mock(NetworkConnection.class);
        ByteBufferSender sender = mock(ByteBufferSender.class);
        when(network.getSender()).thenReturn(sender);
        AmqpPort<?> port = mock(AmqpPort.class);
        TaskExecutor taskExecutor = mock(TaskExecutor.class);
        when(taskExecutor.getExecutor()).thenReturn(mock(Executor.class));
        when(port.getChildExecutor()).thenReturn(taskExecutor);
        when(port.getModel()).thenReturn(BrokerModel.getInstance());
        when(port.getContextValue(Integer.class, AmqpPort.PORT_MAX_MESSAGE_SIZE)).thenReturn(1);

        _protocolSession = new AMQPConnection_0_8(broker, network, port, Transport.TCP, Protocol.AMQP_0_9, 1, new AggregateTicker());
        _protocolSession.setVirtualHost(_virtualHost);

        _messageStore = mock(MessageStore.class);
    }

    public void testReceiveExchangeDeleteWhenIfUsedIsSetAndExchangeHasBindings() throws Exception
    {
        ExchangeImpl<?> exchange = mock(ExchangeImpl.class);
        when(exchange.hasBindings()).thenReturn(true);
        doReturn(exchange).when(_virtualHost).getAttainedExchange(getTestName());

        final AtomicReference<AMQConstant> closeCause = new AtomicReference<>();
        final AtomicReference<String> closeMessage = new AtomicReference<>();
        AMQChannel channel = new AMQChannel(_protocolSession, 1, _messageStore)
        {
            @Override
            public void close(AMQConstant cause, String message)
            {
                super.close(cause, message);
                closeCause.set(cause);
                closeMessage.set(message);
            }
        };
        channel.receiveExchangeDelete(AMQShortString.valueOf(getTestName()), true, false);

        assertEquals("Unexpected close cause", AMQConstant.IN_USE, closeCause.get());
        assertEquals("Unexpected close message", "Exchange has bindings", closeMessage.get());
    }

    public void testReceiveExchangeDeleteWhenIfUsedIsSetAndExchangeHasNoBinding() throws Exception
    {
        ExchangeImpl<?> exchange = mock(ExchangeImpl.class);
        when(exchange.hasBindings()).thenReturn(false);
        doReturn(exchange).when(_virtualHost).getAttainedExchange(getTestName());

        AMQChannel channel = new AMQChannel(_protocolSession, 1, _messageStore);
        channel.receiveExchangeDelete(AMQShortString.valueOf(getTestName()), true, false);

        verify(_virtualHost).removeExchange(exchange, false);
    }

}
