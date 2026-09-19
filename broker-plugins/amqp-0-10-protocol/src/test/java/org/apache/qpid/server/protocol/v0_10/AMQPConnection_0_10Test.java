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

package org.apache.qpid.server.protocol.v0_10;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

class AMQPConnection_0_10Test extends UnitTestBase
{
    private TaskExecutor _taskExecutor;
    private Broker<?> _broker;
    private AmqpPort<?> _port;
    private ServerNetworkConnection _network;

    @BeforeEach
    void setUp()
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        final SystemConfig<?> systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(mock(EventLogger.class));
        _broker = mock(Broker.class);
        doReturn(systemConfig).when(_broker).getParent();
        when(_broker.getModel()).thenReturn(BrokerModel.getInstance());
        doReturn(Broker.class).when(_broker).getCategoryClass();
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(_broker.getNetworkBufferSize()).thenReturn(0xffff);

        _port = mock(AmqpPort.class);
        doReturn(_broker).when(_port).getParent();
        when(_port.getModel()).thenReturn(BrokerModel.getInstance());
        doReturn(Port.class).when(_port).getCategoryClass();
        when(_port.getChildExecutor()).thenReturn(_taskExecutor);
        when(_port.getAuthenticationProvider()).thenReturn(mock(AuthenticationProvider.class));
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(100);

        _network = mock(ServerNetworkConnection.class);
        when(_network.getSender()).thenReturn(mock(ByteBufferSender.class));
        when(_network.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
    }

    @AfterEach
    void tearDown()
    {
        _taskExecutor.stop();
    }

    @ParameterizedTest
    @MethodSource("decodingRuntimeFailures")
    void testRuntimeDecodingFailureIsConnectionScoped(final RuntimeException failure)
    {
        try (final MockedConstruction<ServerInputHandler> construction = mockConstruction(ServerInputHandler.class,
                (handler, context) -> doThrow(failure).when(handler).received(any(QpidByteBuffer.class))))
        {
            final AMQPConnection_0_10Impl connection = createConnection();
            try (final QpidByteBuffer input = QpidByteBuffer.allocate(16))
            {
                final ConnectionScopedRuntimeException exception =
                        assertThrows(ConnectionScopedRuntimeException.class, () -> connection.received(input));
                assertSame(failure, exception.getCause());
                verify(construction.constructed().get(0)).received(input);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("scopedAndFatalFailures")
    void testReceivePreservesScopedAndFatalFailures(final Throwable failure)
    {
        try (final MockedConstruction<ServerInputHandler> construction = mockConstruction(ServerInputHandler.class,
                (handler, context) -> doThrow(failure).when(handler).received(any(QpidByteBuffer.class))))
        {
            final AMQPConnection_0_10Impl connection = createConnection();
            try (final QpidByteBuffer input = QpidByteBuffer.allocate(16))
            {
                assertSame(failure, assertThrows(failure.getClass(), () -> connection.onReceive(input)));
                verify(construction.constructed().get(0)).received(input);
            }
        }
    }

    private AMQPConnection_0_10Impl createConnection()
    {
        return new AMQPConnection_0_10Impl(_broker, _network, _port, Transport.TCP, 0, new AggregateTicker());
    }

    private static Stream<RuntimeException> decodingRuntimeFailures()
    {
        return Stream.of(new ArithmeticException("Injected decoding failure"),
                new ClassCastException("Injected decoding failure"), new RuntimeException("Injected decoding failure"));
    }

    private static Stream<Throwable> scopedAndFatalFailures()
    {
        return Stream.of(new ConnectionScopedRuntimeException("Injected connection failure"),
                new ServerScopedRuntimeException("Injected server failure"),
                new StoreException("Injected store failure"), new InternalError("Injected JVM failure"));
    }
}
