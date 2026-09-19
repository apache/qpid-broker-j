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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;
import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.properties.ConnectionStartProperties;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.FrameCreatingMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.NonBlockingConnection;
import org.apache.qpid.server.transport.NonBlockingConnectionPlainDelegate;
import org.apache.qpid.server.transport.NonBlockingConnectionTLSDelegate;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.VirtualHostPrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes"})
class AMQPConnection_0_8Test extends UnitTestBase
{
    private static final String VIRTUAL_HOST_NAME = "vhost";
    private static final byte[] SASL_RESPONSE = "response".getBytes();
    private static final AMQShortString LOCALE = AMQShortString.createAMQShortString("en_US");
    private static final AMQShortString SASL_MECH = AMQShortString.createAMQShortString("MECH");
    private static final byte FRAME_END = (byte) 0xCE;

    private TaskExecutorImpl _taskExecutor;
    private Broker _broker;
    private QueueManagingVirtualHost _virtualHost;
    private AmqpPort _port;
    private ServerNetworkConnection _network;
    private Transport _transport;
    private Protocol _protocol;
    private AggregateTicker _ticker;
    private AuthenticationProvider _authenticationProvider;
    private SubjectCreator _subjectCreator;
    private ByteBufferSender _sender;

    @BeforeEach
    void setUp() throws Exception
    {
        final EventLogger value = new EventLogger();

        final SystemConfig systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(mock(EventLogger.class));

        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        final Model model = BrokerModel.getInstance();

        _broker = mock(Broker.class);
        when(_broker.getParent()).thenReturn(systemConfig);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getEventLogger()).thenReturn(value);
        when(_broker.getContextValue(Long.class, Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)).thenReturn(0L);

        final VirtualHostNode virtualHostNode = mock(VirtualHostNode.class);
        when(virtualHostNode.getParent()).thenReturn(_broker);
        when(virtualHostNode.getModel()).thenReturn(model);
        when(virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);
        when(virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);

        _virtualHost = mock(QueueManagingVirtualHost.class);
        final VirtualHostPrincipal virtualHostPrincipal = new VirtualHostPrincipal(_virtualHost);
        when(_virtualHost.getParent()).thenReturn(virtualHostNode);
        when(_virtualHost.getModel()).thenReturn(model);
        when(_virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        when(_virtualHost.getState()).thenReturn(State.ACTIVE);
        when(_virtualHost.isActive()).thenReturn(true);

        when(_virtualHost.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHost.getPrincipal()).thenReturn(virtualHostPrincipal);
        when(_virtualHost.getContextValue(Integer.class, Broker.MESSAGE_COMPRESSION_THRESHOLD_SIZE)).thenReturn(1024);
        when(_virtualHost.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(1024L);
        when(_virtualHost.getContextValue(Boolean.class, Broker.BROKER_MSG_AUTH)).thenReturn(false);
        when(_virtualHost.authoriseCreateConnection(any(AMQPConnection.class))).thenReturn(true);
        when(_virtualHost.getEventLogger()).thenReturn(value);

        _subjectCreator = mock(SubjectCreator.class);

        final SaslNegotiator _saslNegotiator = mock(SaslNegotiator.class);
        when(_subjectCreator.createSaslNegotiator(eq(SASL_MECH.toString()), any(SaslSettings.class)))
                .thenReturn(_saslNegotiator);
        when(_subjectCreator.authenticate(_saslNegotiator, SASL_RESPONSE)).thenReturn(new SubjectAuthenticationResult(
                new AuthenticationResult(new AuthenticatedPrincipal(new UsernamePrincipal("username", null))), new Subject()));

        _authenticationProvider = mock(AuthenticationProvider.class);
        when(_authenticationProvider.getAvailableMechanisms(anyBoolean())).thenReturn(List.of(SASL_MECH.toString()));

        _port = mock(AmqpPort.class);
        when(_port.getParent()).thenReturn(_broker);
        when(_port.getCategoryClass()).thenReturn(Port.class);
        when(_port.getChildExecutor()).thenReturn(_taskExecutor);
        when(_port.getModel()).thenReturn(model);
        when(_port.getAuthenticationProvider()).thenReturn(_authenticationProvider);
        when(_port.getAddressSpace(VIRTUAL_HOST_NAME)).thenReturn(_virtualHost);
        when(_port.getContextValue(Long.class, Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY)).thenReturn(2500L);
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(Connection.DEFAULT_MAX_MESSAGE_SIZE);
        when(_port.getSubjectCreator(eq(false), anyString())).thenReturn(_subjectCreator);

        _sender = mock(ByteBufferSender.class);

        _network = mock(ServerNetworkConnection.class);
        when(_network.getSender()).thenReturn(_sender);
        when(_network.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(_network.getSelectedHost()).thenReturn("localhost");

        _transport = Transport.TCP;
        _protocol = Protocol.AMQP_0_8;
        _ticker = new AggregateTicker();
    }

    @AfterEach
    void tearDown()
    {
        _taskExecutor.stopImmediately();
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNKNOWN", "DISABLED", "SECURE_ONLY"})
    void testUnadvertisedSaslMechanismRejected(final String mechanismName)
    {
        final AMQPConnection_0_8Impl connection = createConnectionAwaitingStartOk();
        clearInvocations(_network, _sender);

        connection.receiveConnectionStartOk(FieldTableFactory.createFieldTable(Map.of()),
                AMQShortString.createAMQShortString(mechanismName), SASL_RESPONSE, LOCALE);

        verify(_network).close();
        verifyNoInteractions(_sender);
        verify(_subjectCreator, never()).createSaslNegotiator(eq(mechanismName), any(SaslSettings.class));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void testMissingSaslMechanismClosesNetworkWithoutResponse(final String mechanismName)
    {
        final AMQPConnection_0_8Impl connection = createConnectionAwaitingStartOk();
        clearInvocations(_network, _sender);
        final AMQShortString mechanism = mechanismName == null
                ? null
                : AMQShortString.createAMQShortString(mechanismName);

        connection.receiveConnectionStartOk(FieldTableFactory.createFieldTable(Map.of()), mechanism,
                SASL_RESPONSE, LOCALE);

        verify(_network).close();
        verifyNoInteractions(_sender);
        verify(_subjectCreator, never()).createSaslNegotiator(anyString(), any(SaslSettings.class));
    }

    @Test
    void testMechanismAddedAfterAdvertisementRejected()
    {
        final String mechanismName = "NEWLY_ENABLED";
        final List<String> availableMechanisms = new ArrayList<>(List.of(SASL_MECH.toString()));
        when(_authenticationProvider.getAvailableMechanisms(false)).thenReturn(availableMechanisms);

        final AMQPConnection_0_8Impl connection = createConnectionAwaitingStartOk();
        availableMechanisms.add(mechanismName);
        clearInvocations(_network, _sender);

        connection.receiveConnectionStartOk(FieldTableFactory.createFieldTable(Map.of()),
                AMQShortString.createAMQShortString(mechanismName), SASL_RESPONSE, LOCALE);

        verify(_network).close();
        verifyNoInteractions(_sender);
        verify(_subjectCreator, never()).createSaslNegotiator(eq(mechanismName), any(SaslSettings.class));
    }

    @Test
    void closeOnNoRoute()
    {
        {
            final AMQPConnection_0_8Impl conn =
                    new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
            conn.create();
            conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));

            final FieldTable startFieldTable = FieldTable
                    .convertToFieldTable(Map.of(ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE, Boolean.TRUE));
            conn.receiveConnectionStartOk(startFieldTable, SASL_MECH, SASL_RESPONSE, LOCALE);

            assertTrue(conn.isCloseWhenNoRoute(), "Unexpected closeWhenNoRoute value");
        }

        {
            final AMQPConnection_0_8Impl
                    conn = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
            conn.create();
            conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));

            final FieldTable startFieldTable = FieldTable
                    .convertToFieldTable(Map.of(ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE, Boolean.FALSE));
            conn.receiveConnectionStartOk(startFieldTable, SASL_MECH, SASL_RESPONSE, LOCALE);
            assertFalse(conn.isCloseWhenNoRoute(), "Unexpected closeWhenNoRoute value");
        }
    }

    @Test
    void connectionEnforcesMaxSessions()
    {
        final AMQPConnection_0_8Impl conn = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                _protocol, 0, _ticker);
        conn.create();

        conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));
        conn.receiveConnectionStartOk(FieldTableFactory.createFieldTable(Map.of()), SASL_MECH, SASL_RESPONSE, LOCALE);
        final int maxChannels = 10;
        conn.receiveConnectionTuneOk(maxChannels, 65535, 0);
        conn.receiveConnectionOpen(AMQShortString.createAMQShortString(VIRTUAL_HOST_NAME), AMQShortString.EMPTY_STRING, false);

        // check the channel count is correct
        final int channelCount = conn.getSessionModels().size();
        assertEquals(0, (long) channelCount, "Initial channel count wrong");

        assertEquals(maxChannels, (long) conn.getSessionCountLimit(), "Number of channels not correctly set.");


        assertFalse(conn.isClosing(), "Connection should not be closed after opening " + maxChannels + " channels");
        for (long currentChannel = 1L; currentChannel <= maxChannels; currentChannel++)
        {
            conn.receiveChannelOpen((int) currentChannel);
        }

        assertFalse(conn.isClosing(), "Connection should not be closed after opening " + maxChannels + " channels");
        assertEquals(maxChannels, (long) conn.getSessionModels().size(), "Maximum number of channels not set.");
        conn.receiveChannelOpen(maxChannels + 1);
        assertTrue(conn.isClosing(), "Connection should be closed after opening " + (maxChannels + 1) + " channels");
    }

    @Test
    void nestingViolationBeforeOpenClosesNetworkWithoutResponse()
    {
        final RecordingAMQPConnection connection = new RecordingAMQPConnection(_broker, _network, _port, _transport,
                _protocol, _ticker);
        connection.create();
        connection.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer buffer = createConnectionStartOkFrame(
                buildNestedTable(AMQPConnection_0_8.DEFAULT_CODEC_MAX_NESTED_OBJECTS + 1),
                SASL_MECH.toString(), SASL_RESPONSE, LOCALE.toString()))
        {
            assertDoesNotThrow(() -> connection.received(buffer));
        }

        assertEquals(0, connection.getCloseCount());
        verify(_network).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void nestingViolationAfterOpenProducesResourceError()
    {
        final RecordingAMQPConnection connection = createOpenRecordingConnection(ProtocolVersion.v0_8,
                Protocol.AMQP_0_8);
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createQueueDeclareFrame(
                buildNestedTable(AMQPConnection_0_8.DEFAULT_CODEC_MAX_NESTED_OBJECTS + 1)))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(ErrorCodes.RESOURCE_ERROR, connection.getCloseErrorCode());
        assertEquals(0, connection.getCloseChannelId());
        assertEquals(QueueDeclareBody.CLASS_ID, connection.getCloseClassId());
        assertEquals(QueueDeclareBody.METHOD_ID, connection.getCloseMethodId());
        assertEquals(1, connection.getCloseCount());
        verify(_network, never()).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void codedDecodingErrorAfterOpenProducesConnectionClose()
    {
        final RecordingAMQPConnection connection = createOpenRecordingConnection(ProtocolVersion.v0_8,
                Protocol.AMQP_0_8);
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createMethodFrame(1, ConnectionTuneOkBody.CLASS_ID,
                ConnectionTuneOkBody.METHOD_ID))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(ErrorCodes.COMMAND_INVALID, connection.getCloseErrorCode());
        assertEquals(ConnectionTuneOkBody.CLASS_ID, connection.getCloseClassId());
        assertEquals(ConnectionTuneOkBody.METHOD_ID, connection.getCloseMethodId());
        assertEquals(1, connection.getCloseCount());
        verify(_network, never()).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void truncatedMethodAfterOpenProducesFrameErrorWithMethodIdentifiers()
    {
        final RecordingAMQPConnection connection = createOpenRecordingConnection(ProtocolVersion.v0_91,
                Protocol.AMQP_0_9_1);
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createMethodFrame(1, BasicQosBody.CLASS_ID, BasicQosBody.METHOD_ID))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(ErrorCodes.FRAME_ERROR, connection.getCloseErrorCode());
        assertEquals(BasicQosBody.CLASS_ID, connection.getCloseClassId());
        assertEquals(BasicQosBody.METHOD_ID, connection.getCloseMethodId());
        assertEquals(1, connection.getCloseCount());
        verify(_network, never()).close();
        verifyNoInteractions(_sender);
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void testDecodingFailureWithoutProgressClosesTransport(final boolean direct, final boolean initialProgress)
            throws Exception
    {
        final AtomicInteger decodeCount = new AtomicInteger();
        try (final MockedConstruction<BrokerDecoder> construction = mockConstruction(BrokerDecoder.class,
                (decoder, context) -> doAnswer(invocation ->
                {
                    final QpidByteBuffer input = invocation.getArgument(0);
                    if (decodeCount.getAndIncrement() == 0 && initialProgress)
                    {
                        input.position(input.position() + Integer.BYTES);
                    }
                    throw new AMQFrameDecodingException("Frame header validation failed");
                }).when(decoder).decodeBuffer(any(QpidByteBuffer.class))))
        {
            final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                    _protocol, 0, _ticker);
            openConnection(connection, ProtocolVersion.v0_8);
            clearInvocations(_network, _sender);

            try (final QpidByteBuffer input = QpidByteBuffer.allocate(direct, 16))
            {
                connection.received(input);

                assertFalse(input.hasRemaining(), "Failed input must be discarded before shutdown");
                verify(_network).close();

                input.clear();
                connection.received(input);

                assertFalse(input.hasRemaining(), "Input arriving during transport shutdown must be discarded");
                verify(construction.constructed().get(0), times(initialProgress ? 2 : 1))
                        .decodeBuffer(any(QpidByteBuffer.class));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("terminalDecodingFailures")
    void testTerminalDecodingExceptionDiscardsPendingInput(final Exception failure, final boolean direct)
            throws Exception
    {
        try (final MockedConstruction<BrokerDecoder> construction = mockConstruction(BrokerDecoder.class,
                (decoder, context) -> doThrow(failure).when(decoder).decodeBuffer(any(QpidByteBuffer.class))))
        {
            final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                    _protocol, 0, _ticker);
            openConnection(connection, ProtocolVersion.v0_8);
            try (final QpidByteBuffer input = QpidByteBuffer.allocate(direct, 16))
            {
                final ConnectionScopedRuntimeException exception = assertThrows(ConnectionScopedRuntimeException.class,
                        () -> connection.received(input));
                assertSame(failure, exception.getCause());
                assertFalse(input.hasRemaining());

                input.clear();
                connection.received(input);
                assertFalse(input.hasRemaining());
                verify(construction.constructed().get(0)).decodeBuffer(any(QpidByteBuffer.class));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCloseHandshakeDiscardsPendingInput(final boolean direct)
    {
        final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                _protocol, 0, _ticker);
        openConnection(connection, ProtocolVersion.v0_8);
        connection.sendConnectionClose(ErrorCodes.FRAME_ERROR, "Closing connection", 0);
        clearInvocations(_network);

        try (final QpidByteBuffer reply = createFrame(connection.getMethodRegistry().createConnectionCloseOkBody()
                .generateFrame(0));
             final QpidByteBuffer input = QpidByteBuffer.allocate(direct, reply.remaining() + 3))
        {
            input.put(reply);
            input.put(new byte[3]);
            input.flip();
            connection.received(input);
            assertFalse(input.hasRemaining());
        }
        verify(_network).close();
    }

    @ParameterizedTest
    @MethodSource("rejectedFrameCloseReadVariants")
    void testRejectedFrameCleanupAllowsOrderlyClose(final ProtocolVersion protocolVersion,
                                                   final Protocol protocol,
                                                   final int split,
                                                   final boolean peerCloses,
                                                   final boolean oversized) throws Exception
    {
        final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                protocol, 0, _ticker);
        openConnection(connection, protocolVersion);
        connection.setMaxFrameSize(AMQDecoder.FRAME_MIN_SIZE);
        clearInvocations(_network, _sender);
        final ByteBuffer output = ByteBuffer.allocate(1024);
        doAnswer(invocation ->
        {
            final QpidByteBuffer buffer = invocation.getArgument(0);
            buffer.copyTo(output);
            return null;
        }).when(_sender).send(any(QpidByteBuffer.class));

        final AMQFrame response = peerCloses
                ? new ConnectionCloseBody(protocolVersion, ErrorCodes.REPLY_SUCCESS, AMQShortString.EMPTY_STRING,
                        0, 0).generateFrame(0)
                : connection.getMethodRegistry().createConnectionCloseOkBody().generateFrame(0);
        try (final QpidByteBuffer rejected = oversized ?
                     createFrame(ContentBody.TYPE, 1, new byte[AMQDecoder.FRAME_MIN_SIZE + 1], FRAME_END) :
                     createMethodFrame(1, BasicQosBody.CLASS_ID, BasicQosBody.METHOD_ID);
             final QpidByteBuffer reply = createFrame(response);
             final QpidByteBuffer buffer = QpidByteBuffer.allocate(rejected.remaining() + reply.remaining()))
        {
            final int rejectedFrameSize = rejected.remaining();
            final byte[] input = new byte[buffer.capacity()];
            rejected.get(input, 0, rejectedFrameSize);
            reply.get(input, rejectedFrameSize, reply.remaining());
            final int firstReadLength = split < 0 ? input.length : split;
            buffer.put(input, 0, firstReadLength);
            buffer.flip();
            connection.received(buffer);

            final int rejectionThreshold = oversized ? AMQDecoder.FRAME_HEADER_SIZE : rejectedFrameSize;
            if (firstReadLength < rejectionThreshold)
            {
                assertEquals(0, buffer.position());
                assertFalse(connection.isClosing());
            }
            else
            {
                assertTrue(connection.isClosing());
            }

            if (oversized && firstReadLength >= rejectionThreshold && firstReadLength <= rejectedFrameSize)
            {
                assertFalse(buffer.hasRemaining(), "Rejected payload must not be retained for the next read");
            }

            if (firstReadLength < input.length)
            {
                verify(_network, never()).close();
                buffer.compact();
                buffer.put(input, firstReadLength, input.length - firstReadLength);
                buffer.flip();
                connection.received(buffer);
            }

            assertFalse(buffer.hasRemaining());
            assertTrue(connection.isClosing());
            verify(_network).close();
        }

        output.flip();
        final FrameCreatingMethodProcessor methodProcessor = new FrameCreatingMethodProcessor(protocolVersion);
        new ClientDecoder(methodProcessor).decodeBuffer(output);
        final List<AMQDataBlock> frames = methodProcessor.getProcessedMethods();
        assertEquals(peerCloses ? 2 : 1, frames.size());
        final ConnectionCloseBody close = assertInstanceOf(ConnectionCloseBody.class,
                ((AMQFrame) frames.get(0)).getBodyFrame());
        assertEquals(ErrorCodes.FRAME_ERROR, close.getReplyCode());
        assertEquals(oversized ? 0 : BasicQosBody.CLASS_ID, close.getClassId());
        assertEquals(oversized ? 0 : BasicQosBody.METHOD_ID, close.getMethodId());
        if (peerCloses)
        {
            assertInstanceOf(ConnectionCloseOkBody.class, ((AMQFrame) frames.get(1)).getBodyFrame());
        }
    }

    @Test
    void testIncompleteDiscardRetainsCloseTimeout()
    {
        final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                _protocol, 0, _ticker);
        openConnection(connection, ProtocolVersion.v0_8);
        connection.setMaxFrameSize(AMQDecoder.FRAME_MIN_SIZE);
        clearInvocations(_network);

        try (final QpidByteBuffer frame = createFrame(ContentBody.TYPE, 1, new byte[AMQDecoder.FRAME_MIN_SIZE + 1],
                FRAME_END))
        {
            frame.limit(AMQDecoder.FRAME_HEADER_SIZE);
            connection.received(frame);
            assertTrue(connection.isClosing());
            assertFalse(frame.hasRemaining());
            verify(_network, never()).close();

            final long closeTimeout = connection.getContextValue(Long.class, Connection.CLOSE_RESPONSE_TIMEOUT);
            _ticker.tick(System.currentTimeMillis() + closeTimeout + 1);
            verify(_network).close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRejectedFrameKeepsTransportBuffersBounded(final boolean tls) throws Exception
    {
        final int networkBufferSize = 128;
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(networkBufferSize, 16, 1.0);
        try (final MockedStatic<SSLUtil> sslUtil = mockStatic(SSLUtil.class))
        {
            sslUtil.when(() -> SSLUtil.isSufficientToDetermineClientSNIHost(any(QpidByteBuffer.class)))
                    .thenReturn(true);
            final AMQPConnection_0_8Impl connection =
                    new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
            openConnection(connection, ProtocolVersion.v0_8);
            connection.setMaxFrameSize(AMQDecoder.FRAME_MIN_SIZE);
            clearInvocations(_network, _sender);
            final NonBlockingConnection network = mock(NonBlockingConnection.class);
            when(_port.getNetworkBufferSize()).thenReturn(networkBufferSize);
            doAnswer(invocation ->
            {
                final QpidByteBuffer input = invocation.getArgument(0);
                assertTrue(input.capacity() <= networkBufferSize, "Application buffer grew during shutdown");
                connection.received(input);
                assertFalse(input.hasRemaining(), "Shutdown retained rejected input");
                return null;
            }).when(network).processAmqpData(any(QpidByteBuffer.class));

            final Supplier<QpidByteBuffer> inputBuffer;
            final Callable<Boolean> processInput;
            final Runnable close;
            if (tls)
            {
                final NonBlockingConnectionTLSDelegate delegate = createTlsDelegate(network, networkBufferSize);
                inputBuffer = delegate::getNetInputBuffer;
                processInput = delegate::processData;
                close = () ->
                {
                    delegate.shutdownInput();
                    delegate.shutdownOutput();
                };
            }
            else
            {
                final NonBlockingConnectionPlainDelegate delegate =
                        new NonBlockingConnectionPlainDelegate(network, _port);
                inputBuffer = delegate::getNetInputBuffer;
                processInput = delegate::processData;
                close = () ->
                {
                    delegate.shutdownInput();
                    delegate.shutdownOutput();
                };
            }
            try
            {
                try (final QpidByteBuffer rejected = createFrame(ContentBody.TYPE, 1,
                            new byte[AMQDecoder.FRAME_MIN_SIZE + 1], FRAME_END);
                     final QpidByteBuffer reply = createFrame(connection.getMethodRegistry()
                            .createConnectionCloseOkBody().generateFrame(0)))
                {
                    final byte[] input = new byte[rejected.remaining() + reply.remaining()];
                    final int rejectedSize = rejected.remaining();
                    rejected.get(input, 0, rejectedSize);
                    reply.get(input, rejectedSize, reply.remaining());
                    for (int offset = 0; offset < input.length;)
                    {
                        final int length = Math.min(networkBufferSize, input.length - offset);
                        inputBuffer.get().put(input, offset, length);
                        processInput.call();
                        assertTrue(inputBuffer.get().capacity() <= networkBufferSize);
                        offset += length;
                    }
                }

                verify(_network).close();
                inputBuffer.get().put(new byte[inputBuffer.get().remaining()]);
                processInput.call();
                assertEquals(networkBufferSize, inputBuffer.get().capacity());
            }
            finally
            {
                close.run();
            }
            assertEquals(0, QpidByteBuffer.getAllocatedDirectMemorySize(), "Shutdown retained pooled direct memory");
        }
        finally
        {
            QpidByteBuffer.deinitialisePool();
        }
    }

    private NonBlockingConnectionTLSDelegate createTlsDelegate(final NonBlockingConnection network,
                                                               final int networkBufferSize) throws Exception
    {
        final SSLContext sslContext = mock(SSLContext.class);
        final SSLEngine sslEngine = mock(SSLEngine.class);
        final SSLSession session = mock(SSLSession.class);
        when(_port.getSSLContext()).thenReturn(sslContext);
        when(_port.getContextValue(Boolean.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING)).thenReturn(false);
        when(_port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_WARN_THRESHOLD))
                .thenReturn(0);
        when(_port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_BREAK_THRESHOLD))
                .thenReturn(0);
        when(sslContext.createSSLEngine()).thenReturn(sslEngine);
        when(sslEngine.getSession()).thenReturn(session);
        when(session.getPacketBufferSize()).thenReturn(networkBufferSize);
        when(session.getApplicationBufferSize()).thenReturn(networkBufferSize);

        when(sslEngine.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenAnswer(invocation ->
        {
            final ByteBuffer source = invocation.getArgument(0);
            final ByteBuffer target = invocation.getArgument(1);
            final int length = source.remaining();
            target.put(source);
            return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                    length, length);
        });
        return new NonBlockingConnectionTLSDelegate(network, _port);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFatalDecodingFailureDiscardsPendingInput(final boolean direct)
    {
        final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                _protocol, 0, _ticker);
        openConnection(connection, ProtocolVersion.v0_8);
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createFrame(HeartbeatBody.TYPE, 0, new byte[0], (byte) 0);
             final QpidByteBuffer input = QpidByteBuffer.allocate(direct, frame.remaining() + 3))
        {
            input.put(frame);
            input.put(new byte[3]);
            input.flip();
            connection.received(input);

            assertFalse(input.hasRemaining());
            input.clear();
            connection.received(input);
            assertFalse(input.hasRemaining());
        }
        verify(_network).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void malformedHeartbeatAfterOpenProducesConnectionClose()
    {
        final RecordingAMQPConnection connection = createOpenRecordingConnection(ProtocolVersion.v0_91,
                Protocol.AMQP_0_9_1);
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createFrame(HeartbeatBody.TYPE, 0, new byte[] { 1 }, FRAME_END))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(ErrorCodes.FRAME_ERROR, connection.getCloseErrorCode());
        assertEquals(1, connection.getCloseCount());
        verify(_network, never()).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void unsignedContentBodySizeAfterOpenProducesChannelClose()
    {
        when(_virtualHost.getDefaultDestination()).thenReturn(mock(MessageDestination.class));
        final RecordingAMQPConnection connection = createOpenRecordingConnection(ProtocolVersion.v0_91,
                Protocol.AMQP_0_9_1);
        connection.receiveChannelOpen(1);
        final AMQChannel channel = connection.getChannel(1);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, AMQShortString.EMPTY_STRING, false, false);
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createContentHeaderFrame(1, Long.MIN_VALUE, new byte[] { (byte) 0xFF }))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(0, connection.getCloseCount());
        assertEquals(1, connection.getChannelCloseCount());
        assertSame(channel, connection.getClosedChannel());
        assertEquals(ErrorCodes.MESSAGE_TOO_LARGE, connection.getChannelCloseErrorCode());
        assertEquals("Content body size 9223372036854775808 exceeds the supported range",
                connection.getChannelCloseMessage());
        verify(_network, never()).close();
    }

    @Test
    void unsignedContentBodySizeOnUnknownChannelProducesChannelError()
    {
        final RecordingAMQPConnection connection = createOpenRecordingConnection(ProtocolVersion.v0_91,
                Protocol.AMQP_0_9_1);
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createContentHeaderFrame(1, Long.MIN_VALUE, new byte[0]))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(ErrorCodes.CHANNEL_ERROR, connection.getCloseErrorCode());
        assertEquals(1, connection.getCloseChannelId());
        assertEquals(1, connection.getCloseCount());
        assertEquals(0, connection.getChannelCloseCount());
        verify(_network, never()).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void unsignedContentBodySizeBeforeOpenClosesNetworkWithoutResponse()
    {
        final RecordingAMQPConnection connection = new RecordingAMQPConnection(_broker, _network, _port, _transport,
                _protocol, _ticker);
        connection.create();
        connection.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createContentHeaderFrame(1, Long.MIN_VALUE, new byte[0]))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(0, connection.getCloseCount());
        assertEquals(0, connection.getChannelCloseCount());
        verify(_network).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void invalidFrameEndAfterOpenClosesNetworkWithoutResponse()
    {
        final RecordingAMQPConnection connection = createOpenRecordingConnection(ProtocolVersion.v0_8,
                Protocol.AMQP_0_8);
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createFrame(HeartbeatBody.TYPE, 0, new byte[0], (byte) 0))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(0, connection.getCloseCount());
        verify(_network).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void unsupportedFrameTypeAfterOpenClosesNetworkWithoutResponse()
    {
        final RecordingAMQPConnection connection = createOpenRecordingConnection(ProtocolVersion.v0_8,
                Protocol.AMQP_0_8);
        try (final QpidByteBuffer heartbeat = createFrame(HeartbeatBody.TYPE, 0, new byte[0], FRAME_END))
        {
            assertDoesNotThrow(() -> connection.received(heartbeat));
        }
        clearInvocations(_network, _sender);

        try (final QpidByteBuffer frame = createFrame(Byte.MAX_VALUE, 0, new byte[0], FRAME_END))
        {
            assertDoesNotThrow(() -> connection.received(frame));
        }

        assertEquals(0, connection.getCloseCount());
        verify(_network).close();
        verifyNoInteractions(_sender);
    }

    @Test
    void resetStatistics()
    {
        final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                _protocol, 0, _ticker);
        connection.create();
        connection.setAddressSpace(_virtualHost);
        connection.registerMessageReceived(100L);
        connection.registerMessageDelivered(100L);
        connection.registerTransactedMessageReceived();
        connection.registerTransactedMessageDelivered();

        final Map<String, Object> statisticsBeforeReset = connection.getStatistics();
        assertEquals(100L, statisticsBeforeReset.get("bytesIn"));
        assertEquals(100L, statisticsBeforeReset.get("bytesOut"));
        assertEquals(1L, statisticsBeforeReset.get("messagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("messagesOut"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesOut"));

        connection.resetStatistics();

        final Map<String, Object> statisticsAfterReset = connection.getStatistics();
        assertEquals(0L, statisticsAfterReset.get("bytesIn"));
        assertEquals(0L, statisticsAfterReset.get("bytesOut"));
        assertEquals(0L, statisticsAfterReset.get("messagesIn"));
        assertEquals(0L, statisticsAfterReset.get("messagesOut"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesIn"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesOut"));
    }

    private static QpidByteBuffer createConnectionStartOkFrame(final byte[] fieldTable,
                                                               final String mechanism,
                                                               final byte[] response,
                                                               final String locale)
    {
        final byte[] mechanismBytes = mechanism.getBytes(StandardCharsets.UTF_8);
        final byte[] localeBytes = locale.getBytes(StandardCharsets.UTF_8);
        final int bodySize = Integer.BYTES + Integer.BYTES + fieldTable.length + 1 + mechanismBytes.length +
                Integer.BYTES + response.length + 1 + localeBytes.length;
        final QpidByteBuffer frame = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1);
        frame.put((byte) 1);
        frame.putUnsignedShort(0);
        frame.putUnsignedInt(bodySize);
        frame.putInt((ConnectionStartOkBody.CLASS_ID << 16) | ConnectionStartOkBody.METHOD_ID);
        frame.putUnsignedInt(fieldTable.length);
        frame.put(fieldTable);
        frame.put((byte) mechanismBytes.length);
        frame.put(mechanismBytes);
        frame.putUnsignedInt(response.length);
        frame.put(response);
        frame.put((byte) localeBytes.length);
        frame.put(localeBytes);
        frame.put(FRAME_END);
        frame.flip();
        return frame;
    }

    private static QpidByteBuffer createQueueDeclareFrame(final byte[] arguments)
    {
        final int bodySize = Integer.BYTES + Short.BYTES + 2 + Integer.BYTES + arguments.length;
        final QpidByteBuffer frame = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1);
        frame.put((byte) 1);
        frame.putUnsignedShort(1);
        frame.putUnsignedInt(bodySize);
        frame.putInt((QueueDeclareBody.CLASS_ID << 16) | QueueDeclareBody.METHOD_ID);
        frame.putUnsignedShort(0);
        frame.put((byte) 0);
        frame.put((byte) 0);
        frame.putUnsignedInt(arguments.length);
        frame.put(arguments);
        frame.put(FRAME_END);
        frame.flip();
        return frame;
    }

    private static QpidByteBuffer createMethodFrame(final int channelId,
                                                    final int classId,
                                                    final int methodId)
    {
        final ByteBuffer body = ByteBuffer.allocate(Integer.BYTES);
        body.putInt((classId << 16) | methodId);
        return createFrame((byte) 1, channelId, body.array(), FRAME_END);
    }

    private static QpidByteBuffer createContentHeaderFrame(final int channelId,
                                                           final long bodySize,
                                                           final byte[] properties)
    {
        final ByteBuffer body = ByteBuffer.allocate(Short.BYTES * 3 + Long.BYTES + properties.length);
        body.putShort((short) ContentHeaderBody.CLASS_ID);
        body.putShort((short) 0);
        body.putLong(bodySize);
        body.putShort((short) 0);
        body.put(properties);
        return createFrame(ContentHeaderBody.TYPE, channelId, body.array(), FRAME_END);
    }

    private static QpidByteBuffer createFrame(final AMQFrame frame)
    {
        final QpidByteBuffer buffer = QpidByteBuffer.allocate((int) frame.getSize());
        final ByteBufferSender sender = mock(ByteBufferSender.class);
        doAnswer(invocation ->
        {
            final QpidByteBuffer payload = invocation.getArgument(0);
            buffer.put(payload);
            return null;
        }).when(sender).send(any(QpidByteBuffer.class));
        frame.writePayload(sender);
        buffer.flip();
        return buffer;
    }

    @ParameterizedTest
    @MethodSource("scopedAndFatalFailures")
    void testReceivePreservesScopedAndFatalFailures(final Throwable failure) throws Exception
    {
        try (final MockedConstruction<BrokerDecoder> construction = mockConstruction(BrokerDecoder.class,
                (decoder, context) -> doThrow(failure).when(decoder).decodeBuffer(any(QpidByteBuffer.class))))
        {
            final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                    _protocol, 0, _ticker);
            try (final QpidByteBuffer input = QpidByteBuffer.allocate(16))
            {
                assertSame(failure, assertThrows(failure.getClass(), () -> connection.onReceive(input)));
                verify(construction.constructed().get(0)).decodeBuffer(input);
            }
        }
    }

    private static Stream<Arguments> terminalDecodingFailures()
    {
        return Stream.of(new IOException("Injected decoding failure"),
                new ArithmeticException("Injected decoding failure"), new RuntimeException("Injected decoding failure"))
                .flatMap(failure -> Stream.of(false, true).map(direct -> Arguments.of(failure, direct)));
    }

    private static Stream<Throwable> scopedAndFatalFailures()
    {
        return Stream.of(new ConnectionScopedRuntimeException("Injected connection failure"),
                new ServerScopedRuntimeException("Injected server failure"),
                new StoreException("Injected store failure"), new InternalError("Injected JVM failure"));
    }

    private static Stream<Arguments> rejectedFrameCloseReadVariants()
    {
        return Stream.of(Arguments.of(ProtocolVersion.v0_8, Protocol.AMQP_0_8),
                Arguments.of(ProtocolVersion.v0_9, Protocol.AMQP_0_9),
                Arguments.of(ProtocolVersion.v0_91, Protocol.AMQP_0_9_1))
                .flatMap(arguments -> Stream.of(3, 7, 11, 12, 15, -1)
                        .flatMap(split -> Stream.of(false, true)
                                .flatMap(peerCloses -> Stream.of(false, true)
                                        .map(oversized -> Arguments.of(arguments.get()[0], arguments.get()[1],
                                                                      split, peerCloses, oversized)))));
    }

    private static QpidByteBuffer createFrame(final byte type,
                                              final int channelId,
                                              final byte[] body,
                                              final byte frameEnd)
    {
        final QpidByteBuffer frame = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + body.length + 1);
        frame.put(type);
        frame.putUnsignedShort(channelId);
        frame.putUnsignedInt(body.length);
        frame.put(body);
        frame.put(frameEnd);
        frame.flip();
        return frame;
    }

    private static byte[] buildNestedTable(final int depth)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(7 * depth);
        for (int remainingDepth = depth; remainingDepth > 1; remainingDepth--)
        {
            buffer.put((byte) 1);
            buffer.put((byte) 'n');
            buffer.put(AMQType.FIELD_TABLE.identifier());
            buffer.putInt(7 * (remainingDepth - 1));
        }
        buffer.put((byte) 1);
        buffer.put((byte) 'v');
        buffer.put(AMQType.INT.identifier());
        buffer.putInt(42);
        return buffer.array();
    }

    private static final class RecordingAMQPConnection extends AMQPConnection_0_8Impl
    {
        private int _closeErrorCode;
        private int _closeChannelId;
        private int _closeClassId;
        private int _closeMethodId;
        private int _closeCount;
        private AMQChannel _closedChannel;
        private int _channelCloseErrorCode;
        private String _channelCloseMessage;
        private int _channelCloseCount;

        private RecordingAMQPConnection(final Broker broker,
                                        final ServerNetworkConnection network,
                                        final AmqpPort port,
                                        final Transport transport,
                                        final Protocol protocol,
                                        final AggregateTicker ticker)
        {
            super(broker, network, port, transport, protocol, 0, ticker);
        }

        @Override
        public void sendConnectionClose(final int errorCode, final String message, final int channelId)
        {
            recordConnectionClose(errorCode, channelId, 0, 0);
        }

        @Override
        protected void sendConnectionClose(final int errorCode,
                                           final String message,
                                           final int channelId,
                                           final int classId,
                                           final int methodId)
        {
            recordConnectionClose(errorCode, channelId, classId, methodId);
        }

        @Override
        public void closeChannelAndWriteFrame(final AMQChannel channel, final int cause, final String message)
        {
            _closedChannel = channel;
            _channelCloseErrorCode = cause;
            _channelCloseMessage = message;
            _channelCloseCount++;
            super.closeChannelAndWriteFrame(channel, cause, message);
        }

        private void recordConnectionClose(final int errorCode,
                                           final int channelId,
                                           final int classId,
                                           final int methodId)
        {
            _closeErrorCode = errorCode;
            _closeChannelId = channelId;
            _closeClassId = classId;
            _closeMethodId = methodId;
            _closeCount++;
        }

        private int getCloseErrorCode()
        {
            return _closeErrorCode;
        }

        private int getCloseChannelId()
        {
            return _closeChannelId;
        }

        private int getCloseClassId()
        {
            return _closeClassId;
        }

        private int getCloseMethodId()
        {
            return _closeMethodId;
        }

        private int getCloseCount()
        {
            return _closeCount;
        }

        private AMQChannel getClosedChannel()
        {
            return _closedChannel;
        }

        private int getChannelCloseErrorCode()
        {
            return _channelCloseErrorCode;
        }

        private String getChannelCloseMessage()
        {
            return _channelCloseMessage;
        }

        private int getChannelCloseCount()
        {
            return _channelCloseCount;
        }
    }

    private AMQPConnection_0_8Impl createConnectionAwaitingStartOk()
    {
        final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport,
                _protocol, 0, _ticker);
        connection.create();
        connection.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));
        return connection;
    }

    private RecordingAMQPConnection createOpenRecordingConnection(final ProtocolVersion protocolVersion,
                                                                   final Protocol protocol)
    {
        final RecordingAMQPConnection connection = new RecordingAMQPConnection(_broker, _network, _port, _transport,
                protocol, _ticker);
        openConnection(connection, protocolVersion);
        return connection;
    }

    private void openConnection(final AMQPConnection_0_8Impl connection, final ProtocolVersion protocolVersion)
    {
        connection.create();
        connection.receiveProtocolHeader(new ProtocolInitiation(protocolVersion));
        connection.receiveConnectionStartOk(FieldTableFactory.createFieldTable(Map.of()), SASL_MECH, SASL_RESPONSE,
                LOCALE);
        connection.receiveConnectionTuneOk(10, 65535, 0);
        connection.receiveConnectionOpen(AMQShortString.createAMQShortString(VIRTUAL_HOST_NAME),
                AMQShortString.EMPTY_STRING, false);
    }
}
