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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.security.Principal;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
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
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionHeartbeat;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolError;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolEvent;
import org.apache.qpid.server.protocol.v0_10.transport.SegmentType;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

class ServerAssemblerTest extends UnitTestBase
{
    private static final byte COMPLETE_SEGMENT =
            ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG | ServerFrame.FIRST_FRAME | ServerFrame.LAST_FRAME;
    private static final byte FIRST_FRAGMENT =
            ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG | ServerFrame.FIRST_FRAME;
    private static final byte MIDDLE_FRAGMENT = ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG;
    private static final byte LAST_FRAGMENT =
            ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG | ServerFrame.LAST_FRAME;
    private static final byte COMPLETE_FIRST_SEGMENT =
            ServerFrame.FIRST_SEG | ServerFrame.FIRST_FRAME | ServerFrame.LAST_FRAME;
    private static final byte COMPLETE_MIDDLE_SEGMENT = ServerFrame.FIRST_FRAME | ServerFrame.LAST_FRAME;
    private static final byte FIRST_BODY_FRAGMENT = ServerFrame.LAST_SEG | ServerFrame.FIRST_FRAME;
    private static final byte LAST_BODY_FRAGMENT = ServerFrame.LAST_SEG | ServerFrame.LAST_FRAME;

    private ServerConnection _connection;
    private AMQPConnection_0_10Impl _amqpConnection;

    @BeforeEach
    void setUp()
    {
        _connection = mock(ServerConnection.class);
        when(_connection.getMaxMessageSize()).thenReturn(Integer.MAX_VALUE);
        _amqpConnection = mock(AMQPConnection_0_10Impl.class);
        when(_connection.getAmqpConnection()).thenReturn(_amqpConnection);
        when(_amqpConnection.getMaxNestedObjects()).thenReturn(AMQPConnection_0_10.DEFAULT_CODEC_MAX_NESTED_OBJECTS);
        when(_amqpConnection.getMaxZeroWidthArrayElements())
                .thenReturn(AMQPConnection_0_10.DEFAULT_CODEC_MAX_ZERO_WIDTH_ARRAY_ELEMENTS);
    }

    @Test
    void completeFrameUsesFastPathWithoutInitializingSegmentLimits()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection);
        final QpidByteBuffer body = QpidByteBuffer.wrap(new byte[]{0x01, 0x0A, 0x00, 0x00});

        assembler.frame(frame(COMPLETE_SEGMENT, SegmentType.CONTROL, (byte) 0, 0, body));

        verify(_amqpConnection, never())
                .getContextValue(Integer.class, AMQPConnection_0_10.CONNECTION_MAX_UNASSEMBLED_SEGMENT_BYTES);
        verify(_amqpConnection, never())
                .getContextValue(Integer.class, AMQPConnection_0_10.CONNECTION_MAX_UNASSEMBLED_SEGMENT_FRAMES);
        verify(_connection, never()).getMaxMessageSize();
        verify(_connection).received(isA(ConnectionHeartbeat.class));
    }

    @ParameterizedTest
    @CsvSource({"2, 3", "5, 0"})
    void testLowerMessageSizeLimitAppliesToRetainedSegments(final int retainedSize, final int incomingSize)
    {
        final ServerAssembler assembler = new ServerAssembler(_connection);
        configureSegmentLimits(100);
        when(_connection.getMaxMessageSize()).thenReturn(100);
        final QpidByteBuffer retainedBody = buffer(retainedSize);
        final QpidByteBuffer incomingBody = buffer(incomingSize);

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, retainedBody));
        when(_connection.getMaxMessageSize()).thenReturn(4);

        assertThrows(IllegalArgumentException.class, () ->
                assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 2, incomingBody)));

        assembler.closed();
        verify(retainedBody).dispose();
        verify(incomingBody).dispose();
        verify(_connection).received(isA(ProtocolError.class));
    }

    @Test
    void testHigherMessageSizeLimitPreservesConfiguredSegmentCap()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection);
        configureSegmentLimits(10);
        when(_connection.getMaxMessageSize()).thenReturn(4);
        final QpidByteBuffer firstBody = buffer(2);
        final QpidByteBuffer middleBody = buffer(8);
        final QpidByteBuffer lastBody = buffer(1);

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, firstBody));
        when(_connection.getMaxMessageSize()).thenReturn(100);
        assembler.frame(frame(MIDDLE_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, middleBody));

        assertThrows(IllegalArgumentException.class, () ->
                assembler.frame(frame(LAST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, lastBody)));

        verify(firstBody).dispose();
        verify(middleBody).dispose();
        verify(lastBody).dispose();
        verify(_amqpConnection)
                .getContextValue(Integer.class, AMQPConnection_0_10.CONNECTION_MAX_UNASSEMBLED_SEGMENT_BYTES);
        verify(_amqpConnection)
                .getContextValue(Integer.class, AMQPConnection_0_10.CONNECTION_MAX_UNASSEMBLED_SEGMENT_FRAMES);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testVirtualHostSelectionUpdatesReassemblyLimit(final boolean direct)
    {
        final TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        final ServerAssembler assembler = new ServerAssembler(_connection);
        try (final QpidByteBuffer firstBody = spy(QpidByteBuffer.allocate(direct, 2));
             final QpidByteBuffer lastBody = spy(QpidByteBuffer.allocate(direct, 3)))
        {
            final AMQPConnection_0_10Impl amqpConnection = createAmqpConnection(taskExecutor);
            when(_connection.getMaxMessageSize()).thenAnswer(invocation -> (int) amqpConnection.getMaxMessageSize());
            configureSegmentLimits(100);

            assembler.frame(frame(FIRST_FRAGMENT, SegmentType.CONTROL, (byte) 0, 0,
                    QpidByteBuffer.wrap(new byte[]{0x01, 0x0A})));
            assembler.frame(frame(LAST_FRAGMENT, SegmentType.CONTROL, (byte) 0, 0,
                    QpidByteBuffer.wrap(new byte[]{0x00, 0x00})));

            final QueueManagingVirtualHost<?> virtualHost = mock(QueueManagingVirtualHost.class);
            when(virtualHost.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(4);
            when(virtualHost.getContextValue(Boolean.class, Broker.BROKER_MSG_AUTH)).thenReturn(false);
            when(virtualHost.getContextValue(Integer.class, Broker.MESSAGE_COMPRESSION_THRESHOLD_SIZE))
                    .thenReturn(1024);
            when(virtualHost.getPrincipal()).thenReturn(mock(Principal.class));
            when(virtualHost.getEventLogger()).thenReturn(mock(EventLogger.class));

            amqpConnection.setAddressSpace(virtualHost);
            assertEquals(4L, amqpConnection.getMaxMessageSize());

            assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, firstBody));
            assertThrows(IllegalArgumentException.class, () ->
                    assembler.frame(frame(LAST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, lastBody)));

            verify(firstBody).dispose();
            verify(lastBody).dispose();
            verify(_connection).received(isA(ProtocolError.class));
        }
        finally
        {
            assembler.closed();
            taskExecutor.stop();
        }
    }

    @Test
    void fragmentedControlIsAssembled()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 4L, 2);
        final QpidByteBuffer firstBody = QpidByteBuffer.wrap(new byte[]{0x01, 0x0A});
        final QpidByteBuffer lastBody = QpidByteBuffer.wrap(new byte[]{0x00, 0x00});

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.CONTROL, (byte) 0, 0, firstBody));
        assembler.frame(frame(LAST_FRAGMENT, SegmentType.CONTROL, (byte) 0, 0, lastBody));

        final ProtocolEvent event = captureReceivedEvent();
        assertInstanceOf(ConnectionHeartbeat.class, event);
    }

    @Test
    void fragmentedMessageBodyIsAssembled()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 4L, 2);
        final QpidByteBuffer commandBody = QpidByteBuffer.wrap(new byte[]{0x04, 0x01, 0x01, 0x00, 0x00, 0x00});
        final QpidByteBuffer headerBody = QpidByteBuffer.wrap(new byte[0]);
        final QpidByteBuffer firstBody = QpidByteBuffer.wrap(new byte[]{0x01, 0x02});
        final QpidByteBuffer lastBody = QpidByteBuffer.wrap(new byte[]{0x03, 0x04});

        assembler.frame(frame(COMPLETE_FIRST_SEGMENT, SegmentType.COMMAND, (byte) 1, 1, commandBody));
        assembler.frame(frame(COMPLETE_MIDDLE_SEGMENT, SegmentType.HEADER, (byte) 1, 1, headerBody));
        assembler.frame(frame(FIRST_BODY_FRAGMENT, SegmentType.BODY, (byte) 1, 1, firstBody));
        assembler.frame(frame(LAST_BODY_FRAGMENT, SegmentType.BODY, (byte) 1, 1, lastBody));

        final MessageTransfer transfer = assertInstanceOf(MessageTransfer.class, captureReceivedEvent());
        try
        {
            final byte[] body = new byte[transfer.getBodySize()];
            transfer.getBody().get(body);
            assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04}, body);
        }
        finally
        {
            transfer.dispose();
        }
    }

    @Test
    void segmentKeysDoNotCollideAcrossChannelAndTrack()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 10L, 2);
        final QpidByteBuffer channelTwoTrackZero = buffer(1);
        final QpidByteBuffer channelOneTrackOne = buffer(1);

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 0, 2, channelTwoTrackZero));
        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, channelOneTrackOne));
        assembler.closed();

        verify(channelTwoTrackZero).dispose();
        verify(channelOneTrackOne).dispose();
        verify(_connection).closed();
    }

    @Test
    void byteLimitIsAppliedAcrossChannels()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 7L, 10);
        final QpidByteBuffer firstBody = buffer(4);
        final QpidByteBuffer secondBody = buffer(4);

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, firstBody));
        assertThrows(IllegalArgumentException.class, () ->
                assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 2, secondBody)));

        verify(firstBody).dispose();
        verify(secondBody).dispose();
        verify(_connection).received(isA(ProtocolError.class));
    }

    @Test
    void frameLimitIncludesZeroLengthFrames()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 2);
        final QpidByteBuffer firstBody = buffer(0);
        final QpidByteBuffer middleBody = buffer(0);
        final QpidByteBuffer lastBody = buffer(0);

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, firstBody));
        assembler.frame(frame(MIDDLE_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, middleBody));
        assertThrows(IllegalArgumentException.class, () ->
                assembler.frame(frame(LAST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, lastBody)));

        verify(firstBody).dispose();
        verify(middleBody).dispose();
        verify(lastBody).dispose();
        verify(_connection).received(isA(ProtocolError.class));
    }

    @Test
    void continuationMustMatchSegmentType()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 10);
        final QpidByteBuffer firstBody = buffer(1);
        final QpidByteBuffer lastBody = buffer(1);

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, firstBody));
        assertThrows(IllegalArgumentException.class, () ->
                assembler.frame(frame(LAST_FRAGMENT, SegmentType.HEADER, (byte) 1, 1, lastBody)));

        verify(firstBody).dispose();
        verify(lastBody).dispose();
    }

    @Test
    void continuationWithoutFirstFrameIsRejected()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 10);
        final QpidByteBuffer body = buffer(1);

        assertThrows(IllegalArgumentException.class, () ->
                assembler.frame(frame(LAST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, body)));

        verify(body).dispose();
        verify(_connection).received(isA(ProtocolError.class));
    }

    @Test
    void newFirstFrameDiscardsExistingSegment()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 10);
        final QpidByteBuffer existingBody = buffer(1);
        final QpidByteBuffer newBody = buffer(1);

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, existingBody));
        assertThrows(IllegalArgumentException.class, () ->
                assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, newBody)));

        verify(existingBody).dispose();
        verify(newBody).dispose();
    }

    @Test
    void protocolErrorDisposesRetainedSegments()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 10);
        final QpidByteBuffer body = buffer(1);
        final ProtocolError error = new ProtocolError(ServerFrame.L2, "test error");

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, body));
        assembler.error(error);

        verify(body).dispose();
        verify(_connection).received(error);
    }

    @Test
    void connectionExceptionDisposesRetainedSegments()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 10);
        final QpidByteBuffer body = buffer(1);
        final RuntimeException exception = new RuntimeException("test");

        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, body));
        assembler.exception(exception);

        verify(body).dispose();
        verify(_connection).exception(exception);
    }

    @Test
    void ignoredFrameBodyIsDisposed()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 10);
        final QpidByteBuffer body = buffer(1);
        when(_connection.isIgnoreFutureInput()).thenReturn(true);

        assembler.received(List.of(frame(COMPLETE_SEGMENT, SegmentType.CONTROL, (byte) 0, 0, body)));

        verify(body).dispose();
    }

    @Test
    void malformedFrameDisposesOtherRetainedSegments()
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 10);
        final QpidByteBuffer retainedBody = buffer(1);
        final QpidByteBuffer malformedBody = QpidByteBuffer.wrap(new byte[0]);
        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, retainedBody));

        assertThrows(BufferUnderflowException.class, () ->
                assembler.received(List.of(frame(COMPLETE_SEGMENT, SegmentType.CONTROL, (byte) 0, 0, malformedBody))));

        verify(retainedBody).dispose();
    }

    @ParameterizedTest
    @MethodSource("decodingFailures")
    void testDecodingFailureDisposesCurrentPendingAndRetainedFrames(final Throwable failure)
    {
        final ServerAssembler assembler = new ServerAssembler(_connection, 100L, 10);
        final QpidByteBuffer retainedBody = buffer(1);
        final QpidByteBuffer currentBody = buffer(4);
        final QpidByteBuffer pendingBody = buffer(1);
        assembler.frame(frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 1, retainedBody));

        try (final MockedConstruction<ServerDecoder> construction = mockConstruction(ServerDecoder.class,
                (decoder, context) -> when(decoder.readUint16()).thenThrow(failure)))
        {
            final List<ServerFrame> frames =
                    List.of(frame(COMPLETE_SEGMENT, SegmentType.CONTROL, (byte) 0, 0, currentBody),
                            frame(FIRST_FRAGMENT, SegmentType.COMMAND, (byte) 1, 2, pendingBody));
            assertSame(failure, assertThrows(failure.getClass(), () -> assembler.received(frames)));
            verify(construction.constructed().get(0)).readUint16();
        }

        verify(retainedBody).dispose();
        verify(currentBody, atLeastOnce()).dispose();
        verify(pendingBody).dispose();
    }

    private static Stream<Throwable> decodingFailures()
    {
        return Stream.of(new ArithmeticException("Injected decoding failure"),
                new RuntimeException("Injected decoding failure"), new InternalError("Injected JVM failure"));
    }

    private void configureSegmentLimits(final int maxBytes)
    {
        when(_amqpConnection
                .getContextValue(Integer.class, AMQPConnection_0_10.CONNECTION_MAX_UNASSEMBLED_SEGMENT_BYTES)).thenReturn(maxBytes);
        when(_amqpConnection
                .getContextValue(Integer.class, AMQPConnection_0_10.CONNECTION_MAX_UNASSEMBLED_SEGMENT_FRAMES)).thenReturn(10);
    }

    private AMQPConnection_0_10Impl createAmqpConnection(final TaskExecutor taskExecutor)
    {
        final SystemConfig<?> systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(mock(EventLogger.class));
        final Broker<?> broker = mock(Broker.class);
        doReturn(systemConfig).when(broker).getParent();
        when(broker.getModel()).thenReturn(BrokerModel.getInstance());
        doReturn(Broker.class).when(broker).getCategoryClass();
        when(broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(broker.getChildExecutor()).thenReturn(taskExecutor);
        when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(broker.getNetworkBufferSize()).thenReturn(0xffff);

        final AmqpPort<?> port = mock(AmqpPort.class);
        doReturn(broker).when(port).getParent();
        when(port.getModel()).thenReturn(BrokerModel.getInstance());
        doReturn(Port.class).when(port).getCategoryClass();
        when(port.getChildExecutor()).thenReturn(taskExecutor);
        when(port.getAuthenticationProvider()).thenReturn(mock(AuthenticationProvider.class));
        when(port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(100);

        final ServerNetworkConnection network = mock(ServerNetworkConnection.class);
        when(network.getSender()).thenReturn(mock(ByteBufferSender.class));
        when(network.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));

        return new AMQPConnection_0_10Impl(broker, network, port, Transport.TCP, 0, new AggregateTicker());
    }

    private ProtocolEvent captureReceivedEvent()
    {
        final ArgumentCaptor<ProtocolEvent> captor = ArgumentCaptor.forClass(ProtocolEvent.class);
        verify(_connection).received(captor.capture());
        return captor.getValue();
    }

    private QpidByteBuffer buffer(final int size)
    {
        final QpidByteBuffer buffer = mock(QpidByteBuffer.class);
        when(buffer.remaining()).thenReturn(size);
        return buffer;
    }

    private ServerFrame frame(final byte flags,
                              final SegmentType type,
                              final byte track,
                              final int channel,
                              final QpidByteBuffer body)
    {
        return new ServerFrame(flags, type, track, channel, body);
    }
}
