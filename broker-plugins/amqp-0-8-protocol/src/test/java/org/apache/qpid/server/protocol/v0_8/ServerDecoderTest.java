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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicPublishBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.TxSelectBody;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

class ServerDecoderTest extends UnitTestBase
{
    private static final byte FRAME_END = (byte) 0xCE;
    private static final int CHANNEL_ID = 1;

    @ParameterizedTest
    @MethodSource("rejectedFrameReadVariants")
    void testRejectedFrameIsConsumedBeforeCloseOk(final ProtocolVersion protocolVersion,
                                                  final int classAndMethod,
                                                  final int firstReadLength,
                                                  final boolean direct) throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        when(methodProcessor.getProtocolVersion()).thenReturn(protocolVersion);
        final ServerDecoder decoder = createRejectingDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final int frameSize = AMQDecoder.FRAME_HEADER_SIZE + Integer.BYTES + 1;

        try (final QpidByteBuffer frames = QpidByteBuffer.allocate(frameSize * 2);
             final QpidByteBuffer buffer = QpidByteBuffer.allocate(direct, frameSize * 2))
        {
            writeFrameHeader(frames, (byte) 1, CHANNEL_ID, Integer.BYTES);
            frames.putInt((TxSelectBody.CLASS_ID << 16) | TxSelectBody.METHOD_ID);
            frames.put(FRAME_END);
            writeFrameHeader(frames, (byte) 1, 0, Integer.BYTES);
            frames.putInt(classAndMethod);
            frames.put(FRAME_END);
            frames.flip();
            final byte[] input = new byte[frames.remaining()];
            frames.get(input);

            buffer.put(input, 0, firstReadLength);
            buffer.flip();
            if (firstReadLength < frameSize)
            {
                decoder.decodeBuffer(buffer);
                assertEquals(0, buffer.position(), "Incomplete frames must remain buffered");
                buffer.compact();
                buffer.put(input, firstReadLength, input.length - firstReadLength);
                buffer.flip();
            }

            assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
            assertEquals(frameSize, buffer.position(), "Rejected frame must be completely consumed");
            when(methodProcessor.ignoreAllButCloseOk()).thenReturn(true);

            if (firstReadLength >= frameSize && firstReadLength < input.length)
            {
                buffer.compact();
                buffer.put(input, firstReadLength, input.length - firstReadLength);
                buffer.flip();
            }

            decoder.decodeBuffer(buffer);
            assertFalse(buffer.hasRemaining());
            verify(methodProcessor).receiveConnectionCloseOk();
        }
    }

    @Test
    void fieldLengthCannotConsumeFollowingFrame() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(24))
        {
            writeFrameHeader(buffer, (byte) 1, 0, 8);
            buffer.putInt((ConnectionSecureOkBody.CLASS_ID << 16) | ConnectionSecureOkBody.METHOD_ID);
            buffer.putUnsignedInt(4L);
            buffer.put(FRAME_END);

            writeFrameHeader(buffer, HeartbeatBody.TYPE, 0, 0);
            buffer.put(FRAME_END);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.FRAME_ERROR, exception.getErrorCode());
        assertEquals(ConnectionSecureOkBody.CLASS_ID, exception.getClassId());
        assertEquals(ConnectionSecureOkBody.METHOD_ID, exception.getMethodId());
        verify(methodProcessor, never()).receiveConnectionSecureOk(any(byte[].class));
    }

    @Test
    void connectionClassMethodOnNonZeroChannelIsRejectedBeforeDispatch() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final int bodySize = Integer.BYTES + Short.BYTES + Integer.BYTES + Short.BYTES;
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, bodySize);
            buffer.putInt((ConnectionTuneOkBody.CLASS_ID << 16) | ConnectionTuneOkBody.METHOD_ID);
            buffer.putUnsignedShort(0);
            buffer.putUnsignedInt(AMQDecoder.FRAME_MIN_SIZE);
            buffer.putUnsignedShort(0);
            buffer.put(FRAME_END);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.COMMAND_INVALID, exception.getErrorCode());
        assertEquals(ConnectionTuneOkBody.CLASS_ID, exception.getClassId());
        assertEquals(ConnectionTuneOkBody.METHOD_ID, exception.getMethodId());
        verify(methodProcessor, never()).receiveConnectionTuneOk(anyInt(), anyLong(), anyInt());
        verify(methodProcessor, never()).setCurrentMethod(ConnectionTuneOkBody.CLASS_ID, ConnectionTuneOkBody.METHOD_ID);
        verify(methodProcessor).setCurrentMethod(0, 0);
        verify(methodProcessor, times(1)).setCurrentMethod(anyInt(), anyInt());
    }

    @Test
    void truncatedMethodBodyIsReportedWithFailingMethodIdentifiers() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + Integer.BYTES + 1))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, Integer.BYTES);
            buffer.putInt((BasicQosBody.CLASS_ID << 16) | BasicQosBody.METHOD_ID);
            buffer.put(FRAME_END);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.FRAME_ERROR, exception.getErrorCode());
        assertEquals(BasicQosBody.CLASS_ID, exception.getClassId());
        assertEquals(BasicQosBody.METHOD_ID, exception.getMethodId());
        verify(methodProcessor, never()).getChannelMethodProcessor(CHANNEL_ID);
    }

    @ParameterizedTest
    @MethodSource("heartbeatChannelErrorCodes")
    void heartbeatOnNonZeroChannelIsRejectedBeforeDispatch(final ProtocolVersion protocolVersion,
                                                           final int expectedErrorCode) throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        when(methodProcessor.getProtocolVersion()).thenReturn(protocolVersion);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + 1))
        {
            writeHeartbeatFrame(buffer, CHANNEL_ID, new byte[0]);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(expectedErrorCode, exception.getErrorCode());
        verify(methodProcessor, never()).receiveHeartbeat();
        verify(methodProcessor).getProtocolVersion();
    }

    @Test
    void amqp091HeartbeatWithPayloadIsRejectedBeforeDispatch() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        when(methodProcessor.getProtocolVersion()).thenReturn(ProtocolVersion.v0_91);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + 2))
        {
            writeHeartbeatFrame(buffer, 0, new byte[] { 1 });
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.FRAME_ERROR, exception.getErrorCode());
        verify(methodProcessor, never()).receiveHeartbeat();
    }

    @ParameterizedTest
    @MethodSource("legacyProtocolVersions")
    void legacyHeartbeatWithPayloadRemainsCompatible(final ProtocolVersion protocolVersion) throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        when(methodProcessor.getProtocolVersion()).thenReturn(protocolVersion);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + 2))
        {
            writeHeartbeatFrame(buffer, 0, new byte[] { 1 });
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        verify(methodProcessor).receiveHeartbeat();
    }

    @Test
    void validHeartbeatDoesNotRequireProtocolVersionLookup() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + 1))
        {
            writeHeartbeatFrame(buffer, 0, new byte[0]);
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        verify(methodProcessor).receiveHeartbeat();
        verify(methodProcessor, never()).getProtocolVersion();
    }

    @Test
    void malformedNestedFieldTableIsRejectedBeforeLiveConnectionAccess() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final byte[] fieldTable = malformedNestedFieldTable();
        final int bodySize = Integer.BYTES + Integer.BYTES + fieldTable.length + 6 + Integer.BYTES + 6;
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1))
        {
            writeFrameHeader(buffer, (byte) 1, 0, bodySize);
            buffer.putInt((ConnectionStartOkBody.CLASS_ID << 16) | ConnectionStartOkBody.METHOD_ID);
            buffer.putUnsignedInt(fieldTable.length);
            buffer.put(fieldTable);
            writeShortString(buffer, "PLAIN");
            buffer.putUnsignedInt(0);
            writeShortString(buffer, "en_US");
            buffer.put(FRAME_END);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.FRAME_ERROR, exception.getErrorCode());
        assertEquals(ConnectionStartOkBody.CLASS_ID, exception.getClassId());
        assertEquals(ConnectionStartOkBody.METHOD_ID, exception.getMethodId());
        verify(methodProcessor, never()).ignoreAllButCloseOk();
        verify(methodProcessor, never()).receiveConnectionStartOk(any(), any(), any(), any());
        verify(methodProcessor, never()).setCurrentMethod(ConnectionStartOkBody.CLASS_ID, ConnectionStartOkBody.METHOD_ID);
        verify(methodProcessor).setCurrentMethod(0, 0);
        verify(methodProcessor, times(1)).setCurrentMethod(anyInt(), anyInt());
    }

    @Test
    void malformedNestedQueueArgumentsAreRejectedBeforeChannelLookup() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final byte[] fieldTable = malformedNestedFieldTable();
        final int bodySize = Integer.BYTES + Short.BYTES + 1 + 1 + Integer.BYTES + fieldTable.length;
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, bodySize);
            buffer.putInt((QueueDeclareBody.CLASS_ID << 16) | QueueDeclareBody.METHOD_ID);
            buffer.putUnsignedShort(0);
            writeShortString(buffer, "");
            buffer.put((byte) 1);
            buffer.putUnsignedInt(fieldTable.length);
            buffer.put(fieldTable);
            buffer.put(FRAME_END);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.FRAME_ERROR, exception.getErrorCode());
        assertEquals(QueueDeclareBody.CLASS_ID, exception.getClassId());
        assertEquals(QueueDeclareBody.METHOD_ID, exception.getMethodId());
        verify(methodProcessor, never()).getChannelMethodProcessor(CHANNEL_ID);
        verify(methodProcessor, never()).setCurrentMethod(QueueDeclareBody.CLASS_ID, QueueDeclareBody.METHOD_ID);
        verify(methodProcessor).setCurrentMethod(0, 0);
        verify(methodProcessor, times(1)).setCurrentMethod(anyInt(), anyInt());
    }

    @Test
    void configuredNestingDepthRejectsMethodBeforeDispatch() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor, 1);
        decoder.setExpectProtocolInitiation(false);
        final byte[] fieldTable = nestedFieldTable(2);
        final int bodySize = 24 + fieldTable.length;
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1))
        {
            writeConnectionStartOkFrame(buffer, fieldTable, bodySize);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.RESOURCE_ERROR, exception.getErrorCode());
        assertEquals(ConnectionStartOkBody.CLASS_ID, exception.getClassId());
        assertEquals(ConnectionStartOkBody.METHOD_ID, exception.getMethodId());
        verify(methodProcessor, never()).receiveConnectionStartOk(any(), any(), any(), any());
        verify(methodProcessor, never()).setCurrentMethod(ConnectionStartOkBody.CLASS_ID,
                ConnectionStartOkBody.METHOD_ID);
        verify(methodProcessor).setCurrentMethod(0, 0);
    }

    @Test
    void configuredNestingDepthAcceptsMethodAtLimit() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor, 2);
        decoder.setExpectProtocolInitiation(false);
        final byte[] fieldTable = nestedFieldTable(2);
        final int bodySize = 24 + fieldTable.length;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1))
        {
            writeConnectionStartOkFrame(buffer, fieldTable, bodySize);
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        verify(methodProcessor).receiveConnectionStartOk(any(), any(), any(), any());
        verify(methodProcessor).setCurrentMethod(ConnectionStartOkBody.CLASS_ID, ConnectionStartOkBody.METHOD_ID);
        verify(methodProcessor).setCurrentMethod(0, 0);
    }

    @Test
    void brokerDecoderUsesProtocolSpecificNestingContext() throws Exception
    {
        final AMQPConnection_0_8Impl connection = mock(AMQPConnection_0_8Impl.class);
        when(connection.getContextValue(Integer.class, AMQPConnection_0_8.CODEC_MAX_NESTED_OBJECTS))
                .thenReturn(1);
        final BrokerDecoder decoder = new BrokerDecoder(connection);
        decoder.setExpectProtocolInitiation(false);
        final byte[] fieldTable = nestedFieldTable(2);
        final int bodySize = 24 + fieldTable.length;
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1))
        {
            writeConnectionStartOkFrame(buffer, fieldTable, bodySize);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.RESOURCE_ERROR, exception.getErrorCode());
        assertEquals(ConnectionStartOkBody.CLASS_ID, exception.getClassId());
        assertEquals(ConnectionStartOkBody.METHOD_ID, exception.getMethodId());
        verify(connection).getContextValue(Integer.class, AMQPConnection_0_8.CODEC_MAX_NESTED_OBJECTS);
        verify(connection, never()).receiveConnectionStartOk(any(), any(), any(), any());
    }

    @Test
    void negativeProtocolSpecificNestingContextIsRejected()
    {
        final AMQPConnection_0_8Impl connection = mock(AMQPConnection_0_8Impl.class);
        when(connection.getContextValue(Integer.class, AMQPConnection_0_8.CODEC_MAX_NESTED_OBJECTS))
                .thenReturn(-1);

        assertThrows(IllegalArgumentException.class, () -> new BrokerDecoder(connection));
    }

    @Test
    void methodFrameWithTrailingBodyBytesIsRejected() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);

        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(13))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, 5);
            buffer.putInt((TxSelectBody.CLASS_ID << 16) | TxSelectBody.METHOD_ID);
            buffer.put(FRAME_END);
            buffer.put(FRAME_END);
            buffer.flip();

            assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        verify(channelMethodProcessor, never()).receiveTxSelect();
        verify(methodProcessor, never()).getChannelMethodProcessor(CHANNEL_ID);
        verify(methodProcessor, never()).setCurrentMethod(TxSelectBody.CLASS_ID, TxSelectBody.METHOD_ID);
        verify(methodProcessor).setCurrentMethod(0, 0);
        verify(methodProcessor, times(1)).setCurrentMethod(anyInt(), anyInt());
    }

    @Test
    void methodFrameWithInvalidEndMarkerIsRejectedBeforeDispatch() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);

        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, 4);
            buffer.putInt((TxSelectBody.CLASS_ID << 16) | TxSelectBody.METHOD_ID);
            buffer.put((byte) 0);
            buffer.flip();

            assertThrows(AMQFatalFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        verify(channelMethodProcessor, never()).receiveTxSelect();
        verify(methodProcessor, never()).getChannelMethodProcessor(CHANNEL_ID);
    }

    @Test
    void unsupportedFrameTypeIsFatal() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(2 * (AMQDecoder.FRAME_HEADER_SIZE + 1)))
        {
            writeFrameHeader(buffer, HeartbeatBody.TYPE, 0, 0);
            buffer.put(FRAME_END);
            writeFrameHeader(buffer, Byte.MAX_VALUE, 0, 0);
            buffer.put(FRAME_END);
            buffer.flip();

            assertThrows(AMQFatalFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        verify(methodProcessor).receiveHeartbeat();
    }

    @Test
    void validMethodFrameIsDispatchedAfterValidation() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);

        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, 4);
            buffer.putInt((TxSelectBody.CLASS_ID << 16) | TxSelectBody.METHOD_ID);
            buffer.put(FRAME_END);
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        final InOrder inOrder = inOrder(methodProcessor, channelMethodProcessor);
        inOrder.verify(methodProcessor).setCurrentMethod(TxSelectBody.CLASS_ID, TxSelectBody.METHOD_ID);
        inOrder.verify(methodProcessor).getChannelMethodProcessor(CHANNEL_ID);
        inOrder.verify(channelMethodProcessor).rejectMethodFrameIfContentIncomplete();
        inOrder.verify(channelMethodProcessor).receiveTxSelect();
        inOrder.verify(methodProcessor).setCurrentMethod(0, 0);
    }

    @Test
    void methodFrameIsNotDispatchedWhenContentSequenceIsIncomplete() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(channelMethodProcessor.rejectMethodFrameIfContentIncomplete()).thenReturn(true);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);

        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        final int bodySize = Integer.BYTES + Short.BYTES + 1 + 1 + Integer.BYTES;
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, bodySize);
            buffer.putInt((QueueDeclareBody.CLASS_ID << 16) | QueueDeclareBody.METHOD_ID);
            buffer.putUnsignedShort(0);
            writeShortString(buffer, "");
            buffer.put((byte) 0);
            buffer.putUnsignedInt(0);
            buffer.put(FRAME_END);
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        final InOrder inOrder = inOrder(methodProcessor, channelMethodProcessor);
        inOrder.verify(methodProcessor).setCurrentMethod(QueueDeclareBody.CLASS_ID, QueueDeclareBody.METHOD_ID);
        inOrder.verify(methodProcessor).getChannelMethodProcessor(CHANNEL_ID);
        inOrder.verify(channelMethodProcessor).rejectMethodFrameIfContentIncomplete();
        inOrder.verify(methodProcessor).setCurrentMethod(0, 0);
        verify(channelMethodProcessor, never()).receiveQueueDeclare(any(), anyBoolean(), anyBoolean(), anyBoolean(),
                anyBoolean(), anyBoolean(), any());
    }

    @Test
    void generatedChannelMethodWithTrailingBodyBytesIsRejectedBeforeChannelLookup() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(22))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, 14);
            buffer.putInt((BasicAckBody.CLASS_ID << 16) | BasicAckBody.METHOD_ID);
            buffer.putLong(42L);
            buffer.put((byte) 0);
            buffer.put((byte) 1);
            buffer.put(FRAME_END);
            buffer.flip();

            assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        verify(methodProcessor, never()).getChannelMethodProcessor(CHANNEL_ID);
    }

    @Test
    void directConnectionMethodWithTrailingBodyBytesIsRejectedBeforeDispatch() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        when(methodProcessor.getProtocolVersion()).thenReturn(ProtocolVersion.v0_91);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(13))
        {
            writeFrameHeader(buffer, (byte) 1, 0, 5);
            buffer.putInt(0x000a0033);
            buffer.put((byte) 1);
            buffer.put(FRAME_END);
            buffer.flip();

            assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        verify(methodProcessor, never()).receiveConnectionCloseOk();
    }

    @Test
    void validUnknownMethodPublishesMetadataForCloseReporting() throws Exception
    {
        final int classId = 0x7FFF;
        final int methodId = 1;
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, 0, Integer.BYTES);
            buffer.putInt((classId << 16) | methodId);
            buffer.put(FRAME_END);
            buffer.flip();

            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
        }

        assertEquals(ErrorCodes.COMMAND_INVALID, exception.getErrorCode());
        assertEquals(classId, exception.getClassId());
        assertEquals(methodId, exception.getMethodId());
        final InOrder inOrder = inOrder(methodProcessor);
        inOrder.verify(methodProcessor).setCurrentMethod(classId, methodId);
        inOrder.verify(methodProcessor).setCurrentMethod(0, 0);
    }

    @Test
    void callbackFailureStillResetsCurrentMethod() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);
        doThrow(new IllegalStateException("callback failed")).when(channelMethodProcessor).receiveTxSelect();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, Integer.BYTES);
            buffer.putInt((TxSelectBody.CLASS_ID << 16) | TxSelectBody.METHOD_ID);
            buffer.put(FRAME_END);
            buffer.flip();

            assertThrows(IllegalStateException.class, () -> decoder.decodeBuffer(buffer));
            assertFalse(buffer.hasRemaining());
        }

        verify(methodProcessor).setCurrentMethod(TxSelectBody.CLASS_ID, TxSelectBody.METHOD_ID);
        verify(methodProcessor).setCurrentMethod(0, 0);
    }

    @ParameterizedTest
    @MethodSource("decodingRuntimeFailures")
    void runtimeFailureFromCallbackIsNotReportedAsMalformedWireData(final RuntimeException failure) throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);
        doThrow(failure).when(channelMethodProcessor).receiveTxSelect();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, Integer.BYTES);
            buffer.putInt((TxSelectBody.CLASS_ID << 16) | TxSelectBody.METHOD_ID);
            buffer.put(FRAME_END);
            buffer.flip();

            assertSame(failure, assertThrows(failure.getClass(), () -> decoder.decodeBuffer(buffer)));
            assertFalse(buffer.hasRemaining());
        }

        verify(methodProcessor).setCurrentMethod(TxSelectBody.CLASS_ID, TxSelectBody.METHOD_ID);
        verify(methodProcessor).setCurrentMethod(0, 0);
    }

    @Test
    void runtimeFailureFromProtocolVersionLookupIsNotReportedAsMalformedWireData() throws Exception
    {
        final int classId = 0x7FFF;
        final int methodId = 1;
        final IllegalArgumentException failure = new IllegalArgumentException("Protocol version unavailable");
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        when(methodProcessor.getProtocolVersion()).thenThrow(failure);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, 0, Integer.BYTES);
            buffer.putInt((classId << 16) | methodId);
            buffer.put(FRAME_END);
            buffer.flip();

            final ConnectionScopedRuntimeException exception = assertThrows(ConnectionScopedRuntimeException.class,
                    () -> decoder.decodeBuffer(buffer));
            assertSame(failure, exception.getCause());
            assertFalse(buffer.hasRemaining());
        }

        verify(methodProcessor).setCurrentMethod(0, 0);
        verify(methodProcessor, times(1)).setCurrentMethod(anyInt(), anyInt());
    }

    @ParameterizedTest
    @MethodSource("protocolVersionsAndConnectionCloseOkMethods")
    void validVersionSpecificConnectionCloseOkIsDispatched(final ProtocolVersion protocolVersion,
                                                           final int classAndMethod) throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        when(methodProcessor.getProtocolVersion()).thenReturn(protocolVersion);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, 0, 4);
            buffer.putInt(classAndMethod);
            buffer.put(FRAME_END);
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        verify(methodProcessor).receiveConnectionCloseOk();
        verify(methodProcessor).setCurrentMethod(classAndMethod >> 16, classAndMethod & 0xFFFF);
        verify(methodProcessor).setCurrentMethod(0, 0);
    }

    @Test
    void validBasicPublishIsDispatchedWithDecodedArguments() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);

        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(21))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, 13);
            buffer.putInt((BasicPublishBody.CLASS_ID << 16) | BasicPublishBody.METHOD_ID);
            buffer.putUnsignedShort(0);
            writeShortString(buffer, "ex");
            writeShortString(buffer, "rk");
            buffer.put((byte) 1);
            buffer.put(FRAME_END);
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        verify(channelMethodProcessor).receiveBasicPublish(AMQShortString.valueOf("ex"),
                AMQShortString.valueOf("rk"), true, false);
        verify(channelMethodProcessor).rejectMethodFrameIfContentIncomplete();
        verify(methodProcessor, times(1)).getChannelMethodProcessor(CHANNEL_ID);
    }

    @Test
    void validConnectionMethodRetainsDecodedArgumentsUntilDispatch() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(18))
        {
            writeFrameHeader(buffer, (byte) 1, 0, 10);
            buffer.putInt((ConnectionSecureOkBody.CLASS_ID << 16) | ConnectionSecureOkBody.METHOD_ID);
            buffer.putUnsignedInt(2);
            buffer.put((byte) 1);
            buffer.put((byte) 2);
            buffer.put(FRAME_END);
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        final ArgumentCaptor<byte[]> response = ArgumentCaptor.forClass(byte[].class);
        verify(methodProcessor).receiveConnectionSecureOk(response.capture());
        assertArrayEquals(new byte[]{1, 2}, response.getValue());
    }

    @Test
    void deferredMethodHonoursCloseOnlyMode() throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(channelMethodProcessor.ignoreAllButCloseOk()).thenReturn(true);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);

        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, 4);
            buffer.putInt((TxSelectBody.CLASS_ID << 16) | TxSelectBody.METHOD_ID);
            buffer.put(FRAME_END);
            buffer.flip();

            decoder.decodeBuffer(buffer);
        }

        verify(channelMethodProcessor, never()).receiveTxSelect();
    }

    @ParameterizedTest
    @ValueSource(longs = {2147483648L, 4294967295L})
    void testDiscardPreservesUnsignedFrameSize(final long bodySize) throws Exception
    {
        final ServerDecoder decoder = new ServerDecoder(createMethodProcessor());
        decoder.setExpectProtocolInitiation(false);
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(32))
        {
            writeFrameHeader(buffer, ContentBody.TYPE, CHANNEL_ID, bodySize);
            buffer.flip();

            assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
            assertFalse(buffer.hasRemaining());

            buffer.clear();
            buffer.put(new byte[buffer.remaining()]);
            buffer.flip();
            decoder.decodeBuffer(buffer);

            assertFalse(buffer.hasRemaining(), "Discard must consume the fragment without retaining its payload");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDiscardedFrameEndIsValidated(final boolean fragmented) throws Exception
    {
        final ServerDecoder decoder = new ServerDecoder(createMethodProcessor());
        decoder.setExpectProtocolInitiation(false);
        decoder.setMaxFrameSize(16);
        final int bodySize = 17;
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1))
        {
            writeFrameHeader(buffer, ContentBody.TYPE, CHANNEL_ID, bodySize);
            buffer.put(new byte[bodySize]);
            buffer.put((byte) 0);
            buffer.flip();

            assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
            if (fragmented)
            {
                buffer.limit(buffer.limit() - 1);
                decoder.decodeBuffer(buffer);
                assertFalse(buffer.hasRemaining());
                buffer.limit(buffer.limit() + 1);
            }
            assertThrows(AMQFatalFrameDecodingException.class, () -> decoder.decodeBuffer(buffer));
            assertFalse(buffer.hasRemaining());
        }
    }

    @ParameterizedTest
    @MethodSource("decodingRuntimeFailures")
    void testRuntimeDecodingFailureRetainsMethodContext(final RuntimeException failure) throws Exception
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        final QpidByteBuffer input = createFailingMethodBody(failure);

        final AMQFrameDecodingException exception =
                assertThrows(AMQFrameDecodingException.class, () -> decoder.processMethod(CHANNEL_ID, input));
        assertSame(failure, exception.getCause());
        assertEquals(ErrorCodes.FRAME_ERROR, exception.getErrorCode());
        assertEquals(BasicQosBody.CLASS_ID, exception.getClassId());
        assertEquals(BasicQosBody.METHOD_ID, exception.getMethodId());
        verify(methodProcessor).setCurrentMethod(0, 0);

        decoder.setExpectProtocolInitiation(false);
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(12))
        {
            writeFrameHeader(buffer, (byte) 1, CHANNEL_ID, Integer.BYTES);
            buffer.putInt((TxSelectBody.CLASS_ID << 16) | TxSelectBody.METHOD_ID);
            buffer.put(FRAME_END);
            buffer.flip();
            decoder.decodeBuffer(buffer);
            assertFalse(buffer.hasRemaining());
        }
        verify(channelMethodProcessor).receiveTxSelect();
    }

    @ParameterizedTest
    @MethodSource("scopedAndFatalFailures")
    void testDecodingPreservesScopedAndFatalFailures(final Throwable failure)
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        final ServerDecoder decoder = new ServerDecoder(methodProcessor);
        final QpidByteBuffer input = createFailingMethodBody(failure);

        assertSame(failure, assertThrows(failure.getClass(), () -> decoder.processMethod(CHANNEL_ID, input)));
        verify(methodProcessor).setCurrentMethod(0, 0);
    }

    @ParameterizedTest
    @MethodSource("scopedAndFatalFailures")
    void testProtocolVersionLookupPreservesScopedAndFatalFailures(final Throwable failure)
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = createMethodProcessor();
        when(methodProcessor.getProtocolVersion()).thenThrow(failure);
        final FrameBoundaryValidatingServerMethodProcessor validatingProcessor =
                new FrameBoundaryValidatingServerMethodProcessor(methodProcessor);

        assertSame(failure, assertThrows(failure.getClass(), validatingProcessor::getProtocolVersion));
    }

    private static QpidByteBuffer createFailingMethodBody(final Throwable failure)
    {
        final QpidByteBuffer input = mock(QpidByteBuffer.class);
        when(input.remaining()).thenReturn(Integer.BYTES + Long.BYTES);
        when(input.getInt()).thenReturn((BasicQosBody.CLASS_ID << 16) | BasicQosBody.METHOD_ID);
        when(input.getUnsignedInt()).thenThrow(failure);
        return input;
    }

    private static Stream<RuntimeException> decodingRuntimeFailures()
    {
        return Stream.of(new BufferUnderflowException(), new ArithmeticException("Injected decoding failure"),
                new ClassCastException("Injected decoding failure"), new RuntimeException("Injected decoding failure"));
    }

    private static Stream<Throwable> scopedAndFatalFailures()
    {
        return Stream.of(new ConnectionScopedRuntimeException("Injected connection failure"),
                new ServerScopedRuntimeException("Injected server failure"),
                new StoreException("Injected store failure"), new InternalError("Injected JVM failure"));
    }

    private static ServerMethodProcessor<ServerChannelMethodProcessor> createMethodProcessor()
    {
        return mock(ServerMethodProcessor.class);
    }

    private static ServerDecoder createRejectingDecoder(
            final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor)
    {
        return new ServerDecoder(methodProcessor)
        {
            private boolean _rejectFirstFrame = true;

            @Override
            protected void processFrame(final int channel,
                                        final byte type,
                                        final long bodySize,
                                        final QpidByteBuffer body) throws AMQFrameDecodingException
            {
                if (_rejectFirstFrame)
                {
                    _rejectFirstFrame = false;
                    body.getUnsignedShort();
                    throw new AMQFrameDecodingException("Frame processing failed");
                }
                super.processFrame(channel, type, bodySize, body);
            }
        };
    }

    private static Stream<Arguments> rejectedFrameReadVariants()
    {
        return protocolVersionsAndConnectionCloseOkMethods()
                .flatMap(arguments -> Stream.of(3, 7, 11, 12, 15, 23, 24)
                        .flatMap(firstReadLength -> Stream.of(false, true)
                                .map(direct -> Arguments.of(arguments.get()[0], arguments.get()[1],
                                        firstReadLength, direct))));
    }

    private static Stream<Arguments> protocolVersionsAndConnectionCloseOkMethods()
    {
        return Stream.of(Arguments.of(ProtocolVersion.v0_8, 0x000a003d),
                Arguments.of(ProtocolVersion.v0_9, 0x000a0033), Arguments.of(ProtocolVersion.v0_91, 0x000a0033));
    }

    private static Stream<Arguments> heartbeatChannelErrorCodes()
    {
        return Stream.of(Arguments.of(ProtocolVersion.v0_8, ErrorCodes.FRAME_ERROR),
                Arguments.of(ProtocolVersion.v0_9, ErrorCodes.FRAME_ERROR),
                Arguments.of(ProtocolVersion.v0_91, ErrorCodes.COMMAND_INVALID));
    }

    private static Stream<ProtocolVersion> legacyProtocolVersions()
    {
        return Stream.of(ProtocolVersion.v0_8, ProtocolVersion.v0_9);
    }

    private static void writeFrameHeader(final QpidByteBuffer buffer,
                                         final byte type,
                                         final int channel,
                                         final long bodySize)
    {
        buffer.put(type);
        buffer.putUnsignedShort(channel);
        buffer.putUnsignedInt(bodySize);
    }

    private static void writeHeartbeatFrame(final QpidByteBuffer buffer,
                                            final int channel,
                                            final byte[] payload)
    {
        writeFrameHeader(buffer, HeartbeatBody.TYPE, channel, payload.length);
        buffer.put(payload);
        buffer.put(FRAME_END);
    }

    private static void writeShortString(final QpidByteBuffer buffer, final String value)
    {
        final byte[] encoded = value.getBytes(UTF_8);
        buffer.put((byte) encoded.length);
        buffer.put(encoded);
    }

    private static void writeConnectionStartOkFrame(final QpidByteBuffer buffer,
                                                    final byte[] fieldTable,
                                                    final int bodySize)
    {
        writeFrameHeader(buffer, (byte) 1, 0, bodySize);
        buffer.putInt((ConnectionStartOkBody.CLASS_ID << 16) | ConnectionStartOkBody.METHOD_ID);
        buffer.putUnsignedInt(fieldTable.length);
        buffer.put(fieldTable);
        writeShortString(buffer, "PLAIN");
        buffer.putUnsignedInt(0);
        writeShortString(buffer, "en_US");
        buffer.put(FRAME_END);
    }

    private static byte[] nestedFieldTable(final int depth)
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

    private static byte[] malformedNestedFieldTable()
    {
        final byte[] malformedNestedTable = ByteBuffer.allocate(7)
                .put((byte) 1)
                .put((byte) 's')
                .put(AMQType.LONG_STRING.identifier())
                .putInt(0x80000000)
                .array();
        return ByteBuffer.allocate(13 + malformedNestedTable.length)
                .put((byte) 7)
                .put("ignored".getBytes(UTF_8))
                .put(AMQType.FIELD_TABLE.identifier())
                .putInt(malformedNestedTable.length)
                .put(malformedNestedTable)
                .array();
    }
}
